use crate::db::compaction::{Compaction, ManualCompaction};
use crate::db::config::LEVEL_NUMBER;
use crate::db::errors::{DError, DResult};
use crate::db::file::{
    current_file_name, manifest_file_name, parse_file_name, set_current_file, FileType,
};
use crate::db::file_meta::FileMetaData;
use crate::db::iterator::{new_merging_iterator, LevelFileNumIter, TwoLevelIter};
use crate::db::key::InternalKey;
use crate::db::log_reader::Reader;
use crate::db::log_writer::Writer;
use crate::db::options::Options;
use crate::db::version::{get_file_iterator, total_file_size, Version};
use crate::db::version_edit::VersionEdit;
use crate::db::version_manager_builder::Builder;
use crate::db::{ReadOptions, L0_COMPACTION_TRIGGER};
use crate::env::Env;
use crate::table::TableCache;
use crate::utils::cmp::InternalKeyComparator;
use crate::utils::iter::Iter;
use hashbrown::HashSet;
use slog::{debug, error, info};
use std::cmp::Ordering;
use std::iter;
use std::sync::{Arc, RwLock};

fn target_file_size(options: Options) -> u64 {
    options.max_file_size
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
pub fn max_grand_parent_overlap_bytes(options: Options) -> u64 {
    target_file_size(options) * 10
}

// Maximum number of bytes in all compacted files. We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
fn expanded_compaction_byte_size_limit(options: Options) -> u64 {
    target_file_size(options) * 25
}

fn max_bytes_for_level(mut level: i64) -> f64 {
    // Note: the result for level zero is not really used since we set
    // the level-0 compaction threshold based on number of files.

    // Result for both level-0 and level-1
    let mut result = 10.0 * 1048576.0;
    while level > 1 {
        result *= 10.0;
        level -= 1;
    }
    result
}

pub fn max_file_size_for_level(options: Options) -> u64 {
    target_file_size(options)
}

/// Binary search to find specific file.
/// Especially for level n >0 files list.
pub fn find_file(
    cmp: Arc<dyn InternalKeyComparator>,
    files: &[Arc<FileMetaData>],
    internal_key: &[u8],
) -> usize {
    let mut left = 0;
    let mut right = files.len();
    while left < right {
        let mid = (left + right) / 2;
        let f = &files[mid];
        if cmp.lt(f.get_largest().encode(), internal_key) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    right
}

fn finalize(v: &mut Version) {
    let mut best_level = -1i64;
    let mut best_score = -1.0;
    // level 0   check file number.
    // level > 0 check the file size.
    for level in 0..LEVEL_NUMBER - 1 {
        let score: f64;
        if level == 0 {
            // We treat level-0 specially by bounding the number of files
            // instead of number of bytes for two reasons:
            //
            // (1) With larger write-buffer sizes, it is nice not to do too
            // many level-0 compactions.
            //
            // (2) The files in level-0 are merged on every read and
            // therefore we wish to avoid too many files when the individual
            // file size is small (perhaps because of a small write-buffer
            // setting, or very high compression ratios, or lots of
            // overwrites/deletions).
            score = v.num_files_count(level as usize) as f64 / L0_COMPACTION_TRIGGER as f64;
        } else {
            // Compute the ratio of current size to size limit.
            let level_bytes = total_file_size(v.num_files(level as usize));
            score = level_bytes as f64 / max_bytes_for_level(level as i64);
        }
        if score > best_score {
            best_level = level as i64;
            best_score = score;
        }
    }
    v.set_compaction_level(best_level);
    v.set_compaction_score(best_score);
}

pub struct VersionManager {
    db_name: String,
    /// current version
    current: Option<Arc<RwLock<Version>>>,
    /// log file number
    log_number: u64,
    /// last used sequence number
    last_sequence: u64,
    /// next file number to use
    next_file_number: u64,
    /// manifest file number
    manifest_file_num: u64,
    /// In order to compact each level as evenly as possible, the end-key of this compact
    /// is used as the start-key of the next compact.
    /// The compact_pointer holds the start-key of the next compact for each level.
    /// Versions other than `current` do not compact, so this value is not stored in Version.
    ///
    /// Per-level key at which the next compaction at that level should start.
    /// Either an empty string, or a valid InternalKey.
    compact_pointer: Vec<Vec<u8>>,
    /// manifest writer
    descriptor_log: Option<Writer>,
    dummy_versions: Arc<RwLock<Version>>,

    pub(crate) env: Arc<dyn Env + Send + Sync>,
    pub(crate) options: Options,
    pub(crate) icmp: Arc<dyn InternalKeyComparator>,
    pub(crate) table_cache: Arc<TableCache>,
}

unsafe impl Send for VersionManager {}

unsafe impl Sync for VersionManager {}

impl VersionManager {
    pub fn new(
        table_cache: Arc<TableCache>,
        options: Options,
        icmp: Arc<dyn InternalKeyComparator>,
        db_name: String,
    ) -> Self {
        let tmp = Version::new(icmp.clone(), table_cache.clone(), options.clone());
        let mut s = VersionManager {
            db_name,
            current: None,
            log_number: 0,
            last_sequence: 0,
            manifest_file_num: 0,
            next_file_number: 2,
            descriptor_log: None,
            dummy_versions: Version::new(icmp.clone(), table_cache.clone(), options.clone()),
            compact_pointer: iter::repeat(vec![])
                .take(LEVEL_NUMBER as usize)
                .collect::<Vec<_>>(),
            env: options.env.clone(),
            icmp,
            options,
            table_cache,
        };
        s.append_version(tmp);
        s
    }

    // Returns true if some level needs a compaction.
    // * compaction_score >= 1.0
    // * file_to_compact is not none.
    pub(crate) fn needs_compaction(&self) -> bool {
        let v = self.current.as_ref().unwrap();
        v.read().unwrap().get_compaction_score() >= 1.0
            || v.read().unwrap().file_to_compact_and_level.0.is_some()
    }

    pub(crate) fn get_live_files(&self) -> HashSet<u64> {
        let mut r = HashSet::new();
        let mut v = self.dummy_versions.read().unwrap().next.clone().unwrap();
        while !Arc::ptr_eq(&v, &self.dummy_versions) {
            for level in 0..LEVEL_NUMBER {
                let vv = v.read().unwrap();
                let files = vv.num_files(level);
                for f in files {
                    r.insert(f.get_file_number());
                }
            }
            v = v.clone().read().unwrap().next.clone().unwrap();
        }
        r
    }

    // Apply edit to the current version to form a new descriptor that
    // is both saved to persistent state and installed as the new
    // current version.
    pub(crate) fn log_and_append_version(&mut self, edit: &mut VersionEdit) -> DResult<()> {
        if edit.has_log_number() {
            assert!(edit.log_number >= self.log_number);
            assert!(edit.log_number < self.next_file_number);
        } else {
            edit.set_log_number(self.log_number);
        }

        edit.set_next_file(self.next_file_number);
        edit.set_last_sequence(self.last_sequence);

        let v = Version::new(
            self.icmp.clone(),
            self.table_cache.clone(),
            self.options.clone(),
        );
        {
            let base = &*self.current.as_ref().unwrap().read().unwrap();
            let cp = &mut self.compact_pointer;

            let mut builder = Builder::new(base, self.icmp.clone(), cp);
            builder.apply_changes_from_version_edit(edit);
            let files = builder.save_to();
            v.write().unwrap().set_files(files);
        }
        finalize(&mut *v.write().unwrap());

        // Initialize new descriptor log file if necessary by creating
        // a temporary file that contains a snapshot of the current version.
        let mut new_manifest_file = String::default();
        let mut ret;
        if self.descriptor_log.is_none() {
            edit.set_next_file(self.next_file_number);

            new_manifest_file = manifest_file_name(self.db_name.as_str(), self.manifest_file_num);
            let file = self.env.new_writable_file(&new_manifest_file);
            if let Ok(f) = file {
                self.descriptor_log = Some(Writer::new(f));
                self.write_snapshot();
            }
        }
        {
            let mut record = vec![];
            edit.encode_to(&mut record);
            ret = self
                .descriptor_log
                .as_mut()
                .unwrap()
                .write_record(record.as_slice());
            if ret.is_err() {
                error!(
                    self.options.info_log.as_ref().unwrap(),
                    "MANIFEST write failed: {:?}", ret
                );
            }

            // If we just created a new descriptor file, install it by writing a
            // new CURRENT file that points to it.
            if !new_manifest_file.is_empty() {
                ret = set_current_file(
                    self.env.clone(),
                    self.db_name.as_str(),
                    self.manifest_file_num,
                );
            }
        }
        if ret.is_ok() {
            // Install the new version
            self.append_version(v);
            self.log_number = edit.log_number;
        } else if !new_manifest_file.is_empty() {
            self.descriptor_log = None;
            self.env.delete_file(&new_manifest_file)?;
        }
        ret
    }

    /// Recover the last saved descriptor from persistent storage.
    pub(crate) fn recover(&mut self, save_manifest: &mut bool) -> DResult<()> {
        info!(
            self.options.info_log.as_ref().unwrap(),
            "recover version set"
        );
        let mut current = self
            .env
            .read_file_to_string(&current_file_name(&self.db_name))?;

        if current.is_empty() || current.as_bytes()[current.len() - 1] != b'\n' {
            error!(
                self.options.info_log.as_ref().unwrap(),
                "CURRENT file does not end with newline"
            );
            return Err(DError::CustomError(
                "CURRENT file does not end with newline",
            ));
        }
        // remove the \n new line.
        current.truncate(current.len() - 1);

        let dsc = format!("{}/{}", &self.db_name, current);

        let f = match self.env.new_sequential_file(&dsc) {
            Ok(f) => {
                info!(
                    self.options.info_log.as_ref().unwrap(),
                    "read CURRENT file {}", &dsc
                );
                f
            }
            Err(_) => return Err(DError::CustomError("CURRENT points to a non-existent file")),
        };

        let mut have_log_number = false;
        let have_prev_log_number = false;
        let mut have_next_file = false;
        let mut have_last_sequence = false;

        let mut next_file = 0;
        let mut last_sequence = 0;
        let mut log_number = 0;
        let mut prev_log_number = 0;

        let v = Version::new(
            self.icmp.clone(),
            self.table_cache.clone(),
            self.options.clone(),
        );

        {
            let cp = &mut self.compact_pointer;
            let base = &*self.current.as_ref().unwrap().read().unwrap();
            let mut builder = Builder::new(base, self.icmp.clone(), cp);
            {
                let mut reader = Reader::new(f, true);
                let mut record = vec![];
                while reader.read_record(&mut record) {
                    let mut edit = VersionEdit::new();
                    edit.decode_from(record.as_slice())?;
                    if edit.has_cmp()
                        && edit.comparator_name() != self.icmp.clone().user_comparator().name()
                    {
                        return Err(DError::CustomError("Comparator does not match"));
                    }
                    builder.apply_changes_from_version_edit(&edit);
                    if edit.has_log_number() {
                        log_number = edit.log_number;
                        have_log_number = true;
                    }
                    if edit.has_next_file_number() {
                        next_file = edit.next_file_number;
                        have_next_file = true;
                    }
                    if edit.has_last_sequence() {
                        last_sequence = edit.last_sequence;
                        have_last_sequence = true;
                    }
                }
            }
            if !have_next_file {
                return Err(DError::CustomError("no meta-nextfile entry in descriptor"));
            } else if !have_log_number {
                return Err(DError::CustomError("no meta-lognumber entry in descriptor"));
            } else if !have_last_sequence {
                return Err(DError::CustomError(
                    "no last-sequence-number entry in descriptor",
                ));
            }
            if !have_prev_log_number {
                prev_log_number = 0;
            }

            // TODO: self.mark_file_number_used(prev_log_number);
            if self.next_file_number <= prev_log_number {
                self.next_file_number = prev_log_number + 1;
            }

            // self.mark_file_number_used(log_number);
            if self.next_file_number <= log_number {
                self.next_file_number = log_number + 1;
            }

            let files = builder.save_to();
            v.write().unwrap().set_files(files);
            finalize(&mut *v.write().unwrap());
        }

        self.append_version(v);
        self.manifest_file_num = next_file;
        self.next_file_number = next_file + 1;
        self.last_sequence = last_sequence;
        self.log_number = log_number;

        // See if we can reuse the existing MANIFEST file.
        if self.reuse_manifest(&dsc, &current) {
            // No need to save new manifest
        } else {
            *save_manifest = true;
        }

        Ok(())
    }

    // Arrange to reuse "file_number" unless a newer file number has
    // already been allocated.
    pub(crate) fn reuse_file_number(&mut self, file_number: u64) {
        if self.next_file_number == file_number + 1 {
            self.next_file_number = file_number;
        }
    }

    pub(crate) fn reuse_manifest(&mut self, dsc: &str, base: &str) -> bool {
        if !self.options.reuse_logs {
            return false;
        }
        info!(self.options.info_log.as_ref().unwrap(), "reuse manifest");
        let mut manifest_type = FileType::TempFile;
        let mut manifest_number = 0;
        let mut manifest_size = 0;
        if !parse_file_name(base, &mut manifest_number, &mut manifest_type)
            || manifest_type != FileType::DescriptorFile
            || self.env.get_file_size(dsc, &mut manifest_size).is_err()
            || manifest_size >= target_file_size(self.options.clone())
        {
            return false;
        }
        assert!(self.descriptor_log.is_none());

        let ret = self.env.new_appendable_file(dsc);
        match ret {
            Ok(descriptor_file) => {
                self.descriptor_log = Some(Writer::new_with_offset(
                    descriptor_file,
                    manifest_size as usize,
                ));
                info!(
                    self.options.info_log.as_ref().unwrap(),
                    "reusing manifest {}", dsc
                );
                self.manifest_file_num = manifest_number;
                true
            }
            Err(err) => {
                error!(
                    self.options.info_log.as_ref().unwrap(),
                    "reusing manifest {} error:{}", dsc, err
                );
                false
            }
        }
    }

    // Mark the specified file number as used.
    pub(crate) fn mark_file_number_used(&mut self, num: u64) {
        if self.next_file_number <= num {
            self.next_file_number = num + 1;
        }
    }

    pub(crate) fn num_level_files(&self, level: i64) -> usize {
        assert!(level >= 0);
        assert!(level < LEVEL_NUMBER as i64);
        let c = self.current.as_ref().unwrap();
        c.read().unwrap().num_files_count(level as usize)
    }

    pub fn num_level_bytes(&self, level: i64) -> u64 {
        assert!(level >= 0);
        assert!(level < LEVEL_NUMBER as i64);
        total_file_size(
            self.current
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .num_files(level as usize),
        )
    }

    pub fn level_summary<'a>(&self, scratch: &'a mut LevelSummaryStorage) -> &'a String {
        let c = &self.current.as_ref().unwrap().read().unwrap();
        scratch.buffer = format!(
            "{:?} Files {} {} {} {} {} {} {}",
            scratch,
            c.num_files_count(0),
            c.num_files_count(1),
            c.num_files_count(2),
            c.num_files_count(3),
            c.num_files_count(4),
            c.num_files_count(5),
            c.num_files_count(6),
        );
        &scratch.buffer
    }

    // Save current contents to `log`
    fn write_snapshot(&mut self) {
        // save metadata
        let mut edit = VersionEdit::new();
        edit.set_comparator_name(self.icmp.user_comparator().name());

        // save compaction pointers
        for level in 0..LEVEL_NUMBER {
            if !self.compact_pointer[level].is_empty() {
                let mut key = InternalKey::default();
                key.decode(self.compact_pointer[level].as_slice());
                edit.set_compact_pointer(level as i64, key);
            }
        }

        // save files
        for level in 0..LEVEL_NUMBER {
            let c = self.current.as_ref().unwrap();
            let aa = c.read().unwrap();

            let files = aa.num_files(level);
            for i in files {
                edit.add_files(
                    level as i64,
                    i.get_file_number(),
                    i.get_file_size(),
                    i.get_smallest(),
                    i.get_largest(),
                )
            }
        }
        // write into log
        let mut record = vec![];
        edit.encode_to(&mut record);
        self.descriptor_log
            .as_mut()
            .unwrap()
            .write_record(record.as_slice())
            .unwrap();
    }

    pub(crate) fn approximate_offset_of(&mut self, v: &Version, ikey: &InternalKey) -> u64 {
        let mut result = 0;
        for level in 0..LEVEL_NUMBER {
            let files = v.num_files(level as usize);
            for f in files.iter() {
                if self.icmp.compare_internal_key(f.get_largest(), ikey) != Ordering::Greater {
                    // Entire file is before "ikey", so just add the file size
                    result += f.get_file_size();
                } else if self.icmp.compare_internal_key(f.get_smallest(), ikey)
                    == Ordering::Greater
                {
                    // Entire file is after `ikey`, so ignore
                    if level > 0 {
                        // Files other than level 0 are sorted by meta.smallest, so
                        // no further files in this level will contain data for `ikey`
                        break;
                    }
                } else {
                    // `ikey` falls in the range for this table. Add the
                    // approximate offset of `ikey` within the table.
                    if let Ok(table) = self
                        .table_cache
                        .find_table(f.get_file_number(), f.get_file_size())
                    {
                        result += table.approximate_offset_of(ikey.encode());
                    };
                }
            }
        }

        result
    }

    // Stores the minimal range that covers all entries in inputs in smallest, largest.
    // Inputs can not be not empty
    pub(crate) fn get_range(
        &self,
        inputs: &[Arc<FileMetaData>],
        smallest: &mut InternalKey,
        largest: &mut InternalKey,
    ) {
        assert!(!inputs.is_empty());
        smallest.clear();
        largest.clear();
        for (i, f) in inputs.iter().enumerate() {
            if i == 0 {
                *smallest = f.get_smallest().clone();
                *largest = f.get_largest().clone();
            } else {
                if self.icmp.compare_internal_key(f.get_smallest(), smallest) == Ordering::Less {
                    *smallest = f.get_smallest().clone();
                }
                if self.icmp.compare_internal_key(f.get_largest(), largest) == Ordering::Greater {
                    *largest = f.get_largest().clone();
                }
            }
        }
    }

    // TODO: test
    fn get_range2(
        &self,
        inputs1: &[Arc<FileMetaData>],
        inputs2: &[Arc<FileMetaData>],
        smallest: &mut InternalKey,
        largest: &mut InternalKey,
    ) {
        let mut all = vec![];
        for i in inputs1 {
            all.push(i.clone());
        }
        for i in inputs2 {
            all.push(i.clone());
        }
        self.get_range(&all, smallest, largest);
    }

    // Create an iterator that reads over the compaction inputs for `c`.
    // The caller should delete the iterator when no longer needed.
    pub(crate) fn make_input_iterator(&self, c: &Compaction) -> Box<dyn Iter> {
        debug!(
            self.options.info_log.as_ref().unwrap(),
            "make_input_iterator"
        );
        let options = ReadOptions::new_with_verify(self.options.paranoid_checks, false);

        // Level-0 files have to be merged together. For other levels,
        // we will make a concatenating iterator per level.
        // TODO:(opt): use concatenating iterator for level-0 if there is no overlap
        let space = if c.level() == 0 {
            c.num_input_files(0) + 1
        } else {
            2
        };
        let mut list = vec![];
        for i in 0..2 {
            if !c.get_input(i).is_empty() {
                if c.level() + i as i64 == 0 {
                    let files = c.get_input(i);
                    for f in files.iter() {
                        let (iter, _) = self.table_cache.new_iter(
                            options.clone(),
                            f.get_file_number(),
                            f.get_file_size(),
                        );
                        list.push(iter);
                    }
                } else {
                    // clone the table_cache, and move to the closure.
                    let tc = self.table_cache.clone();
                    list.push(Box::new(TwoLevelIter::new(
                        Box::new(LevelFileNumIter::new(
                            self.icmp.clone(),
                            c.get_input(i).clone(),
                        )),
                        options.clone(),
                        move |read_opt, s| get_file_iterator(read_opt, s, tc.clone()),
                    )));
                }
            }
        }

        let num = list.len();
        assert!(num <= space);
        let result = new_merging_iterator(self.icmp.clone().into_cmp(), list, num);
        result
    }

    /// Return a compaction object for compacting the range [begin,end] in
    /// the specified level. Returns None if there is nothing in that
    /// level that overlaps the specified range. Caller should delete the result.
    /// For ManualCompaction
    pub(crate) fn compact_range(&mut self, manual: &ManualCompaction) -> Option<Compaction> {
        let begin = &manual.begin;
        let end = &manual.end;
        let level = manual.level;

        let mut inputs: Vec<Arc<FileMetaData>> = vec![];
        self.current
            .as_ref()
            .unwrap()
            .read()
            .unwrap()
            .get_overlapping_files(level, begin, end, &mut inputs);
        if inputs.is_empty() {
            return None;
        }
        // Avoid compacting too much in one shot in case the range is large.
        // But we cannot do this for level-0 since level-0 files can overlap
        // and we must not pick one file and drop another older file if the
        // two files overlap.
        let index;
        if level > 0 {
            let limit = max_file_size_for_level(self.options.clone());
            let mut total = 0;
            for (i, f) in inputs.iter().enumerate() {
                total += f.get_file_size();
                if total >= limit {
                    index = i + 1;
                    // ignore the files if reach the file size limit.
                    inputs.drain(index..);
                    break;
                }
            }
        }
        let mut c = Compaction::new(self.options.clone(), level);
        c.set_input_version(self.current.clone());
        c.set_input(0, inputs);
        self.setup_other_inputs(&mut c);
        Some(c)
    }

    // Pick level and inputs for a new compaction.
    // Returns None if there is no compaction to be done.
    // Otherwise returns a pointer to a heap-allocated object that
    // describes the compaction. Caller should delete the result.
    pub(crate) fn pick_compaction(&mut self) -> Option<Compaction> {
        let level: i64;
        let mut c: Compaction;

        let size_compaction = self
            .current
            .as_ref()
            .unwrap()
            .read()
            .unwrap()
            .get_compaction_score()
            >= 1.0;
        let seek_compaction = self
            .current
            .as_ref()
            .unwrap()
            .read()
            .unwrap()
            .file_to_compact_and_level
            .0
            .is_some();

        if size_compaction {
            level = self
                .current
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .get_compaction_level();
            assert!(level >= 0);
            assert!(level + 1 < LEVEL_NUMBER as i64);
            c = Compaction::new(self.options.clone(), level);

            // Pick the first file that comes after compact_pointer[level]
            for f in self
                .current
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .num_files(level as usize)
                .iter()
            {
                if self.compact_pointer[level as usize].is_empty()
                    || self.icmp.gt(
                        f.get_largest().encode(),
                        self.compact_pointer[level as usize].as_slice(),
                    )
                {
                    c.get_mut_input(0).push(f.clone());
                    break;
                }
            }
            if c.get_input(0).is_empty() {
                // Wrap-around to the beginning of the key space
                c.get_mut_input(0).push(
                    self.current
                        .as_ref()
                        .unwrap()
                        .read()
                        .unwrap()
                        .num_files(level as usize)[0]
                        .clone(),
                );
            }
        } else if seek_compaction {
            // reach the allow_seeks limit, just add the file that needs to compact.
            level = self
                .current
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .file_to_compact_and_level
                .1;
            c = Compaction::new(self.options.clone(), level);
            c.get_mut_input(0).push(
                self.current
                    .as_ref()
                    .unwrap()
                    .read()
                    .unwrap()
                    .file_to_compact_and_level
                    .0
                    .clone()
                    .unwrap(),
            );
        } else {
            return None;
        }
        c.set_input_version(self.current.clone());

        // Files in level 0 may overlap each other, so pick up all overlapping ones
        if level == 0 {
            let mut smallest = InternalKey::default();
            let mut largest = InternalKey::default();
            self.get_range(c.get_input(0), &mut smallest, &mut largest);

            self.current
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .get_overlapping_files(0, &Some(smallest), &Some(largest), c.get_mut_input(0));
            assert!(!c.get_input(0).is_empty());
        }
        self.setup_other_inputs(&mut c);
        Some(c)
    }

    /// TODO: benchmark 10,000 files
    /// 1.get key range [smallest,largest] from level n.
    /// 2.get overlapping files from level[n+1] by range [smallest,largest], puts in inputs[1]
    /// 3.get range [all_start,all_limit] from level[n] and level[n+1].
    fn setup_other_inputs(&mut self, c: &mut Compaction) {
        let level = c.level();

        let mut smallest = Some(InternalKey::default());
        let mut largest = Some(InternalKey::default());
        self.get_range(
            c.get_input(0),
            smallest.as_mut().unwrap(),
            largest.as_mut().unwrap(),
        );

        // get overlapping files from level[n+1], puts in inputs[1]
        let current = self.current.as_ref().unwrap();
        current.read().unwrap().get_overlapping_files(
            level + 1,
            &smallest,
            &largest,
            c.get_mut_input(1),
        );

        // Get entire range covered by compaction
        let mut all_start = Some(InternalKey::default());
        let mut all_limit = Some(InternalKey::default());
        self.get_range2(
            c.get_input(0),
            c.get_input(1),
            all_start.as_mut().unwrap(),
            all_limit.as_mut().unwrap(),
        );

        // Check if there are overlapping files in level[n+1] from range [A_smallest,A_largest]
        // if got overlapping files (B,C,D), recalculate range [all_start,all_limit] equals to
        // [B_smallest,D_largest].
        // Then, recalculate expanded files in level[n], got extra file (E).
        // See if we can add file (E) without changing the number of level[n+1] files.

        // e.g. [B_smallest,D_largest] contains E, it's ok.
        // level n+1    <--> <--> <-------><--><----> <-->
        //                           B      C     D
        // level n      <------->  <----><--------->
        //                            E        A
        if !c.get_input(1).is_empty() {
            let mut expanded0: Vec<Arc<FileMetaData>> = vec![];
            current.read().unwrap().get_overlapping_files(
                level,
                &all_start,
                &all_limit,
                &mut expanded0,
            );
            let inputs0_size = total_file_size(c.get_input(0));
            let inputs1_size = total_file_size(c.get_input(1));
            let expanded0_size = total_file_size(&expanded0);
            // if we got extra file `E` && check total file size has not reached the limit
            if expanded0.len() > c.num_input_files(0)
                && inputs1_size + expanded0_size
                    < expanded_compaction_byte_size_limit(self.options.clone())
            {
                let mut new_start = Some(InternalKey::default());
                let mut new_limit = Some(InternalKey::default());
                // calculate new range in level[n]
                self.get_range(
                    &expanded0,
                    new_start.as_mut().unwrap(),
                    new_limit.as_mut().unwrap(),
                );
                let mut expanded1: Vec<Arc<FileMetaData>> = vec![];
                // get overlapping files by using new range.
                current.read().unwrap().get_overlapping_files(
                    level + 1,
                    &new_start,
                    &new_limit,
                    &mut expanded1,
                );
                // if the number of files is equal to the previous count, the extra file can be added.
                if expanded1.len() == c.num_input_files(1) {
                    info!(
                        self.options.info_log.as_ref().unwrap(),
                        "Expanding level<{}> {}+{} ({}+{} bytes) to {}+{} ({}+{} bytes)",
                        level,
                        c.num_input_files(0),
                        c.num_input_files(1),
                        inputs0_size,
                        inputs1_size,
                        expanded0.len(),
                        expanded1.len(),
                        expanded0_size,
                        inputs1_size
                    );
                    // value assigned to `smallest` is never read
                    // smallest = new_start;
                    largest = new_limit;
                    c.set_input(0, expanded0);
                    c.set_input(1, expanded1);
                    self.get_range2(
                        c.get_input(0),
                        c.get_input(1),
                        all_start.as_mut().unwrap(),
                        all_limit.as_mut().unwrap(),
                    );
                }
            }
        }

        // Compute the set of grandparent files that overlap this compaction
        if level + 2 < LEVEL_NUMBER as i64 {
            current.read().unwrap().get_overlapping_files(
                level + 2,
                &all_start,
                &all_limit,
                c.get_mut_grandparents(),
            );
        }
        // Update the place where we will do the next compaction for this level.
        // We update this immediately instead of waiting for the VersionEdit
        // to be applied so that if the compaction fails, we will try a different
        // key range next time.
        self.compact_pointer[level as usize] = largest.as_ref().unwrap().encode().to_owned();
        c.get_mut_edit()
            .set_compact_pointer(level, largest.unwrap());
    }

    /// Append new version to version set.
    pub fn append_version(&mut self, v: Arc<RwLock<Version>>) {
        if let Some(c) = &self.current {
            assert!(!Arc::ptr_eq(&v, c));
        }
        // Append to linked list
        self.current = Some(v.clone());
        let tmp_v = v.clone();
        let mut v = v.write().unwrap();

        v.prev = self.dummy_versions.read().unwrap().prev.clone();
        v.next = Some(self.dummy_versions.clone());
        v.prev.as_mut().unwrap().write().unwrap().next = Some(tmp_v.clone());
        v.next.as_mut().unwrap().write().unwrap().prev = Some(tmp_v);
    }

    // Allocate and return a new file number
    pub(crate) fn new_file_num(&mut self) -> u64 {
        let tmp = self.next_file_number;
        self.next_file_number += 1;
        tmp
    }

    pub(crate) fn get_last_sequence(&self) -> u64 {
        self.last_sequence
    }

    pub(crate) fn set_last_sequence(&mut self, s: u64) {
        self.last_sequence = s;
    }

    pub(crate) fn get_log_number(&self) -> u64 {
        self.log_number
    }

    pub(crate) fn current_version(&self) -> Arc<RwLock<Version>> {
        self.current.clone().unwrap()
    }

    // for test
    #[allow(dead_code)]
    pub(crate) fn set_manifest_num(&mut self, num: u64) {
        self.manifest_file_num = num;
    }

    pub(crate) fn get_manifest_num(&self) -> u64 {
        self.manifest_file_num
    }

    // for test
    pub(crate) fn max_next_level_overlapping_bytes(&self) -> u64 {
        let mut ret = 0;
        let mut overlaps = vec![];
        for level in 1..LEVEL_NUMBER {
            let current = self.current_version();
            let current = current.read().unwrap();
            for file in current.num_files(level) {
                let s = file.get_smallest().clone();
                let l = file.get_largest().clone();
                current.get_overlapping_files(
                    (level + 1) as i64,
                    &Some(s),
                    &Some(l),
                    &mut overlaps,
                );
                let sum = total_file_size(&overlaps);
                if sum > ret {
                    ret = sum;
                }
            }
        }
        ret
    }
}

// TODO:
#[derive(Debug, Default)]
pub struct LevelSummaryStorage {
    buffer: String,
}
