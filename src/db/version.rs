use crate::db::config::LEVEL_NUMBER;
use crate::db::file_meta::FileMetaData;
use crate::db::format::ValueType;
use crate::db::iterator::LevelFileNumIter;
use crate::db::key::{parse_internal_key, InternalKey, LookUpKey, ParsedInternalKey};
use crate::db::options::Options;
use crate::db::version_manager::{find_file, max_grand_parent_overlap_bytes};
use crate::db::{
    DError, DResult, ErrorIterator, ReadOptions, TwoLevelIter, MAX_MEM_COMPACT_LEVEL,
    MAX_SEQUENCE_NUMBER,
};
use crate::table::TableCache;
use crate::utils::cmp::{Comparator, InternalKeyComparator};
use crate::utils::coding::decode_u64_le;
use crate::utils::iter::Iter;
use atomic_float::AtomicF64;
use std::cmp::Ordering;
use std::fmt;
use std::sync::atomic::AtomicI64;
use std::sync::{atomic, Arc, RwLock};

pub struct Version {
    table_cache: Arc<TableCache>,
    // Level that should be compacted next and its compaction score.
    // Score < 1 means compaction is not strictly needed.
    // These fields are initialized by finalize().
    compaction_level: AtomicI64,
    compaction_score: AtomicF64,
    files: [Vec<Arc<FileMetaData>>; LEVEL_NUMBER],
    pub(crate) next: Option<Arc<RwLock<Version>>>,
    pub(crate) prev: Option<Arc<RwLock<Version>>>,
    // file to compact and level
    pub(crate) file_to_compact_and_level: (Option<Arc<FileMetaData>>, i64),
    // use `table_cache` and `icmp` and `opt` to replace VersionManager pointer.
    // Decoupling version and version_manager, only version_manager contains version
    pub(crate) icmp: Arc<dyn InternalKeyComparator>,
    pub(crate) opt: Options,
}

pub fn get_file_iterator(read_opt: ReadOptions, s: &[u8], tc: Arc<TableCache>) -> Box<dyn Iter> {
    if s.len() != 16 {
        Box::new(ErrorIterator::new())
    } else {
        tc.new_iter(read_opt, decode_u64_le(s), decode_u64_le(&s[8..]))
            .0
    }
}

#[derive(Default)]
pub struct GetStats {
    seek_file: Option<Arc<FileMetaData>>,
    seek_file_level: i64,
}

#[derive(Default)]
struct State {
    stats: GetStats,
    matches: u64,
}

fn match_state(state: &mut State, level: i64, f: Arc<FileMetaData>) -> bool {
    state.matches += 1;
    if state.matches == 1 {
        // Remember first match.
        state.stats.seek_file = Some(f);
        state.stats.seek_file_level = level;
    }
    // We can stop iterating once we have a second match.
    state.matches < 2
}

impl Version {
    pub fn new(
        icmp: Arc<dyn InternalKeyComparator>,
        table_cache: Arc<TableCache>,
        opt: Options,
    ) -> Arc<RwLock<Self>> {
        let files: [Vec<_>; LEVEL_NUMBER] = Default::default();
        let v = Self {
            files,
            file_to_compact_and_level: (None, -1),
            compaction_score: AtomicF64::new(-1.0),
            next: None,
            prev: None,
            compaction_level: AtomicI64::new(-1),
            icmp,
            table_cache,
            opt,
        };

        let rv = Arc::new(RwLock::new(v));
        rv.write().unwrap().next = Some(rv.clone());
        rv.write().unwrap().prev = Some(rv.clone());
        rv
    }

    pub fn get_compaction_level(&self) -> i64 {
        self.compaction_level.load(atomic::Ordering::Acquire)
    }

    pub fn set_compaction_level(&self, v: i64) {
        self.compaction_level.store(v, atomic::Ordering::Release);
    }

    pub fn get_compaction_score(&self) -> f64 {
        self.compaction_score.load(atomic::Ordering::Acquire)
    }

    pub fn set_compaction_score(&self, v: f64) {
        self.compaction_score.store(v, atomic::Ordering::Release);
    }

    // Iterate all level files, and update State matches to determine if compaction is required
    fn for_each_overlapping(
        &self,
        user_key: &[u8],
        internal_key: &[u8],
        state: &mut State,
        mut func: impl FnMut(&mut State, i64, Arc<FileMetaData>) -> bool,
    ) {
        let ucmp = self.icmp.user_comparator();
        // Search level-0 in order from newest to oldest.
        let mut tmp: Vec<Arc<FileMetaData>> = vec![];
        tmp.reserve(self.num_files_count(0));
        for i in 0..self.num_files_count(0) {
            let f: &Arc<FileMetaData> = &self.num_files(0)[i];
            compare_and_insert(f.clone(), &mut tmp, ucmp.clone(), user_key);
        }
        if !tmp.is_empty() {
            tmp.sort_by(newest_first);
            for i in tmp {
                if !func(state, 0, i.clone()) {
                    return;
                }
            }
        }
        // Search other levels.
        for level in 1..LEVEL_NUMBER {
            let num_files = self.num_files_count(level);
            if num_files == 0 {
                continue;
            }
            // Binary search to find earliest index whose largest key >= internal_key.
            let index = find_file(self.icmp.clone(), self.num_files(level), internal_key);
            if index < num_files {
                let f: &Arc<FileMetaData> = &self.num_files(level)[index];
                if ucmp.lt(user_key, f.get_smallest().user_key()) {
                    // All of "f" is past any data for user_key
                } else if !func(state, level as i64, f.clone()) {
                    return;
                }
            }
        }
    }

    // new_concatenating_iterator returns an iterator that concatenates its input.
    // Walking the resultant iterator will walk each input iterator in turn,
    // exhausting each input before moving on to the next.
    //
    // The sequence of the combined inputs' keys are assumed to be in strictly
    // increasing order: iters[i]'s last key is less than iters[i+1]'s first key.
    fn new_concatenating_iterator(&self, opt: ReadOptions, level: usize) -> impl Iter {
        let tc = self.table_cache.clone();
        TwoLevelIter::new(
            Box::new(LevelFileNumIter::new(
                self.icmp.clone(),
                self.num_files(level).clone(),
            )),
            opt,
            move |read_opt, s| get_file_iterator(read_opt, s, tc.clone()),
        )
    }

    #[allow(clippy::never_loop)]
    pub fn get(
        &mut self,
        read_opt: ReadOptions,
        k: &LookUpKey,
        stats: &mut GetStats,
    ) -> DResult<Vec<u8>> {
        let ikey = k.internal_key();
        let user_key = k.user_key();
        let ucmp = self.icmp.user_comparator();

        stats.seek_file = None;
        stats.seek_file_level = -1;
        let mut last_file_read = None;
        let mut last_file_read_level = -1;

        // We can search level-by-level since entries never hop across
        // levels. Therefore we are guaranteed that if we find data
        // in an smaller level, later levels are irrelevant.
        let mut tmp: Vec<Arc<FileMetaData>> = vec![];
        let mut tmp2;
        let mut tmp3 = vec![];
        for level in 0..LEVEL_NUMBER {
            let mut num_files = self.num_files_count(level);
            if num_files == 0 {
                continue;
            }
            // Get the list of files to search in this level
            let mut files: &Vec<Arc<FileMetaData>> = self.num_files(level);
            if level == 0 {
                // Level-0 files may overlap each other.  Find all files that
                // overlap user_key and process them in order from newest to oldest.
                for nf in files.iter().take(num_files) {
                    compare_and_insert(nf.clone(), &mut tmp, ucmp.clone(), user_key);
                }
                if tmp.is_empty() {
                    continue;
                }
                tmp.sort_by(newest_first);
                files = &tmp;
                num_files = tmp.len();
            } else {
                // Binary search to find earliest index whose largest key >= ikey.
                let index = find_file(self.icmp.clone(), self.num_files(level), ikey);
                if index >= num_files {
                    break;
                } else {
                    tmp2 = files[index].clone();
                    if ucmp.lt(user_key, tmp2.get_smallest().user_key()) {
                        break;
                    } else {
                        tmp3.clear();
                        tmp3.push(tmp2.clone());
                        files = &tmp3;
                        num_files = 1;
                    }
                }
            }
            for i in files.iter().take(num_files) {
                if last_file_read.is_some() && stats.seek_file.is_none() {
                    // We have had more than one seek for this read.  Charge the 1st file.
                    stats.seek_file = last_file_read;
                    stats.seek_file_level = last_file_read_level;
                }
                let f = i.clone();
                last_file_read = Some(f.clone());
                last_file_read_level = level as i64;
                //todo
                let mut saver =
                    Saver::new(SaverState::NotFound, ucmp.clone(), user_key.to_owned());

                self.table_cache.get(
                    read_opt.clone(),
                    f.get_file_number(),
                    f.get_file_size(),
                    ikey,
                    &mut saver,
                    save_value,
                )?;
                match saver.state {
                    SaverState::NotFound => {}
                    SaverState::Found => {
                        return Ok(saver.value);
                    }
                    SaverState::Deleted => return Err(DError::RecordNotFound),
                    SaverState::Corrupt => return Err(DError::CorruptedKey),
                }
            }
        }
        Err(DError::RecordNotFound)
    }

    // Append to `iters` a sequence of iterators that will
    // yield the contents of this Version when merged together.
    pub fn add_iters(&self, opt: ReadOptions, list: &mut Vec<Box<dyn Iter>>) {
        // Merge all level zero files together since they may overlap
        let level_0_files: &Vec<Arc<FileMetaData>> = self.num_files(0);
        for (_, f) in level_0_files.iter().enumerate() {
            let number = f.get_file_number();
            let size = f.get_file_size();
            list.push(self.table_cache.new_iter(opt.clone(), number, size).0)
        }
        // For levels > 0, we can use a concatenating iterator that sequentially
        // walks through the non-overlapping files in the level, opening them
        // lazily.
        for level in 1..LEVEL_NUMBER {
            if !self.num_files(level).is_empty() {
                list.push(Box::new(
                    self.new_concatenating_iterator(opt.clone(), level),
                ));
            }
        }
    }

    /// Return the level at which we should place a new memtable compaction
    /// result that covers the range [smallest_user_key,largest_user_key].
    /// * if overlap in level 0 return:
    /// * if not overlap in level n (n<2) and overlap files total size < 20M, return n+1.
    /// Only use `Version.files` and `icmp`.
    pub fn pick_level_for_mem_table_output(
        &self,
        smallest_user_key: &[u8],
        largest_user_key: &[u8],
    ) -> i64 {
        let mut level = 0i64;
        if !self.is_overlapped_in_level(0, Some(smallest_user_key), Some(largest_user_key)) {
            // Push to next level if there is no overlap in next level,
            // and the #bytes overlapping in the level after that are limited.
            let start = Some(InternalKey::new(
                smallest_user_key,
                MAX_SEQUENCE_NUMBER,
                ValueType::TypeValue,
            ));
            let limit = Some(InternalKey::new(
                largest_user_key,
                0,
                ValueType::TypeDeletion,
            ));
            let mut overlaps = vec![];
            while level < MAX_MEM_COMPACT_LEVEL as i64 {
                if self.is_overlapped_in_level(
                    level + 1,
                    Some(smallest_user_key),
                    Some(largest_user_key),
                ) {
                    break;
                }
                if level + 2 < LEVEL_NUMBER as i64 {
                    self.get_overlapping_files(level + 2, &start, &limit, &mut overlaps);
                    let sum = total_file_size(&overlaps);
                    if sum > max_grand_parent_overlap_bytes(self.opt.clone()) {
                        break;
                    }
                }
                level += 1;
            }
        }
        level
    }

    // Store in `inputs` all files in `level` that overlap [begin, end]
    // Level-0 files may overlap each other. So check if the newly
    // added file has expanded the range, if so, restart search.
    #[allow(clippy::if_same_then_else)]
    pub fn get_overlapping_files(
        &self,
        level: i64,
        begin: &Option<InternalKey>,
        end: &Option<InternalKey>,
        inputs: &mut Vec<Arc<FileMetaData>>,
    ) {
        assert!(level >= 0);
        assert!(level < LEVEL_NUMBER as i64);
        inputs.clear();
        let mut user_begin = vec![];
        let mut user_end = vec![];
        if let Some(b) = begin {
            user_begin = b.user_key().to_vec();
        }
        if let Some(e) = end {
            user_end = e.user_key().to_vec();
        }
        let user_cmp = self.icmp.user_comparator();
        let mut i = 0;
        while i < self.num_files_count(level as usize) {
            let ff: &Arc<FileMetaData> = &self.num_files(level as usize)[i];
            i += 1;
            let file_start = ff.get_smallest().user_key();
            let file_limit = ff.get_largest().user_key();
            // "f" is completely before specified range [user_begin, user_end]; skip it
            if begin.is_some() && user_cmp.lt(file_limit, &user_begin) {
                // Nothing to do
            } else if end.is_some() && user_cmp.gt(file_start, &user_end) {
                // Nothing to do
            } else {
                // Append overlaping files.
                inputs.push(ff.clone());
                if level == 0 {
                    // Level-0 files may overlap each other.  So check if the newly
                    // added file has expanded the range.  If so, restart search.
                    if begin.is_some() && user_cmp.lt(file_start, &user_begin) {
                        user_begin = file_start.to_vec();
                        inputs.clear();
                        i = 0;
                    } else if end.is_some() && user_cmp.gt(file_limit, &user_end) {
                        user_end = file_limit.to_vec();
                        inputs.clear();
                        i = 0;
                    }
                }
            }
        }
    }

    /// Returns true if some file in the specified level overlaps
    /// some part of [smallest_user_key, largest_user_key].
    /// smallest_user_key==None represents a key smaller than all the DB's keys.
    /// largest_user_key==None represents a key largest than all the DB's keys.
    pub(crate) fn is_overlapped_in_level(
        &self,
        level: i64,
        smallest_user_key: Option<&[u8]>,
        largest_user_key: Option<&[u8]>,
    ) -> bool {
        has_overlapped_files_in_range(
            self.icmp.clone(),
            level > 0,
            self.num_files(level as usize),
            smallest_user_key,
            largest_user_key,
        )
    }

    // Record a sample of bytes read at the specified internal key.
    // Samples are taken approximately once every `READ_BYTES_PERIOD`
    // bytes. Returns true if a new compaction may need to be triggered.
    pub fn record_read_sample(&mut self, internal_key: &[u8]) -> bool {
        let mut ikey = ParsedInternalKey::default();
        if !parse_internal_key(internal_key, &mut ikey) {
            return false;
        }

        let mut state = State::default();
        self.for_each_overlapping(ikey.user_key(), internal_key, &mut state, match_state);

        if state.matches >= 2 {
            return self.update_stats(&state.stats);
        }
        false
    }

    // Adds "stats" into the current state. Returns true if a new
    // compaction may need to be triggered, false otherwise.
    pub fn update_stats(&mut self, stats: &GetStats) -> bool {
        let f = stats.seek_file.clone();
        if let Some(f) = f {
            f.decrease_seeks();
            if f.allow_seeks() <= 0 && self.file_to_compact_and_level.0.is_none() {
                self.file_to_compact_and_level = (Some(f), stats.seek_file_level);
            }
            return true;
        }
        false
    }

    pub fn num_files_count(&self, num: usize) -> usize {
        self.files[num].len()
    }

    pub fn num_files(&self, num: usize) -> &Vec<Arc<FileMetaData>> {
        &self.files[num]
    }

    pub fn set_files(&mut self, files: [Vec<Arc<FileMetaData>>; LEVEL_NUMBER]) {
        self.files = files;
    }
}

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        ignore!(write!(f, "file {:?}", self.files));
        ignore!(write!(f, "compaction_level: {:?}", self.compaction_level));
        ignore!(write!(f, "compaction_score: {:?}", self.compaction_score));
        let _ = write!(
            f,
            "file_to_compact_and_level: {:?}",
            self.file_to_compact_and_level
        );
        if let Some(n) = &self.next {
            ignore!(write!(f, "next {:?}", *n.read().unwrap()));
        } else {
            ignore!(write!(f, "next None"));
        }
        if let Some(p) = &self.prev {
            write!(f, "prev {:?}", *p.read().unwrap())
        } else {
            write!(f, "prev None")
        }
    }
}

pub fn total_file_size<'a, I>(files: I) -> u64
where
    I: IntoIterator<Item = &'a Arc<FileMetaData>>,
{
    let mut sum = 0;
    for i in files {
        sum += i.get_file_size();
    }
    sum
}

/// sort by file number in descending order: e.g. 3, 2, 1
fn newest_first(a: &Arc<FileMetaData>, b: &Arc<FileMetaData>) -> Ordering {
    b.get_file_number().cmp(&a.get_file_number())
}

/// * user_key is none, return false.
/// * user_key is greater than file largest user_key, it should be after file.
fn after_file(cmp: Arc<dyn Comparator>, user_key: Option<&[u8]>, f: Arc<FileMetaData>) -> bool {
    user_key.is_some() && cmp.gt(user_key.as_ref().unwrap(), f.get_largest().user_key())
}

/// * user_key is none, return false.
/// * user_key is smaller than file smallest user_key, it should be before file.
fn before_file(cmp: Arc<dyn Comparator>, user_key: Option<&[u8]>, f: Arc<FileMetaData>) -> bool {
    user_key.is_some() && cmp.lt(user_key.as_ref().unwrap(), f.get_smallest().user_key())
}

fn compare_and_insert(
    f: Arc<FileMetaData>,
    tmp: &mut Vec<Arc<FileMetaData>>,
    cmp: Arc<dyn Comparator>,
    user_key: &[u8],
) {
    if cmp.ge(user_key, f.get_smallest().user_key())
        && cmp.le(user_key, f.get_largest().user_key())
    {
        tmp.push(f);
    }
}

/// * level 0 files are disjoint, so we need check all files.
/// * level n > 0 files are sorted, so we use binary search to check the list.
pub fn has_overlapped_files_in_range(
    icmp: Arc<dyn InternalKeyComparator>,
    disjoint_sorted_files: bool, // level0 files are disjoint, so value is true.
    files: &[Arc<FileMetaData>],
    smallest_user_key: Option<&[u8]>,
    largest_user_key: Option<&[u8]>,
) -> bool {
    let user_cmp = icmp.user_comparator();
    if !disjoint_sorted_files {
        // Need to check against all files for level 0
        for f in files {
            if after_file(user_cmp.clone(), smallest_user_key, f.clone())
                || before_file(user_cmp.clone(), largest_user_key, f.clone())
            {
                // No overlap
            } else {
                return true;
            }
        }
        return false;
    }
    let mut index = 0;
    // check smallest_user_key
    if let Some(s) = smallest_user_key {
        let small = InternalKey::new(s, MAX_SEQUENCE_NUMBER, ValueType::TypeValue);
        index = find_file(icmp, files, small.encode());
    }
    if index >= files.len() {
        // beginning of range is after all files, so no overlap.
        return false;
    }
    // check largest_user_key
    !before_file(user_cmp, largest_user_key, files[index].clone())
}

#[derive(Eq, PartialEq)]
pub enum SaverState {
    NotFound,
    Found,
    Deleted,
    Corrupt,
}

pub struct Saver {
    state: SaverState,
    ucmp: Arc<dyn Comparator>,
    user_key: Vec<u8>,
    value: Vec<u8>,
}

impl Saver {
    pub fn new(state: SaverState, ucmp: Arc<dyn Comparator>, user_key: Vec<u8>) -> Self {
        Saver {
            state,
            ucmp,
            user_key,
            value: vec![],
        }
    }
}

fn save_value(s: &mut Saver, ikey: &[u8], value: &[u8]) {
    let mut parsed = ParsedInternalKey::default();
    if !parse_internal_key(ikey, &mut parsed) {
        s.state = SaverState::Corrupt;
    } else if s.ucmp.eq(parsed.user_key(), s.user_key.as_slice()) {
        s.state = if parsed.value_type() == ValueType::TypeValue {
            SaverState::Found
        } else {
            SaverState::Deleted
        };
        if s.state == SaverState::Found {
            s.value = value.to_vec();
        }
    }
}
