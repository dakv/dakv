use crate::db::config::LEVEL_NUMBER;
use crate::db::file_meta::FileMetaData;
use crate::db::format::SequenceNumber;
use crate::db::key::InternalKey;
use crate::db::options::Options;
use crate::db::version::{total_file_size, Version};
use crate::db::version_edit::VersionEdit;
use crate::db::version_manager::{max_file_size_for_level, max_grand_parent_overlap_bytes};
use crate::env::WritableFile;
use crate::table::TableBuilder;
use std::mem::MaybeUninit;
use std::ops::AddAssign;
use std::sync::{Arc, RwLock};

pub(crate) struct Compaction {
    /// Return the level that is being compacted.
    /// Inputs from "level" and "level+1" will be merged to produce a set of "level+1" files.
    level: i64,
    /// Maximum size of files to build during this compaction.
    max_output_file_size: u64,
    /// Some output key has been seen
    seen_key: bool,
    /// Bytes of overlap between current output
    overlapped_bytes: u64,
    /// level_ptr holds indices into input_version->levels: our state
    /// is that we are positioned at one of the file ranges for each
    /// higher level than the ones involved in this compaction (i.e. for
    /// all L >= level + 2).
    level_ptr: [usize; LEVEL_NUMBER],
    /// Modify this VersionEdit attribute for adding files and deleting files.
    /// Generate new version by using Version and VersionEdit in `log_and_apply` method.
    edit: VersionEdit,
    /// Each compaction reads inputs from `level` and `level+1`
    /// inputs[0] -> level[n]
    /// inputs[1] -> level[n+1]
    inputs: [Vec<Arc<FileMetaData>>; 2],
    input_version: Option<Arc<RwLock<Version>>>,
    /// State used to check for number of of overlapping grandparent files.
    /// If the new parent table file has a lot of overlapping files at
    /// the grandparent level, a very expensive merge will be required later.
    /// parent: level + 1, grandparent: level + 2
    grandparents: Vec<Arc<FileMetaData>>,
    /// Index in grandparents
    grandparent_index: usize,
}

impl Compaction {
    pub fn new(options: Options, level: i64) -> Self {
        Self {
            max_output_file_size: max_file_size_for_level(options),
            inputs: [vec![], vec![]],
            input_version: None,
            grandparents: vec![],
            level,
            level_ptr: [0; LEVEL_NUMBER],
            overlapped_bytes: Default::default(),
            seen_key: Default::default(),
            grandparent_index: Default::default(),
            edit: Default::default(),
        }
    }

    pub(crate) fn set_input_version(&mut self, v: Option<Arc<RwLock<Version>>>) {
        self.input_version = v;
    }

    pub(crate) fn get_mut_grandparents(&mut self) -> &mut Vec<Arc<FileMetaData>> {
        &mut self.grandparents
    }

    pub(crate) fn get_mut_edit(&mut self) -> &mut VersionEdit {
        &mut self.edit
    }

    pub(crate) fn get_input(&self, i: usize) -> &Vec<Arc<FileMetaData>> {
        &self.inputs[i]
    }

    pub(crate) fn get_mut_input(&mut self, i: usize) -> &mut Vec<Arc<FileMetaData>> {
        &mut self.inputs[i]
    }

    pub(crate) fn input(&self, w: usize, i: usize) -> Arc<FileMetaData> {
        self.inputs[w][i].clone()
    }

    pub(crate) fn set_input(&mut self, i: usize, input: Vec<Arc<FileMetaData>>) {
        self.inputs[i] = input;
    }

    pub(crate) fn level(&self) -> i64 {
        self.level
    }

    pub(crate) fn max_output_file_size(&self) -> u64 {
        self.max_output_file_size
    }

    pub(crate) fn num_input_files(&self, i: usize) -> usize {
        assert!(i <= 1);
        self.inputs[i].len()
    }

    /// Is this a trivial compaction that can be implemented by just
    /// moving a single input file to the next level (no merging or splitting)
    pub fn is_trivial_move(&self) -> bool {
        // Avoid a move if there is lots of overlapping grandparent data.
        // Otherwise, the move could create a parent file that will require
        // a very expensive merge later on.
        let opt = self
            .input_version
            .as_ref()
            .unwrap()
            .read()
            .unwrap()
            .opt
            .clone();
        self.num_input_files(0) == 1
            && self.num_input_files(1) == 0
            && total_file_size(&self.grandparents) <= max_grand_parent_overlap_bytes(opt)
    }

    /// If the compaction is successful, remove the input Version.
    pub fn release_inputs(&mut self) {
        if self.input_version.is_some() {
            self.input_version = None;
        }
    }

    /// Delete all inputs and put inputs to VersionEdit
    pub(crate) fn delete_all_inputs(&mut self) {
        for which in 0..2usize {
            for i in 0..self.num_input_files(which) {
                self.edit.delete_files(
                    self.level + which as i64,
                    self.inputs[which][i].get_file_number(),
                )
            }
        }
    }

    /// Returns true if the information we have available guarantees that
    /// the compaction is producing data in `level+1` for which no data exists
    /// in levels greater than `level+1`.
    pub fn is_base_level_for_key(&mut self, user_key: &[u8]) -> bool {
        let user_cmp = self
            .input_version
            .as_ref()
            .unwrap()
            .read()
            .unwrap()
            .icmp
            .user_comparator();
        for lvl in self.level as usize + 2..LEVEL_NUMBER {
            let tmp = self.input_version.as_ref().unwrap().read().unwrap();
            let files: &Vec<Arc<FileMetaData>> = tmp.num_files(lvl);
            while self.level_ptr[lvl] < files.len() {
                let f: &Arc<FileMetaData> = &files[self.level_ptr[lvl]];
                if user_cmp.le(user_key, f.get_largest().user_key()) {
                    if user_cmp.ge(user_key, f.get_smallest().user_key()) {
                        return false;
                    }
                    break;
                }
                self.level_ptr[lvl] += 1;
            }
        }
        true
    }

    /// Returns true if we should stop building the current output
    /// before processing "internal_key".
    pub fn should_stop_before(&mut self, internal_key: &[u8]) -> bool {
        let icmp = self
            .input_version
            .as_ref()
            .unwrap()
            .read()
            .unwrap()
            .icmp
            .clone();
        let opt = self
            .input_version
            .as_ref()
            .unwrap()
            .read()
            .unwrap()
            .opt
            .clone();
        while self.grandparent_index < self.grandparents.len()
            && icmp.gt(
                internal_key,
                self.grandparents[self.grandparent_index]
                    .get_largest()
                    .encode(),
            )
        {
            if self.seen_key {
                self.overlapped_bytes += self.grandparents[self.grandparent_index].get_file_size();
            }
            self.grandparent_index += 1;
        }
        self.seen_key = true;
        // Too much overlap for current output; start new output
        if self.overlapped_bytes > max_grand_parent_overlap_bytes(opt) {
            self.overlapped_bytes = 0;
            true
        } else {
            false
        }
    }
}

// Files produced by compaction
#[derive(Default)]
pub(crate) struct Output {
    file_number: u64,
    file_size: u64,
    smallest: InternalKey,
    largest: InternalKey,
}

impl Output {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_largest_from_slice(&mut self, key: &[u8]) {
        self.largest.rep.clear();
        self.largest.rep.extend_from_slice(key);
    }

    pub fn set_smallest_from_slice(&mut self, key: &[u8]) {
        self.smallest.rep.clear();
        self.smallest.rep.extend_from_slice(key);
    }

    pub fn get_number(&self) -> u64 {
        self.file_number
    }

    pub(crate) fn set_file_number(&mut self, number: u64) {
        self.file_number = number
    }

    pub(crate) fn set_file_size(&mut self, file_size: u64) {
        self.file_size = file_size
    }

    pub fn get_file_size(&self) -> u64 {
        self.file_size
    }

    pub fn get_smallest(&self) -> &InternalKey {
        &self.smallest
    }

    pub fn get_largest(&self) -> &InternalKey {
        &self.largest
    }
}

pub(crate) struct CompactionState<'a> {
    pub(crate) compaction: &'a mut Compaction,
    pub(crate) output: Vec<Output>,
    pub(crate) outfile: Option<Arc<dyn WritableFile>>,
    pub(crate) builder: Option<TableBuilder>,
    pub(crate) total_bytes: u64,
    /// Sequence numbers < smallest_snapshot are not significant since we
    /// will never have to service a snapshot below smallest_snapshot.
    /// Therefore if we have seen a sequence number S <= smallest_snapshot,
    /// we can drop all entries for the same key with sequence numbers < S.
    pub(crate) smallest_snapshot: SequenceNumber,
}

impl<'a> CompactionState<'a> {
    pub fn new(compaction: &'a mut Compaction) -> Self {
        Self {
            compaction,
            output: vec![],
            outfile: None,
            total_bytes: 0,
            builder: None,
            smallest_snapshot: 0,
        }
    }

    /// return last output mutable reference
    pub fn current_output(&mut self) -> &mut Output {
        self.output.last_mut().unwrap()
    }
}

/// Per level compaction stats.  stats[level] stores the stats for
/// compactions that produced data for the specified `level`.
#[derive(Default, PartialEq, Clone)]
pub(crate) struct CompactionStats {
    pub(crate) micros: f64,
    pub(crate) read_bytes: u64,
    pub(crate) written_bytes: u64,
}

impl CompactionStats {
    pub fn create_slice() -> [CompactionStats; LEVEL_NUMBER] {
        let mut array: [MaybeUninit<CompactionStats>; LEVEL_NUMBER] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for (_, element) in array.iter_mut().enumerate() {
            let tmp = CompactionStats::default();
            *element = MaybeUninit::new(tmp);
        }

        unsafe { std::mem::transmute::<_, [CompactionStats; LEVEL_NUMBER]>(array) }
    }
}

impl AddAssign for CompactionStats {
    fn add_assign(&mut self, other: Self) {
        *self = Self {
            micros: self.micros + other.micros,
            read_bytes: self.read_bytes + other.read_bytes,
            written_bytes: self.written_bytes + other.written_bytes,
        };
    }
}

pub(crate) struct ManualCompaction {
    pub(crate) level: i64,
    pub(crate) done: bool,
    pub(crate) begin: Option<InternalKey>,
    pub(crate) end: Option<InternalKey>,
    pub(crate) tmp: InternalKey,
}
