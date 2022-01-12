use crate::db::builder::build_table;
use crate::db::compaction::{
    Compaction, CompactionState, CompactionStats, ManualCompaction, Output,
};
use crate::db::db_iter::DBIter;
use crate::db::file::{
    consume_decimal_num, current_file_name, info_log_file, lock_file_name, log_file_name,
    manifest_file_name, old_info_log_file, parse_file_name, set_current_file, FileType,
};
use crate::db::format::VALUE_TYPE_FOR_SEEK;
use crate::db::key::LookUpKey;
use crate::db::log_reader::Reader;
use crate::db::log_writer::Writer as LogWriter;
use crate::db::memory_table::MemIter;
use crate::db::snapshot::{SnapNode, SnapshotList};
use crate::db::version::{GetStats, Version};
use crate::db::version_edit::VersionEdit;
use crate::db::version_manager::{LevelSummaryStorage, VersionManager};
use crate::db::write_batch::BatchWriter;
use crate::db::{
    new_merging_iterator, parse_internal_key, table_file_name, DError, DResult, FileMetaData,
    InternalKey, MemoryTable, ParsedInternalKey, SequenceNumber, ValueType, WriteBatch,
    L0_SLOWDOWN_WRITES_TRIGGER, L0_STOP_WRITES_TRIGGER, LEVEL_NUMBER, MAX_SEQUENCE_NUMBER,
    NUM_NON_TABLE_CACHE_FILES,
};
use crate::env::Env;
use crate::table::{InternalFilterPolicy, TableBuilder, TableCache};
use crate::utils::cmp::{Comparator, InternalKeyComparator, InternalKeyComparatorImpl};
use crate::utils::constants::{
    APPROXIMATE_MEM_USAGE_PROPERTY, NUM_FILES_PROPERTY, PROPERTY_PREFIX, STATS_PROPERTY,
};
use crate::utils::iter::Iter;
use crate::utils::lru_cache::SharedLRUCache;
use crate::utils::time::get_micro;
use crate::{Database, Options, ReadOptions, WriteOptions};
use crossbeam_channel::{Receiver, RecvError, SendError, Sender};
use logger::set_logger_level;
use slog::{debug, error, info};
use std::collections::{HashSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::{io, mem as std_mem, thread};

struct Channel<T> {
    send: Sender<T>,
    recv: Receiver<T>,
}

impl<T> Channel<T> {
    fn new() -> Self {
        let (send, recv) = crossbeam_channel::unbounded();
        Self { send, recv }
    }
    fn send(&self, msg: T) -> Result<(), SendError<T>> {
        self.send.send(msg)
    }

    fn recv(&self) -> Result<T, RecvError> {
        self.recv.recv()
    }
}

pub struct Inner {
    // Database property
    db_name: String,
    options: Options,
    env: Arc<dyn Env + Send + Sync>,
    internal_comparator: Arc<dyn InternalKeyComparator + Send + Sync>,
    snapshots: SnapshotList,
    seed: AtomicU32, // random seed for DBIter
    bg_error: RwLock<DResult<()>>,
    shutting_down: AtomicBool,
    // Table, TableCache and VersionManager
    mem: RwLock<Option<MemoryTable>>, // RwLock makes it easy to modify.
    imm: RwLock<Option<MemoryTable>>,
    has_imm: AtomicBool,
    table_cache: Arc<TableCache>,
    writers: Mutex<VecDeque<BatchWriter>>,
    version_manager: Mutex<VersionManager>,
    // Log file
    log_file_num: AtomicU64,
    log: Mutex<Option<LogWriter>>,
    // For compaction
    pending_outputs: Arc<Mutex<HashSet<u64>>>,
    manual_compaction: Arc<Mutex<Option<Arc<Mutex<ManualCompaction>>>>>,
    stats: Arc<Mutex<[CompactionStats; LEVEL_NUMBER]>>,
    background_compaction_scheduled: AtomicBool,
    // Signals
    compact_channel: Channel<()>,
    background_work_finished_channel: Channel<()>, // fix `test_compact_mem_table` stuck issue, lock after being notified, so use Channel to replace it.
    process_batch_channel: Channel<()>,
    // todo db_lock
}

fn clip_to_range<T>(v: &mut T, min: T, max: T)
where
    T: PartialOrd,
{
    if *v > max {
        *v = max;
    } else if *v < min {
        *v = min;
    }
}

struct Discard;

impl io::Write for Discard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn sanitize_options(
    db_name: &str,
    src_opt: Options,
    icmp: Arc<dyn InternalKeyComparator + Send + Sync>,
) -> Options {
    let mut opt = src_opt;
    opt.comparator = icmp.into_cmp();
    if let Some(f) = opt.filter_policy {
        let p = InternalFilterPolicy::new(f);
        opt.filter_policy = Some(Arc::new(p));
    } else {
        opt.filter_policy = None;
    }
    clip_to_range(
        &mut opt.max_open_files,
        64 + NUM_NON_TABLE_CACHE_FILES,
        50000,
    );
    clip_to_range(&mut opt.write_buffer_size, 64 << 10, 1 << 30);
    clip_to_range(&mut opt.max_file_size, 1 << 20, 1 << 30);
    clip_to_range(&mut opt.block_size, 1 << 10, 4 << 20);

    if opt.info_log.is_none() {
        ignore!(opt.env.create_dir(db_name));
        ignore!(opt
            .env
            .rename_file(&info_log_file(db_name), &old_info_log_file(db_name)));
        let log_file = opt.env.new_log_file(&info_log_file(db_name));
        opt.info_log = Some(if let Some(l) = log_file {
            set_logger_level(l, true, None)
        } else {
            // for test, we can use `std::io::stdout()`
            set_logger_level(Discard, true, None)
        });
    }
    if opt.block_cache.is_none() {
        opt.block_cache = Some(Arc::new(Mutex::new(SharedLRUCache::new(8 << 20))));
    }
    opt
}

impl Inner {
    pub(crate) fn new(db_name: String, raw_options: Options) -> Self {
        let internal_comparator = Arc::new(InternalKeyComparatorImpl::new(
            raw_options.comparator.clone(),
        ));
        let options = sanitize_options(&db_name, raw_options.clone(), internal_comparator.clone());

        let table_cache = Arc::new(TableCache::new(
            db_name.clone(),
            options.clone(),
            table_cache_size(options.clone()),
        ));
        Inner {
            seed: AtomicU32::new(0),
            env: raw_options.env.clone(),
            pending_outputs: Arc::new(Mutex::new(HashSet::new())),
            manual_compaction: Arc::new(Mutex::new(None)),
            mem: RwLock::new(None),
            imm: RwLock::new(None),
            log: Mutex::new(None),
            log_file_num: AtomicU64::new(0),
            snapshots: SnapshotList::new(),
            writers: Mutex::new(VecDeque::new()),
            process_batch_channel: Channel::new(),
            version_manager: Mutex::new(VersionManager::new(
                table_cache.clone(),
                options.clone(),
                internal_comparator.clone(),
                db_name.clone(),
            )),
            background_work_finished_channel: Channel::new(),
            bg_error: RwLock::new(Ok(())),
            compact_channel: Channel::new(),
            background_compaction_scheduled: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
            has_imm: AtomicBool::new(false),
            stats: Arc::new(Mutex::new(CompactionStats::create_slice())),
            options,
            db_name,
            table_cache,
            internal_comparator,
        }
    }

    fn open(db_name: String, options: Options) -> DResult<Self> {
        let mut db = Self::new(db_name.clone(), options);
        debug!(db.options.info_log.as_ref().unwrap(), "open db");

        let mut edit = VersionEdit::new();
        let mut save_manifest = false;
        db.recover(&mut save_manifest, &mut edit)?;

        // if memory table is none, create new log file and new memory table
        if db.mem.read().unwrap().is_none() {
            let new_log_num = db.version_manager.lock().unwrap().new_file_num();
            let log_file = log_file_name(&db_name, new_log_num);
            let file = db.env.new_writable_file(&log_file)?;

            edit.set_log_number(new_log_num);

            db.log = Mutex::new(Some(LogWriter::new(file)));
            db.log_file_num.store(new_log_num, Ordering::Release);
            let c = MemoryTable::new(db.internal_comparator.clone());
            db.mem = RwLock::new(Some(c));
        }
        if save_manifest {
            edit.set_log_number(db.log_file_num.load(Ordering::Acquire));
            db.version_manager
                .lock()
                .unwrap()
                .log_and_append_version(&mut edit)?;
        }

        db.delete_obsolete_files();
        db.maybe_schedule_compaction();
        Ok(db)
    }

    fn new_db(&self, env: Arc<dyn Env + Send + Sync>, cmp_name: &str) -> DResult<()> {
        info!(
            self.options.info_log.as_ref().unwrap(),
            "new database {}",
            self.db_name.as_str()
        );
        let mut new_db = VersionEdit::new();
        new_db.set_comparator_name(cmp_name);
        new_db.set_log_number(0);
        new_db.set_next_file(2);
        new_db.set_last_sequence(0);

        let manifest = manifest_file_name(self.db_name.as_str(), 1);
        let log = &mut LogWriter::new(env.new_writable_file(&manifest).unwrap());
        let s = &mut vec![];
        new_db.encode_to(s);

        let result = log.write_record(s.as_slice());
        match result {
            Ok(_) => {
                info!(self.options.info_log.as_ref().unwrap(), "set_current_file");
                set_current_file(env.clone(), self.db_name.as_str(), 1)?;
            }
            Err(_) => {
                env.delete_file(&manifest)?;
            }
        }
        result
    }

    fn recover(&self, save_manifest: &mut bool, edit: &mut VersionEdit) -> DResult<()> {
        info!(self.options.info_log.as_ref().unwrap(), "recover database");
        let mut logs: Vec<u64> = vec![];
        let mut max_seq: SequenceNumber = 0;

        // Ignore error from `create_dir` since the creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        ignore!(self.env.create_dir(&self.db_name));
        self.env.lock_file(&self.db_name)?;
        if !self
            .env
            .file_exists(&current_file_name(self.db_name.as_str()))
        {
            if self.options.create_if_missing {
                self.new_db(self.env.clone(), self.user_comparator().name())?;
            } else {
                return Err(DError::CustomError(
                    "db does not exist (create_if_missing is false)",
                ));
            }
        } else if self.options.error_if_exists {
            return Err(DError::CustomError("db exists (error_if_exists is true)"));
        }
        self.version_manager
            .lock()
            .unwrap()
            .recover(save_manifest)?;
        // Recover from all newer log files than the ones named in the
        // descriptor (new log files may have been added by the previous
        // incarnation without registering them in the descriptor).
        let min_log = self.version_manager.lock().unwrap().get_log_number();

        let mut expected = self.version_manager.lock().unwrap().get_live_files();
        let mut num = 0;
        let file_type = &mut FileType::TempFile;
        // Get all files in the directory, and filter the log file, sort the log files and recover log file
        for file in self.env.get_children(&self.db_name)? {
            if parse_file_name(&file, &mut num, file_type) {
                expected.remove(&num);
                // Filter out log files,
                if *file_type == FileType::LogFile && (num >= min_log) {
                    logs.push(num);
                }
            }
        }

        if !expected.is_empty() {
            return Err(DError::CustomError("missing log files"));
        }
        logs.sort_unstable();

        for i in 0..logs.len() {
            self.recover_log_file(
                logs[i],
                i == logs.len() - 1,
                &mut max_seq,
                save_manifest,
                edit,
            )?;
            // The previous incarnation may not have written any MANIFEST
            // records after allocating this log number.  So we manually
            // update the file number allocation counter in VersionManager.
            self.version_manager
                .lock()
                .unwrap()
                .mark_file_number_used(logs[i]);
        }
        if self.version_manager.lock().unwrap().get_last_sequence() < max_seq {
            self.version_manager
                .lock()
                .unwrap()
                .set_last_sequence(max_seq);
        }
        Ok(())
    }

    fn maybe_ignore_error(&self, ret: &mut DResult<()>) {
        if ret.is_ok() || self.options.paranoid_checks {
            // No change needed
        } else {
            info!(
                self.options.info_log.as_ref().unwrap(),
                "Ignoring error {}",
                ret.clone().err().unwrap()
            );
            *ret = Ok(());
        }
    }

    /// Recover from log when restart db, and create a mem table.
    /// Write into level 0 if necessary.
    fn recover_log_file(
        &self,
        log_number: u64,
        last_log: bool,
        max_seq: &mut SequenceNumber,
        save_manifest: &mut bool,
        edit: &mut VersionEdit,
    ) -> DResult<()> {
        info!(
            self.options.info_log.as_ref().unwrap(),
            "recovering log {}", log_number
        );

        let file_name = log_file_name(self.db_name.as_str(), log_number);
        let mut ret: DResult<()> = Ok(());

        let file = self.env.new_sequential_file(&file_name);
        if file.is_err() {
            ret = Err(DError::CustomError("new_sequential_file"));
            self.maybe_ignore_error(&mut ret);
            return ret;
        }

        let mut reader = Reader::new(file.unwrap(), true);
        let mut batch = WriteBatch::new();
        let mut data = vec![];
        let mut compactions = 0;
        let mut mem = None;
        let cmp = self.internal_comparator.clone();
        let write_buffer_size = self.options.write_buffer_size;
        // Read the log and insert into mem table, check the memory usage and decide
        // whether to do minor compaction (write to level 0-MaxCompactLevel file).
        while reader.read_record(&mut data) && ret.is_ok() {
            if data.len() < 12 {
                error!(
                    self.options.info_log.as_ref().unwrap(),
                    "error data.len() < 12 "
                );
                // todo reporter
                continue;
            }
            batch.set_contents(data.as_slice());
            if mem.is_none() {
                mem = Some(MemoryTable::new(cmp.clone()));
            }

            ret = WriteBatch::insert_batch(&batch, &mut *mem.as_mut().unwrap());
            self.maybe_ignore_error(&mut ret);
            if ret.is_err() {
                break;
            }

            let last_seq = batch.sequence() + batch.count() as SequenceNumber - 1;
            if last_seq > *max_seq {
                *max_seq = last_seq;
            }

            if let Some(m) = &mem {
                if m.approximate_memory_usage() > write_buffer_size {
                    compactions += 1;
                    *save_manifest = true;
                    let ret = self.write_level0_table(m.new_iter(), edit, None);
                    mem = None;
                    if ret.is_err() {
                        break;
                    }
                }
            }
        }
        if self.options.reuse_logs && last_log && compactions == 0 {
            assert!(self.log.lock().unwrap().is_none());
            let mut lfile_size = 0;
            if self
                .env
                .clone()
                .get_file_size(&file_name, &mut lfile_size)
                .is_ok()
            {
                debug!(
                    self.options.info_log.as_ref().unwrap(),
                    "reusing old log {}", &file_name
                );
                *self.log.lock().unwrap() = Some(LogWriter::new_with_offset(
                    self.env.new_appendable_file(&file_name)?,
                    lfile_size as usize,
                ));
                self.log_file_num.store(log_number, Ordering::Release);
                if mem.is_some() {
                    *self.mem.write().unwrap() = mem;
                    mem = None;
                } else {
                    *self.mem.write().unwrap() = Some(MemoryTable::new(cmp.clone()));
                }
            }
            // Keep reusing the last log file
        }
        if mem.is_some() && ret.is_ok() {
            *save_manifest = true;
            ret = self.write_level0_table(mem.as_ref().unwrap().new_iter(), edit, None);
        }
        ret
    }

    fn maybe_schedule_compaction(&self) {
        if self.background_compaction_scheduled.load(Ordering::Acquire) {
            // Already scheduled, nothing to do
            debug!(
                self.options.info_log.as_ref().unwrap(),
                "has background compaction scheduled"
            );
        } else if self.shutting_down.load(Ordering::Acquire) {
            // DB is being deleted; no more background compactions
            debug!(self.options.info_log.as_ref().unwrap(), "shutting down");
        } else if self.bg_error.read().unwrap().is_err() {
            // Already got an error; no more changes
            debug!(self.options.info_log.as_ref().unwrap(), "bg error");
        } else if self.imm.read().unwrap().is_none()
            && self.manual_compaction.lock().unwrap().is_none()
            && !self.version_manager.lock().unwrap().needs_compaction()
        {
            // No work to be done
            debug!(
                self.options.info_log.as_ref().unwrap(),
                "empty immutable table"
            );
        } else {
            debug!(
                self.options.info_log.as_ref().unwrap(),
                "compaction scheduled {} {}",
                self.imm.read().unwrap().is_none(),
                self.version_manager.lock().unwrap().needs_compaction()
            );
            self.background_compaction_scheduled
                .store(true, Ordering::Release);
            self.trigger_compaction();
        }
    }

    /// * Minor compaction: immutable memory table -> level 0.
    /// * Major compaction: when the number of files at a certain level exceeds a certain threshold,
    /// the table file of this level will be compacted with the table file of the higher level+1
    /// to become a new level+1 file.
    fn background_compaction(&self) {
        debug!(
            self.options.info_log.as_ref().unwrap(),
            "background compaction"
        );
        if self.imm.read().unwrap().is_some() {
            debug!(self.options.info_log.as_ref().unwrap(), "imm not null");
            self.compact_immutable_mem_table();
            return;
        }
        let mut c: Option<Compaction> = None;
        let is_manual = self.manual_compaction.lock().unwrap().is_some();
        let mut manual_end = InternalKey::default();
        if is_manual {
            if let Some(m) = &*self.manual_compaction.lock().unwrap() {
                let mut m = m.lock().unwrap();
                c = self.version_manager.lock().unwrap().compact_range(&m);
                m.done = c.is_none();
                if let Some(tmp_c) = &mut c {
                    manual_end = tmp_c
                        .input(0, tmp_c.num_input_files(0) - 1)
                        .get_largest()
                        .clone();
                }

                info!(
                    self.options.info_log.as_ref().unwrap(),
                    "Manual compaction at level-{} from {} .. {}; will stop at {}",
                    m.level,
                    if m.begin.is_none() {
                        "(begin)".to_string()
                    } else {
                        format!("{}", m.begin.as_ref().unwrap())
                    },
                    if m.end.is_none() {
                        "(end)".to_string()
                    } else {
                        format!("{}", m.end.as_ref().unwrap())
                    },
                    if m.done {
                        "(end)".to_string()
                    } else {
                        format!("{}", manual_end)
                    }
                );
            }
        } else {
            c = self.version_manager.lock().unwrap().pick_compaction();
        }

        let mut ret = Ok(());
        if c.is_none() {
            // Nothing to do
        } else if !is_manual && c.as_ref().unwrap().is_trivial_move() {
            let tc = c.as_mut().unwrap();
            // Move file to next level
            assert_eq!(tc.num_input_files(0), 1);
            let f: Arc<FileMetaData> = tc.input(0, 0);
            let level = tc.level();
            tc.get_mut_edit().delete_files(level, f.get_file_number());
            tc.get_mut_edit().add_files(
                level + 1,
                f.get_file_number(),
                f.get_file_size(),
                f.get_smallest(),
                f.get_largest(),
            );
            ret = self
                .version_manager
                .lock()
                .unwrap()
                .log_and_append_version(tc.get_mut_edit());

            if ret.is_err() {
                self.record_background_error(ret.clone());
            }
            let mut tmp = LevelSummaryStorage::default();
            info!(
                self.options.info_log.as_ref().unwrap(),
                "Moved {} to level-{} {} bytes {:?}: {}",
                f.get_file_number(),
                tc.level() + 1,
                f.get_file_size(),
                ret,
                self.version_manager.lock().unwrap().level_summary(&mut tmp)
            );
        } else {
            let tc = c.as_mut().unwrap();

            let mut compact = CompactionState::new(tc);
            ret = self.do_compaction_work(&mut compact);
            if ret.is_err() {
                self.record_background_error(ret.clone());
            }
            self.cleanup_compaction(&mut compact);
            c.as_mut().unwrap().release_inputs();
            // delete files
            self.delete_obsolete_files();
        }

        if let Err(ref err) = ret {
            if self.shutting_down.load(Ordering::Acquire) {
                // Ignore compaction errors found during shutting down
            } else {
                info!(
                    self.options.info_log.as_ref().unwrap(),
                    "Compaction error: {}", err
                );
            }
        }
        if is_manual {
            if let Some(m) = &*self.manual_compaction.lock().unwrap() {
                let mut m = m.lock().unwrap();
                if ret.is_err() {
                    m.done = true;
                }
                if !m.done {
                    // Compact part of the requested range.
                    m.tmp = manual_end;
                    m.begin = Some(m.tmp.clone())
                }
            }
            *self.manual_compaction.lock().unwrap() = None;
        }
    }

    #[allow(clippy::if_same_then_else)]
    fn do_compaction_work(&self, compact: &mut CompactionState) -> DResult<()> {
        let start_micros = get_micro();
        // Micros spent doing immutable table compactions
        let mut imm_micros = 0f64;
        debug!(
            self.options.info_log.as_ref().unwrap(),
            "do_compaction_work"
        );

        info!(
            self.options.info_log.as_ref().unwrap(),
            "Compacting {}@{} + {}@{} files",
            compact.compaction.num_input_files(0),
            compact.compaction.level(),
            compact.compaction.num_input_files(1),
            compact.compaction.level() + 1,
        );
        assert!(
            self.version_manager
                .lock()
                .unwrap()
                .num_level_bytes(compact.compaction.level())
                > 0
        );
        assert!(compact.builder.is_none());
        assert!(compact.outfile.is_none());
        compact.smallest_snapshot = if self.snapshots.is_empty() {
            self.version_manager.lock().unwrap().get_last_sequence()
        } else {
            self.snapshots.oldest().read().unwrap().seq()
        };
        let mut input = {
            let tmp = self.version_manager.lock().unwrap();
            tmp.make_input_iterator(compact.compaction)
        };

        input.seek_to_first();

        let mut ret = Ok(());

        let mut current_user_key = vec![];
        let mut has_current_user_key = false;
        let mut last_sequence_for_key = MAX_SEQUENCE_NUMBER;

        // Iterate the compact iterator.
        while input.valid() && !self.shutting_down.load(Ordering::Acquire) {
            if self.has_imm.load(Ordering::Acquire) {
                let imm_start = get_micro();
                if self.imm.read().unwrap().is_some() {
                    debug!(
                        self.options.info_log.as_ref().unwrap(),
                        "imm not null in do_compaction_work"
                    );
                    self.compact_immutable_mem_table();
                    ignore!(self.background_work_finished_channel.send(()));
                }
                imm_micros += get_micro() - imm_start;
            }
            let key = input.key();
            if compact.compaction.should_stop_before(key) && compact.builder.is_some() {
                ret = self.write_compaction_output_file(compact, &input);
                if ret.is_err() {
                    break;
                }
            }
            let mut drop = false;

            let mut ikey = ParsedInternalKey::default();
            if !parse_internal_key(key, &mut ikey) {
                current_user_key.clear();
                has_current_user_key = false;
                last_sequence_for_key = MAX_SEQUENCE_NUMBER;
            } else {
                if !has_current_user_key
                    || self
                        .user_comparator()
                        .ne(ikey.user_key(), current_user_key.as_slice())
                {
                    current_user_key = ikey.user_key().to_owned();
                    has_current_user_key = true;
                    last_sequence_for_key = MAX_SEQUENCE_NUMBER;
                }

                if last_sequence_for_key <= compact.smallest_snapshot {
                    drop = true;
                } else if ikey.value_type() == ValueType::TypeDeletion
                    && ikey.sequence() <= compact.smallest_snapshot
                    && compact.compaction.is_base_level_for_key(ikey.user_key())
                {
                    drop = true;
                }
                // update last sequence
                last_sequence_for_key = ikey.sequence();
            }

            if !drop {
                // Open output file if necessary
                if compact.builder.is_none() {
                    ret = self.open_compaction_output_file(compact);
                    if ret.is_err() {
                        break;
                    }
                }
                if compact.builder.as_ref().unwrap().num_entries() == 0 {
                    compact.current_output().set_smallest_from_slice(key);
                }
                compact.current_output().set_largest_from_slice(key);
                // Write key value entry.
                compact.builder.as_mut().unwrap().add(key, input.value());

                // Close output file if it is big enough
                if compact.builder.as_ref().unwrap().file_size()
                    >= compact.compaction.max_output_file_size()
                {
                    ret = self.write_compaction_output_file(compact, &input);
                    if ret.is_err() {
                        break;
                    }
                }
            }

            input.next();
        }
        if ret.is_ok() && self.shutting_down.load(Ordering::Acquire) {
            ret = Err(DError::CustomError("Deleting DB during compaction"))
        }
        if ret.is_ok() && compact.builder.is_some() {
            ret = self.write_compaction_output_file(compact, &input);
        }
        if ret.is_ok() {
            ret = input.status();
        }

        // update CompactionStats
        let mut stats = CompactionStats {
            micros: get_micro() - start_micros - imm_micros,
            ..Default::default()
        };
        for i in 0..2 {
            for k in 0..compact.compaction.num_input_files(i) {
                stats.read_bytes += compact.compaction.input(i, k).get_file_size();
            }
        }
        for i in 0..compact.output.len() {
            stats.written_bytes += compact.output[i].get_file_size();
        }
        self.stats.lock().unwrap()[compact.compaction.level() as usize + 1] += stats;

        if ret.is_ok() {
            ret = self.install_compaction_results(compact);
        }
        if ret.is_err() {
            self.record_background_error(ret.clone());
        }
        let mut tmp = LevelSummaryStorage::default();
        info!(
            self.options.info_log.as_ref().unwrap(),
            "compacted to: {}",
            self.version_manager.lock().unwrap().level_summary(&mut tmp)
        );
        ret
    }

    fn user_comparator(&self) -> Arc<dyn Comparator + Send + Sync> {
        self.internal_comparator.user_comparator()
    }

    #[allow(clippy::borrowed_box)]
    fn write_compaction_output_file(
        &self,
        compact: &mut CompactionState,
        input: &Box<dyn Iter>,
    ) -> DResult<()> {
        assert!(compact.outfile.is_some());
        assert!(compact.builder.is_some());

        let output_number = compact.current_output().get_number();
        assert_ne!(output_number, 0);
        // Check for iterator errors
        let mut ret = input.status();
        let current_entries = compact.builder.as_ref().unwrap().num_entries();
        // Write block to output file
        if ret.is_ok() {
            ret = compact.builder.as_mut().unwrap().finish();
        } else {
            compact.builder.as_mut().unwrap().abandon();
        }
        let current_bytes = compact.builder.as_ref().unwrap().file_size();
        compact.current_output().set_file_size(current_bytes);
        compact.total_bytes += current_bytes;
        compact.builder = None;

        // Finish and check for file errors
        if ret.is_ok() {
            ret = compact.outfile.as_ref().unwrap().sync();
        }
        if ret.is_ok() {
            ret = compact.outfile.as_ref().unwrap().close();
        }
        compact.outfile = None;

        if ret.is_ok() && current_entries > 0 {
            // After writing the new table file, update the table cache for using later.
            let (iter, _) =
                self.table_cache
                    .new_iter(ReadOptions::new(), output_number, current_bytes);
            ret = iter.status();
            if ret.is_ok() {
                info!(
                    self.options.info_log.as_ref().unwrap(),
                    "Generated table {}@{}: {} keys, {} bytes",
                    output_number,
                    compact.compaction.level(),
                    current_entries,
                    current_bytes
                );
            }
        }
        ret
    }

    fn open_compaction_output_file(&self, compact: &mut CompactionState) -> DResult<()> {
        assert!(compact.builder.is_none());

        let file_number = self.version_manager.lock().unwrap().new_file_num();
        self.pending_outputs.lock().unwrap().insert(file_number);

        let mut out = Output::new();
        out.set_file_number(file_number);

        compact.output.push(out);

        // Make the output file
        let file_name = table_file_name(self.db_name.as_str(), file_number);

        let ret = self.env.new_writable_file(&file_name).map(|f| {
            compact.outfile = Some(f);
        });

        if ret.is_ok() {
            compact.builder = Some(TableBuilder::new(
                self.options.clone(),
                compact.outfile.clone().unwrap(),
            ));
        }
        ret
    }

    fn install_compaction_results(&self, compact: &mut CompactionState) -> DResult<()> {
        info!(
            self.options.info_log.as_ref().unwrap(),
            "Compacted {} in level{} + {} in level{} files => {} bytes",
            compact.compaction.num_input_files(0),
            compact.compaction.level(),
            compact.compaction.num_input_files(1),
            compact.compaction.level() + 1,
            compact.total_bytes
        );
        // Add compaction outputs
        compact.compaction.delete_all_inputs();
        let level = compact.compaction.level();
        for out in &compact.output {
            compact.compaction.get_mut_edit().add_files(
                level + 1,
                out.get_number(),
                out.get_file_size(),
                out.get_smallest(),
                out.get_largest(),
            );
        }
        self.version_manager
            .lock()
            .unwrap()
            .log_and_append_version(compact.compaction.get_mut_edit())
    }

    /// Minor compaction, immutable memory table to level 0 files.
    fn compact_immutable_mem_table(&self) {
        debug!(self.options.info_log.as_ref().unwrap(), "compact imm table");
        assert!(self.imm.read().unwrap().is_some());

        // Save the contents of the memtable as a new Table
        let mut edit = VersionEdit::new();
        let base = Some(self.version_manager.lock().unwrap().current_version());

        let mut s = self.write_level0_table(
            self.imm.read().unwrap().as_ref().unwrap().new_iter(),
            &mut edit,
            base,
        );
        if s.is_ok() && self.shutting_down.load(Ordering::Acquire) {
            s = Err(DError::CustomError(
                "Deleting DB during mem table compaction",
            ))
        }
        // Replace immutable memtable with the generated Table
        if s.is_ok() {
            edit.set_log_number(self.log_file_num.load(Ordering::Acquire));
            s = self
                .version_manager
                .lock()
                .unwrap()
                .log_and_append_version(&mut edit);
        }

        if s.is_ok() {
            // Commit to the new state
            *self.imm.write().unwrap() = None;
            self.has_imm.store(false, Ordering::Release);
            self.delete_obsolete_files();
        } else {
            self.record_background_error(s);
        }
    }

    // `recover_log_file` will call this method, but mostly it's for `compact_immutable_mem_table`
    pub(crate) fn write_level0_table(
        &self,
        mem_iter: MemIter,
        edit: &mut VersionEdit,
        base: Option<Arc<RwLock<Version>>>,
    ) -> DResult<()> {
        debug!(
            self.options.info_log.as_ref().unwrap(),
            "write_level0_table"
        );
        let start = get_micro();

        let mut meta = FileMetaData::new();
        meta.set_file_number(self.version_manager.lock().unwrap().new_file_num());
        info!(
            self.options.info_log.as_ref().unwrap(),
            "Level-0 table file {}: started",
            meta.get_file_number()
        );

        let ret;
        {
            ret = build_table(
                &self.db_name,
                self.options.clone(),
                &mut meta,
                self.table_cache.clone(),
                Box::new(mem_iter),
                self.env.clone(),
            );
        }
        info!(
            self.options.info_log.as_ref().unwrap(),
            "Level-0 table file:{} bytes:{}",
            meta.get_file_number(),
            meta.get_file_size()
        );
        self.pending_outputs
            .lock()
            .unwrap()
            .remove(&meta.get_file_number());

        // Note that if file_size is zero, the file has been deleted and
        // should not be added to the manifest.
        let mut level = 0;
        if ret.is_ok() && meta.get_file_size() > 0 {
            let min_uk = meta.get_smallest().user_key();
            let max_uk = meta.get_largest().user_key();

            if let Some(b) = &base {
                level = b
                    .read()
                    .unwrap()
                    .pick_level_for_mem_table_output(min_uk, max_uk);
            }
            edit.add_files(
                level,
                meta.get_file_number(),
                meta.get_file_size(),
                meta.get_smallest(),
                meta.get_largest(),
            );
        }
        let mut stats = CompactionStats {
            micros: get_micro() - start,
            ..Default::default()
        };
        stats.written_bytes = meta.get_file_size();
        self.stats.lock().unwrap()[level as usize] += stats;
        ret
    }

    fn delete_obsolete_files(&self) {
        if self.bg_error.read().unwrap().is_err() {
            // After a background error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect.
            return;
        }
        let version_manager = self.version_manager.lock().unwrap();
        let mut live = self.pending_outputs.lock().unwrap().clone();
        live.extend(version_manager.get_live_files());

        let mut number = 0;
        let mut file_type = FileType::TempFile;
        // Get all files in the directory
        // todo if get_children failed, it will panic.
        for file in self.env.get_children(&self.db_name).unwrap_or_default() {
            if parse_file_name(&file, &mut number, &mut file_type) {
                let keep;
                match file_type {
                    FileType::LogFile => keep = number >= version_manager.get_log_number(),
                    FileType::DescriptorFile => {
                        keep = number >= version_manager.get_manifest_num()
                    }
                    FileType::TableFile => {
                        keep = live.contains(&number);
                    }
                    FileType::TempFile => {
                        // Any temp files that are currently being written to must
                        // be recorded in pending_outputs_, which is inserted into "live"
                        keep = live.contains(&number);
                    }
                    FileType::CurrentFile => keep = true,
                    FileType::LockFile => keep = true,
                    FileType::InfoLogFile => keep = true,
                    FileType::OldInfoLogFile => keep = true,
                }
                if !keep {
                    if file_type == FileType::TableFile {
                        // todo self.table_cache.Evict(number);
                    }
                    info!(
                        self.options.info_log.as_ref().unwrap(),
                        "delete file {}", file
                    );
                    let target = Path::new(&self.db_name).join(file);
                    ignore!(self.env.delete_file(target.to_str().unwrap()));
                }
            }
        }
    }

    fn cleanup_compaction(&self, compact: &mut CompactionState) {
        if let Some(b) = &mut compact.builder {
            b.abandon();
        } else {
            assert!(compact.outfile.is_none());
        }
        compact.outfile = None;
        compact.builder = None;
        for out in &compact.output {
            self.pending_outputs
                .lock()
                .unwrap()
                .remove(&out.get_number());
        }
    }

    fn record_background_error(&self, s: DResult<()>) {
        if self.bg_error.read().unwrap().is_ok() {
            *self.bg_error.write().unwrap() = s;
            ignore!(self.background_work_finished_channel.send(()));
        }
    }

    pub(crate) fn record_read_sample(&self, key: &[u8]) {
        debug!(
            self.options.info_log.as_ref().unwrap(),
            "record_read_sample"
        );
        let c = self.version_manager.lock().unwrap().current_version();
        if c.write().unwrap().record_read_sample(key) {
            self.maybe_schedule_compaction();
        }
    }

    /// Send message to trigger compaction thread.
    fn trigger_compaction(&self) {
        let ret = self.compact_channel.send(());
        if ret.is_err() {
            error!(
                self.options.info_log.as_ref().unwrap(),
                "trigger_compaction failed {:?}",
                ret.err()
            );
        }
    }

    /// Got channel message, process the compaction job.
    fn accept_compaction(&self) -> Result<(), RecvError> {
        self.compact_channel.recv()
    }

    fn build_batch_group<'a>(
        &self,
        first: &'a mut BatchWriter,
    ) -> (&'a mut WriteBatch, Vec<Sender<DResult<()>>>) {
        let result = first.batch.as_mut().unwrap();
        let mut receivers = vec![first.sender.clone()];

        // Allow the group to grow up to a maximum size, but if the
        // original write is small, limit the growth so we do not slow
        // down the small write too much.
        let mut size = result.byte_size();
        let max_size = if size <= (128 << 10) {
            size + (128 << 10) // size + 128KB
        } else {
            1 << 20 // 1MB
        };

        let mut writers = self.writers.lock().unwrap();
        while !writers.is_empty() {
            // Do not include a sync write into a batch handled by a non-sync write.
            let w = writers.pop_front().unwrap();
            if w.sync && !first.sync {
                writers.push_front(w);
                break;
            }
            size += w.batch.as_ref().unwrap().byte_size();
            if size > max_size {
                writers.push_front(w);
                break;
            }
            // group the batches
            result.append(w.batch.as_ref().unwrap());
            receivers.push(w.sender.clone());
        }
        (result, receivers)
    }

    fn make_room_for_write(&self, mut force: bool) -> DResult<()> {
        debug!(
            self.options.info_log.as_ref().unwrap(),
            "make_room_for_write"
        );
        let mut allow_delay = !force;
        let mut ret = Ok(());

        loop {
            if let Err(err) = self.bg_error.read().unwrap().as_ref() {
                // Yield previous error
                ret = Err(err.clone());
                break;
            } else if allow_delay
                && self.version_manager.lock().unwrap().num_level_files(0)
                    >= L0_SLOWDOWN_WRITES_TRIGGER
            {
                // We are getting close to hitting a hard limit on the number of
                // L0 files.  Rather than delaying a single write by several
                // seconds when we hit the hard limit, start delaying each
                // individual write by 1ms to reduce latency variance.  Also,
                // this delay hands over some CPU to the compaction thread in
                // case it is sharing the same core as the writer.
                debug!(self.options.info_log.as_ref().unwrap(), "delay");
                self.env.sleep_for_microseconds(1000);
                // Do not delay a single write more than once
                allow_delay = false;
            } else if !force
                && self
                    .mem
                    .read()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .approximate_memory_usage()
                    <= self.options.write_buffer_size
            {
                debug!(
                    self.options.info_log.as_ref().unwrap(),
                    "There is room in current memtable"
                );
                // There is room in current memtable
                break;
            } else if self.imm.read().unwrap().is_some() {
                // We have filled up the current memtable, but the previous
                // one is still being compacted, so we wait.
                debug!(
                    self.options.info_log.as_ref().unwrap(),
                    "Current memtable full; waiting..."
                );
                ignore!(self.background_work_finished_channel.recv());
            } else if self.version_manager.lock().unwrap().num_level_files(0)
                >= L0_STOP_WRITES_TRIGGER
            {
                // There are too many level-0 files.
                info!(
                    self.options.info_log.as_ref().unwrap(),
                    "Too many L0 files; waiting..."
                );
                ignore!(self.background_work_finished_channel.recv());
            } else {
                debug!(self.options.info_log.as_ref().unwrap(), "mem -> imm");
                let new_log_num = self.version_manager.lock().unwrap().new_file_num();
                let log_name = log_file_name(self.db_name.as_str(), new_log_num);
                let result = self
                    .version_manager
                    .lock()
                    .unwrap()
                    .env
                    .new_writable_file(&log_name);
                match result {
                    Ok(f) => {
                        self.log_file_num.store(new_log_num, Ordering::Release);
                        *self.log.lock().unwrap() = Some(LogWriter::new(f));
                        {
                            // mem table -> immutable mem table
                            if !self.mem.read().unwrap().as_ref().unwrap().is_empty() {
                                let mem = std_mem::replace(
                                    &mut *self.mem.write().unwrap(),
                                    Some(MemoryTable::new(self.internal_comparator.clone())),
                                );
                                let mut imm = self.imm.write().unwrap();
                                *imm = mem;
                            }

                            self.has_imm.store(true, Ordering::Release);
                            // create a new mem table
                            *self.mem.write().unwrap() =
                                Some(MemoryTable::new(self.internal_comparator.clone()));
                        }
                        force = false; // break the next loop
                        self.maybe_schedule_compaction();
                    }
                    Err(err) => {
                        // Avoid chewing through file number space in a tight loop.
                        self.version_manager
                            .lock()
                            .unwrap()
                            .reuse_file_number(new_log_num);
                        ret = Err(err);
                        break;
                    }
                }
            }
        }
        ret
    }
}

fn table_cache_size(opt: Options) -> u64 {
    opt.max_open_files - NUM_NON_TABLE_CACHE_FILES
}

pub struct DB {
    inner: Arc<Inner>,
}

impl DB {
    pub(crate) fn new(db_name: String, options: Options) -> DResult<Self> {
        match Inner::open(db_name, options) {
            Ok(db) => Ok(Self {
                inner: Arc::new(db),
            }),
            Err(err) => Err(err),
        }
    }

    pub fn open(db_name: String, options: Options) -> DResult<Self> {
        let imp = DB::new(db_name, options)?;
        // start_compaction_worker
        imp.start_compaction_worker();
        // start_batch_processor
        imp.start_write_worker();
        Ok(imp)
    }

    fn start_compaction_worker(&self) {
        let db = self.inner.clone();
        let ret = thread::Builder::new()
            .name("compact".to_string())
            .spawn(move || {
                debug!(
                    db.options.info_log.as_ref().unwrap(),
                    "start the compaction worker"
                );
                while let Ok(()) = db.accept_compaction() {
                    debug!(db.options.info_log.as_ref().unwrap(), "accept_compaction");
                    // First we check whether the db is shutting down
                    // cause when we close db we send a fake compact signal.
                    if db.shutting_down.load(Ordering::Acquire) {
                        // No more background work when shutting down.
                        break;
                    }
                    // assert!(db.background_compaction_scheduled.load(Ordering::Acquire));
                    if db.bg_error.read().unwrap().is_err() {
                        // No more background work after a background error.
                    } else {
                        db.background_compaction();
                    }

                    db.background_compaction_scheduled
                        .store(false, Ordering::Release);
                    // Previous compaction may have produced too many files in a level,
                    // so reschedule another compaction if needed.
                    db.maybe_schedule_compaction();
                    debug!(db.options.info_log.as_ref().unwrap(), "notify");
                    ignore!(db.background_work_finished_channel.send(()));
                    debug!(db.options.info_log.as_ref().unwrap(), "notify done");
                }
                info!(
                    db.options.info_log.as_ref().unwrap(),
                    "compaction worker shutting down"
                );
            });
        if ret.is_err() {
            debug!(
                self.inner.options.info_log.as_ref().unwrap(),
                "start_compaction_worker ERROR {:?}",
                ret.err()
            );
        }
    }

    fn start_write_worker(&self) {
        let db = self.inner.clone();
        let ret = thread::Builder::new()
            .name("writer".to_string())
            .spawn(move || {
                debug!(
                    db.options.info_log.as_ref().unwrap(),
                    "start the write worker"
                );
                while let Ok(()) = db.process_batch_channel.recv() {
                    if db.shutting_down.load(Ordering::Acquire) {
                        break;
                    }
                    let mut first = {
                        let mut queue = db.writers.lock().unwrap();
                        if queue.is_empty() {
                            continue;
                        }
                        queue.pop_front().unwrap()
                    };

                    let mut ret = db.make_room_for_write(first.batch.is_none());

                    let mut last_sequence = db.version_manager.lock().unwrap().get_last_sequence();
                    // if the batch is empty, it's triggered by a test.
                    let sync = first.sync;
                    if ret.is_ok() && first.batch.is_some() {
                        let (updates, senders) = db.build_batch_group(&mut first);

                        updates.set_sequence(last_sequence + 1);
                        last_sequence += updates.count() as u64;
                        // Add to log and apply to memtable.  We can release the lock
                        // during this phase since &w is currently responsible for logging
                        // and protects against concurrent loggers and concurrent writes
                        // into `mem`.
                        if let Some(logger) = db.log.lock().unwrap().as_mut() {
                            ret = logger.write_record(WriteBatch::contents(updates));
                            let mut sync_error = false;
                            if ret.is_ok() && sync {
                                // && opt.sync
                                ret = logger.sync();
                                if ret.is_err() {
                                    sync_error = true;
                                }
                            }
                            if ret.is_ok() {
                                ret = WriteBatch::insert_batch(
                                    updates,
                                    &mut *db.mem.write().unwrap().as_mut().unwrap(),
                                );
                            }
                            if sync_error {
                                db.record_background_error(ret.clone());
                            }
                        }
                        db.version_manager
                            .lock()
                            .unwrap()
                            .set_last_sequence(last_sequence);
                        // Send message to the writers which were written.
                        for i in senders {
                            i.send(ret.clone()).unwrap_or_else(|err| {
                                error!(
                                    db.options.info_log.as_ref().unwrap(),
                                    "send writer failed {:?}", err
                                );
                            });
                        }
                    }
                    // for force compaction, send to the empty batch.
                    if first.batch.is_none() {
                        first.sender.send(Ok(())).unwrap_or_else(|err| {
                            error!(
                                db.options.info_log.as_ref().unwrap(),
                                "send first writer failed {:?}", err
                            );
                        });
                    }
                }
                debug!(
                    db.options.info_log.as_ref().unwrap(),
                    "writer worker shutting down"
                );
            });
        if ret.is_err() {
            debug!(
                self.inner.options.info_log.as_ref().unwrap(),
                "start_write_worker ERROR {:?}",
                ret.err()
            );
        }
    }

    fn new_internal_iter(
        &self,
        opt: ReadOptions,
        latest_snapshot: &mut SequenceNumber,
        seed: &mut u32,
    ) -> Box<dyn Iter> {
        *latest_snapshot = self
            .inner
            .version_manager
            .lock()
            .unwrap()
            .get_last_sequence();

        let mut list: Vec<Box<dyn Iter>> = vec![];
        {
            // Push memory table and immutable memory table iterator.
            let mem_iter = self.inner.mem.read().unwrap().as_ref().unwrap().new_iter();
            list.push(Box::new(mem_iter));
        }
        if let Some(im) = &self.inner.imm.read().unwrap() as &Option<MemoryTable> {
            list.push(Box::new(im.new_iter()));
        }
        // Push version iterators.
        self.inner
            .version_manager
            .lock()
            .unwrap()
            .current_version()
            .read()
            .unwrap()
            .add_iters(opt, &mut list);
        let n = list.len(); // make the compiler happy.
        let internal_iter =
            new_merging_iterator(self.inner.internal_comparator.clone().into_cmp(), list, n);
        self.inner.seed.fetch_add(1, Ordering::Acquire);
        *seed = self.inner.seed.load(Ordering::Acquire);
        internal_iter
    }

    /// Destroy the contents of the specified database.
    /// Be very careful using this method.
    #[allow(dead_code)]
    pub(crate) fn destroy_db(db_name: &str, opt: Options) -> DResult<()> {
        let filenames = opt.env.get_children(db_name)?;

        let lock_name = lock_file_name(db_name);
        opt.env.lock_file(db_name)?;
        let mut number = 0;
        let mut type_ = FileType::TempFile;
        for file in filenames {
            if parse_file_name(&file, &mut number, &mut type_) && type_ != FileType::LockFile {
                // Lock file will be deleted at end
                let target = Path::new(db_name).join(file);
                // debug!("DESTROY_DB remove file {}", target.to_str().unwrap());
                opt.env.delete_file(target.to_str().unwrap())?;
            }
        }
        ignore!(opt.env.unlock_file(db_name));
        // debug!("[DESTROY_DB]: remove lock {}", &lock_name);
        ignore!(opt.env.delete_file(&lock_name));
        // debug!("[DESTROY_DB]: remove dir {}", &db_name);
        ignore!(opt.env.delete_dir(db_name));
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn name(&self) -> &str {
        &self.inner.db_name
    }
}

impl Database for DB {
    type Iterator = DBIter;

    fn put(&self, key: &[u8], value: &[u8], opt: WriteOptions) -> DResult<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write(Some(batch), opt)
    }

    fn delete(&self, key: &[u8], opt: WriteOptions) -> DResult<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write(Some(batch), opt)
    }

    fn get(&self, key: &[u8], read_opt: ReadOptions) -> DResult<Vec<u8>> {
        let ret = Err(DError::NotFound);
        let seq = if let Some(snap) = &read_opt.snapshot {
            snap.read().unwrap().seq()
        } else {
            self.inner
                .version_manager
                .lock()
                .unwrap()
                .get_last_sequence()
        };
        let have_stat_update;
        let mut stats = GetStats::default();

        let lookup_key = LookUpKey::new(key, seq);
        // search the mem table
        if let Ok(v) = self
            .inner
            .mem
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .get(&lookup_key)
        {
            return Ok(v);
        }
        // search the immutable mem table
        if let Some(imm) = self.inner.imm.read().unwrap().as_ref() {
            if let Ok(v) = imm.get(&lookup_key) {
                info!(
                    self.inner.options.info_log.as_ref().unwrap(),
                    "found in imm"
                );
                return Ok(v);
            }
        }
        // search the table files in current version.
        have_stat_update = true;
        let current = self.inner.version_manager.lock().unwrap().current_version();
        if let Ok(v) = current
            .write()
            .unwrap()
            .get(read_opt, &lookup_key, &mut stats)
        {
            info!(
                self.inner.options.info_log.as_ref().unwrap(),
                "found in current"
            );
            return Ok(v);
        }

        let current = self.inner.version_manager.lock().unwrap().current_version();
        if have_stat_update && current.write().unwrap().update_stats(&stats) {
            self.inner.maybe_schedule_compaction();
        }
        ret
    }

    fn write(&self, batch: Option<WriteBatch>, opt: WriteOptions) -> DResult<()> {
        let (sender, receiver) = crossbeam_channel::bounded(0);
        let w = BatchWriter::new(opt.sync, batch, sender);
        self.inner.writers.lock().unwrap().push_back(w);
        // notify the writer thread to write batch.
        self.inner
            .process_batch_channel
            .send(())
            .unwrap_or_else(|err| {
                error!(
                    self.inner.options.info_log.as_ref().unwrap(),
                    "send process_batch_sem failed {:?}", err
                );
            });
        // Write result will be send by this channel.
        match receiver.recv() {
            Ok(ret) => ret,
            Err(err) => {
                error!(
                    self.inner.options.info_log.as_ref().unwrap(),
                    "recv failed: {}", err
                );
                Err(DError::CustomError("write failed"))
            }
        }
    }

    fn new_iter(&self, opt: ReadOptions) -> Self::Iterator {
        let mut latest_snapshot: SequenceNumber = 0;
        let mut seed = 0u32;
        let iter = self.new_internal_iter(opt.clone(), &mut latest_snapshot, &mut seed);
        return DBIter::new(
            self.inner.clone(),
            self.inner.user_comparator(),
            iter,
            if let Some(s) = &opt.snapshot {
                s.read().unwrap().seq()
            } else {
                latest_snapshot
            },
            seed,
        );
    }

    fn close(&self) -> DResult<()> {
        debug!(self.inner.options.info_log.as_ref().unwrap(), "close");
        if self.inner.shutting_down.load(Ordering::Acquire) {
            return Ok(());
        }
        self.inner.shutting_down.store(true, Ordering::Release);

        // send a signal to the compact thread to wake up, and
        self.inner.trigger_compaction();
        self.inner
            .process_batch_channel
            .send(())
            .unwrap_or_else(|err| {
                error!(
                    self.inner.options.info_log.as_ref().unwrap(),
                    "send process_batch_sem failed {:?}", err
                );
            });
        ignore!(self.inner.env.unlock_file(&self.inner.db_name));
        Ok(())
    }

    fn get_property(&self, property: String, value: &mut String) -> bool {
        value.clear();
        let prefix = PROPERTY_PREFIX;
        if !property.starts_with(prefix) {
            return false;
        }

        let mut property = String::from(property.trim_start_matches(prefix));

        if property.starts_with(NUM_FILES_PROPERTY) {
            property.drain(..NUM_FILES_PROPERTY.len());

            let mut level = 0;
            let ok = consume_decimal_num(&mut property, &mut level) && property.is_empty();

            return if !ok || level >= LEVEL_NUMBER as u64 {
                false
            } else {
                let n = format!(
                    "{}",
                    self.inner
                        .version_manager
                        .lock()
                        .unwrap()
                        .num_level_files(level as i64)
                );
                value.push_str(n.as_str());
                true
            };
        } else if property.as_str() == STATS_PROPERTY {
            let mut data = String::default();
            data.push_str("Compactions\n");
            data.push_str("Level   Files   Size(MB)   Time(ms)   Read(MB)   Write(MB)\n");
            data.push_str("----------------------------------------------------------\n");
            for level in 0..LEVEL_NUMBER as i64 {
                let files = self
                    .inner
                    .version_manager
                    .lock()
                    .unwrap()
                    .num_level_files(level);
                let stats = self.inner.stats.lock().unwrap();
                if stats[level as usize].micros > 0f64 || files > 0 {
                    // {:"Level".len()} {:2 + "Files".len()} {:2 + "Size(MB)".len()} ...
                    // At least one space between the stats.
                    data.push_str(&format!(
                        "{:5} {:7} {:10.5} {:10.5} {:10.6} {:11.6}",
                        level,
                        files,
                        self.inner
                            .version_manager
                            .lock()
                            .unwrap()
                            .num_level_bytes(level) as f64
                            / 1048576f64,
                        stats[level as usize].micros * 1000_f64,
                        stats[level as usize].read_bytes as f64 / 1048576f64,
                        stats[level as usize].written_bytes as f64 / 1048576f64,
                    ));
                }
            }
            *value = data;
            return true;
        } else if property.as_str() == APPROXIMATE_MEM_USAGE_PROPERTY {
            let mut total_usage = 0;
            if let Some(b) = &self.inner.options.block_cache {
                total_usage += b.lock().unwrap().total_charge();
            }
            total_usage += self
                .inner
                .mem
                .read()
                .unwrap()
                .as_ref()
                .unwrap()
                .approximate_memory_usage();
            if let Some(imm) = &*self.inner.imm.read().unwrap() {
                total_usage += imm.approximate_memory_usage();
            }
            *value = format!("{}", total_usage);
            return true;
        }
        false
    }

    fn get_snapshot(&self) -> Arc<RwLock<SnapNode>> {
        let seq = self
            .inner
            .version_manager
            .lock()
            .unwrap()
            .get_last_sequence();
        self.inner.snapshots.create_and_append(seq)
    }

    fn release_snapshot(&self, snap: &mut SnapNode) {
        self.inner.snapshots.delete(snap);
    }

    fn get_approximate_sizes(&self, ranges: Vec<Range>) -> Vec<u64> {
        let v = self.inner.version_manager.lock().unwrap().current_version();
        let mut ret = vec![];

        for range in ranges {
            let k1 = InternalKey::new(
                range.start.as_slice(),
                MAX_SEQUENCE_NUMBER,
                VALUE_TYPE_FOR_SEEK,
            );
            let k2 = InternalKey::new(
                range.limit.as_slice(),
                MAX_SEQUENCE_NUMBER,
                VALUE_TYPE_FOR_SEEK,
            );
            let start = self
                .inner
                .version_manager
                .lock()
                .unwrap()
                .approximate_offset_of(&*v.read().unwrap(), &k1);
            let limit = self
                .inner
                .version_manager
                .lock()
                .unwrap()
                .approximate_offset_of(&*v.read().unwrap(), &k2);
            ret.push(if limit >= start { limit - start } else { 0 });
        }
        ret
    }
}

pub struct Range {
    start: Vec<u8>,
    limit: Vec<u8>,
}

impl Range {
    pub fn new(s: Vec<u8>, l: Vec<u8>) -> Self {
        Self { start: s, limit: l }
    }
}

// Only for test
pub trait TestExt: Database {
    fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>);
    fn test_compact_range(&self, level: usize, begin: Option<&[u8]>, end: Option<&[u8]>);
    fn test_compact_mem_table(&self) -> DResult<()>;
    fn test_max_next_level_overlapping_bytes(&self) -> u64;
    fn test_new_internal_iterator(&self) -> Box<dyn Iter>;
}

impl TestExt for DB {
    fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) {
        let mut max_level_with_files = 1;
        {
            let vs = self.inner.version_manager.lock().unwrap();
            let current = vs.current_version();
            for level in 1..LEVEL_NUMBER {
                if current
                    .read()
                    .unwrap()
                    .is_overlapped_in_level(level as i64, begin, end)
                {
                    max_level_with_files = level;
                }
            }
        }
        ignore!(self.test_compact_mem_table());
        for level in 0..max_level_with_files {
            self.test_compact_range(level, begin, end);
        }
    }

    fn test_compact_range(&self, level: usize, begin: Option<&[u8]>, end: Option<&[u8]>) {
        assert!(level + 1 < LEVEL_NUMBER);
        debug!(
            self.inner.options.info_log.as_ref().unwrap(),
            "test_compact_range"
        );
        let begin_storage;
        let end_storage;
        let mut manual = ManualCompaction {
            done: false,
            begin: None,
            end: None,
            level: level as i64,
            tmp: Default::default(),
        };
        if let Some(b) = begin {
            begin_storage = InternalKey::new(b, MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK);
            manual.begin = Some(begin_storage);
        }
        if let Some(e) = end {
            end_storage = InternalKey::new(e, 0, ValueType::TypeDeletion);
            manual.end = Some(end_storage);
        }
        let manual = Arc::new(Mutex::new(manual));
        while !manual.lock().unwrap().done
            && !self.inner.shutting_down.load(Ordering::Acquire)
            && self.inner.bg_error.read().unwrap().is_ok()
        {
            if self.inner.manual_compaction.lock().unwrap().is_none() {
                {
                    let mut manual_compaction = self.inner.manual_compaction.lock().unwrap();
                    *manual_compaction = Some(manual.clone());
                }
                self.inner.maybe_schedule_compaction();
            } else {
                // Running either my compaction or another compaction.
                ignore!(self.inner.background_work_finished_channel.recv());
            }
        }
        let mut manual_compaction = self.inner.manual_compaction.lock().unwrap();
        if let Some(m2) = &*manual_compaction {
            if Arc::ptr_eq(&manual, m2) {
                *manual_compaction = None;
            }
        }
    }

    fn test_compact_mem_table(&self) -> DResult<()> {
        // empty batch means just wait for earlier writes to be done
        debug!(
            self.inner.options.info_log.as_ref().unwrap(),
            "test_compact_mem_table"
        );
        debug!(self.inner.options.info_log.as_ref().unwrap(), "write");
        self.write(None, WriteOptions::default())?;
        debug!(self.inner.options.info_log.as_ref().unwrap(), "write done");

        while self.inner.imm.read().unwrap().is_some()
            && self.inner.bg_error.read().unwrap().is_ok()
        {
            debug!(self.inner.options.info_log.as_ref().unwrap(), "go lock");
            ignore!(self.inner.background_work_finished_channel.recv());
            debug!(self.inner.options.info_log.as_ref().unwrap(), "wake up");
        }
        debug!(
            self.inner.options.info_log.as_ref().unwrap(),
            "test_compact_mem_table go"
        );
        if self.inner.imm.read().unwrap().is_some() {
            return self.inner.bg_error.read().unwrap().clone();
        }
        Ok(())
    }

    fn test_max_next_level_overlapping_bytes(&self) -> u64 {
        dbg!(self
            .inner
            .version_manager
            .lock()
            .unwrap()
            .max_next_level_overlapping_bytes())
    }

    fn test_new_internal_iterator(&self) -> Box<dyn Iter> {
        let mut _ignored: SequenceNumber = 0;
        let mut _ignored_seed: u32 = 0;
        self.new_internal_iter(ReadOptions::new(), &mut _ignored, &mut _ignored_seed)
    }
}
