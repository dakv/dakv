#[cfg(test)]
mod test {
    use crate::db::database::{Range, TestExt};
    use crate::db::file::{parse_file_name, FileType};
    use crate::db::format::ValueType;
    use crate::db::snapshot::SnapNode;
    use crate::db::write_batch::TableInserter;
    use crate::db::{
        parse_internal_key, table_file_name, CompressionType, DError, DResult, InternalKey,
        Options, ParsedInternalKey, ReadOptions, WriteBatch, WriteOptions, L0_STOP_WRITES_TRIGGER,
        LEVEL_NUMBER, MAX_MEM_COMPACT_LEVEL, MAX_SEQUENCE_NUMBER,
    };
    use crate::env::{Env, PosixEnv};
    use crate::env::{RandomAccessFile, SequentialFile, WritableFile};
    use crate::table::{BloomFilterPolicy, FilterPolicy};
    use crate::utils::cmp::{BaseComparator, BytewiseComparatorImpl, Comparator};
    use crate::utils::constants::PROPERTY_PREFIX;
    use crate::utils::iter::Iter;
    use crate::utils::lru_cache::SharedLRUCache;
    use crate::utils::random::{Random, RandomGenerator};
    use crate::utils::time::get_micro;
    use crate::{Database, DB};
    use lazy_static::lazy_static;
    use rb_tree::RBMap;
    use regex::Regex;
    use std::cmp::Ordering;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex, RwLock};
    use std::thread;
    use std::time::Duration;

    fn delay(m: u64) {
        thread::sleep(Duration::from_millis(m));
    }

    #[derive(Clone)]
    struct AtomicCounter {
        count: Arc<Mutex<u64>>,
    }

    impl AtomicCounter {
        fn new() -> Self {
            Self {
                count: Arc::new(Mutex::new(0)),
            }
        }
        fn increment(&self) {
            self.increment_by(1);
        }

        fn increment_by(&self, count: u64) {
            let mut c = self.count.lock().unwrap();
            *c += count;
        }

        fn read(&self) -> u64 {
            let c = self.count.lock().unwrap();
            *c
        }

        fn reset(&self) {
            let mut c = self.count.lock().unwrap();
            *c = 0;
        }
    }

    struct SpecialEnv {
        random_read_counter: AtomicCounter,
        delay_data_sync: Arc<AtomicBool>,
        data_sync_error: Arc<AtomicBool>,
        non_writable: Arc<AtomicBool>,
        no_space: Arc<AtomicBool>,
        manifest_write_error: Arc<AtomicBool>,
        manifest_sync_error: Arc<AtomicBool>,
        count_random_reads: AtomicBool,
        base: Box<dyn Env + Send + Sync>,
    }

    unsafe impl Sync for SpecialEnv {}

    unsafe impl Send for SpecialEnv {}

    impl SpecialEnv {
        pub fn new(base: Box<dyn Env + Send + Sync>) -> Self {
            Self {
                base,
                delay_data_sync: Arc::new(AtomicBool::new(false)),
                data_sync_error: Arc::new(AtomicBool::new(false)),
                no_space: Arc::new(AtomicBool::new(false)),
                non_writable: Arc::new(AtomicBool::new(false)),
                manifest_write_error: Arc::new(AtomicBool::new(false)),
                manifest_sync_error: Arc::new(AtomicBool::new(false)),
                random_read_counter: AtomicCounter::new(),
                count_random_reads: AtomicBool::new(false),
            }
        }
    }

    impl Env for SpecialEnv {
        fn new_random_access_file(&self, file_name: &str) -> DResult<Arc<dyn RandomAccessFile>> {
            struct CountingFile {
                counter: AtomicCounter,
                base: Arc<dyn RandomAccessFile>,
            }

            impl RandomAccessFile for CountingFile {
                fn read(&self, offset: usize, result: &mut [u8]) -> DResult<usize> {
                    self.counter.increment();
                    self.base.read(offset, result)
                }
            }

            let r = self.base.new_random_access_file(file_name);
            if let Ok(base) = &r {
                if self
                    .count_random_reads
                    .load(std::sync::atomic::Ordering::Acquire)
                {
                    let m = CountingFile {
                        counter: self.random_read_counter.clone(),
                        base: base.clone(),
                    };
                    return Ok(Arc::new(m));
                }
            }
            return r;
        }

        fn new_writable_file(&self, file_name: &str) -> DResult<Arc<dyn WritableFile>> {
            struct DataFile {
                delay_data_sync: Arc<AtomicBool>,
                data_sync_error: Arc<AtomicBool>,
                no_space: Arc<AtomicBool>,
                base: Arc<dyn WritableFile>,
            }

            impl WritableFile for DataFile {
                fn append(&self, buf: &[u8]) -> DResult<()> {
                    if self.no_space.load(std::sync::atomic::Ordering::Acquire) {
                        eprintln!("no_space writer error");
                        return Ok(());
                    }
                    self.base.append(buf)
                }

                fn flush(&self) -> DResult<()> {
                    self.base.flush()
                }

                fn sync(&self) -> DResult<()> {
                    if self
                        .data_sync_error
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        return Err(DError::CustomError("simulated data sync error"));
                    }
                    while self
                        .delay_data_sync
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        delay(100);
                    }
                    self.base.sync()
                }

                fn close(&self) -> DResult<()> {
                    self.base.close()
                }
            }

            struct ManifestFile {
                manifest_write_error: Arc<AtomicBool>,
                manifest_sync_error: Arc<AtomicBool>,
                base: Arc<dyn WritableFile>,
            }

            impl WritableFile for ManifestFile {
                fn append(&self, buf: &[u8]) -> DResult<()> {
                    if self
                        .manifest_write_error
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        eprintln!("manifest_write_error simulated writer error");
                        return Err(DError::CustomError("simulated writer error"));
                    }
                    self.base.append(buf)
                }

                fn flush(&self) -> DResult<()> {
                    self.base.flush()
                }

                fn sync(&self) -> DResult<()> {
                    if self
                        .manifest_sync_error
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        return Err(DError::CustomError("simulated sync error"));
                    }
                    self.base.sync()
                }

                fn close(&self) -> DResult<()> {
                    self.base.close()
                }
            }

            if self.non_writable.load(std::sync::atomic::Ordering::Acquire) {
                return Err(DError::CustomError("simulated write error"));
            }
            let r = self.base.new_writable_file(file_name);
            if let Ok(base) = &r {
                if file_name.ends_with(FileType::TableFile.to_str())
                    || file_name.ends_with(FileType::LogFile.to_str())
                {
                    let r = DataFile {
                        delay_data_sync: self.delay_data_sync.clone(),
                        data_sync_error: self.data_sync_error.clone(),
                        no_space: self.no_space.clone(),
                        base: base.clone(),
                    };
                    return Ok(Arc::new(r));
                } else if file_name.contains("MANIFEST") {
                    let r = ManifestFile {
                        manifest_write_error: self.manifest_write_error.clone(),
                        manifest_sync_error: self.manifest_sync_error.clone(),
                        base: base.clone(),
                    };
                    return Ok(Arc::new(r));
                }
            }
            return r;
        }

        fn new_sequential_file(&self, file_name: &str) -> DResult<Arc<dyn SequentialFile>> {
            self.base.new_sequential_file(file_name)
        }

        fn new_appendable_file(&self, file_name: &str) -> DResult<Arc<dyn WritableFile>> {
            self.base.new_appendable_file(file_name)
        }

        fn create_dir(&self, name: &str) -> DResult<()> {
            self.base.create_dir(name)
        }

        fn delete_dir(&self, name: &str) -> DResult<()> {
            self.base.delete_dir(name)
        }

        fn delete_file(&self, file_name: &str) -> DResult<()> {
            self.base.delete_file(file_name)
        }

        fn rename_file(&self, from: &str, to: &str) -> DResult<()> {
            self.base.rename_file(from, to)
        }

        fn file_exists(&self, file_name: &str) -> bool {
            self.base.file_exists(file_name)
        }

        fn get_children(&self, file_name: &str) -> DResult<Vec<String>> {
            self.base.get_children(file_name)
        }

        fn read_file_to_string(&self, file_name: &str) -> DResult<String> {
            self.base.read_file_to_string(file_name)
        }

        // May create the named file if it does not already exist.
        fn lock_file(&self, file_name: &str) -> DResult<()> {
            self.base.lock_file(file_name)
        }

        fn unlock_file(&self, file_name: &str) -> DResult<()> {
            self.base.unlock_file(file_name)
        }

        fn get_file_size(&self, file_name: &str, size: &mut u64) -> DResult<()> {
            self.base.get_file_size(file_name, size)
        }
    }

    enum OptionConfig {
        Default = 0,
        Reuse = 1,
        Filter = 2,
        Uncompressed = 3,
        End = 4,
    }

    impl From<u64> for OptionConfig {
        fn from(a: u64) -> Self {
            match a {
                0 => OptionConfig::Default,
                1 => OptionConfig::Reuse,
                2 => OptionConfig::Filter,
                3 => OptionConfig::Uncompressed,
                4 => OptionConfig::End,
                _ => {
                    panic!()
                }
            }
        }
    }

    // Simple test by using the real file not mock.
    const TEST_DB: &str = "test_data_";

    struct DBTest {
        option_config: u64,
        name: String,
        db: Option<DB>,
        filter_policy: Option<Arc<dyn FilterPolicy + Send + Sync>>,
        last_option: Option<Options>,
        env: Arc<SpecialEnv>,
    }

    impl Drop for DBTest {
        fn drop(&mut self) {
            if let Some(d) = &self.db {
                ignore!(d.close());
                ignore!(DB::destroy_db(&d.name(), Options::default()));
            }
        }
    }

    impl DBTest {
        // for concurrency test issue.
        fn new(name: &str) -> Self {
            let mut db = Self {
                db: None,
                env: Arc::new(SpecialEnv::new(Box::new(PosixEnv::default()))),
                filter_policy: Some(Arc::new(BloomFilterPolicy::new(10))),
                last_option: None,
                option_config: OptionConfig::Default as u64,
                name: format!("{}{}{}", TEST_DB, name, get_micro()),
            };
            ignore!(DB::destroy_db(&db.name, Options::default()));
            db.reopen(None);
            db
        }

        fn get_with_opt(&self, k: &str, read_opt: ReadOptions) -> String {
            if let Some(db) = &self.db {
                let ret = db.get(k.as_bytes(), read_opt);
                return if ret.is_err() {
                    format!("{}", ret.err().unwrap())
                } else {
                    unsafe { String::from_utf8_unchecked(ret.unwrap()) }
                };
            }
            panic!();
        }

        fn db(&self) -> &DB {
            self.db.as_ref().unwrap()
        }

        fn get(&mut self, k: &str) -> String {
            let read_opt = ReadOptions::new();
            self.get_with_opt(k, read_opt)
        }

        fn get_with_snap(&self, k: &str, snap: Arc<RwLock<SnapNode>>) -> String {
            let mut read_opt = ReadOptions::new();
            read_opt.snapshot = Some(snap.clone());
            self.get_with_opt(k, read_opt)
        }

        // for simple string to test, use str
        fn put(&mut self, k: &str, v: &str) -> DResult<()> {
            self.db
                .as_ref()
                .unwrap()
                .put(k.as_bytes(), v.as_bytes(), WriteOptions::default())
        }

        fn delete(&mut self, k: &str) -> DResult<()> {
            self.db
                .as_ref()
                .unwrap()
                .delete(k.as_bytes(), WriteOptions::default())
        }

        fn reopen(&mut self, opt: Option<Options>) {
            self.try_reopen(opt).unwrap();
        }

        fn try_reopen(&mut self, opt: Option<Options>) -> DResult<()> {
            // stop the thread.
            if let Some(db) = &self.db {
                ignore!(db.close());
            }

            self.db = None;
            let option;
            if let Some(o) = opt {
                option = o;
            } else {
                let mut opt = self.current_options();
                opt.create_if_missing = true;
                option = opt
            }

            self.last_option = Some(option.clone());

            match DB::open(self.name.clone(), option) {
                Ok(db) => self.db = Some(db),
                Err(err) => return Err(err),
            }
            Ok(())
        }

        fn current_options(&self) -> Options {
            let mut option = Options::default();
            // mock file system
            option.reuse_logs = false;
            match self.option_config.into() {
                OptionConfig::Reuse => {
                    option.reuse_logs = true;
                }
                OptionConfig::Filter => {
                    option.filter_policy = self.filter_policy.clone();
                }
                OptionConfig::Uncompressed => {
                    option.compression = CompressionType::NoCompress;
                }
                _ => {}
            }
            option
        }

        fn destroy_and_reopen(&mut self, opt: Option<Options>) {
            // stop the thread.
            if let Some(db) = &self.db {
                ignore!(db.close());
            }

            ignore!(DB::destroy_db(&self.name, Options::default()));
            self.db = None;
            assert!(self.try_reopen(opt).is_ok());
        }

        fn change_options(&mut self) -> bool {
            self.option_config += 1;
            if self.option_config >= OptionConfig::End as u64 {
                false
            } else {
                self.destroy_and_reopen(None);
                true
            }
        }

        // Return a string that contains all key,value pairs in order,
        // formatted like "(k1->v1)(k2->v2)".
        fn contents(&self) -> String {
            let mut forward = vec![];
            let mut result = String::new();
            let mut iter = self.db().new_iter(ReadOptions::new());
            iter.seek_to_first();
            while iter.valid() {
                let s = iter_to_string(&iter);
                result.push_str("(");
                result.push_str(s.as_str());
                result.push_str(")");
                forward.push(s);
                iter.next();
            }
            let mut matched = 0;
            iter.seek_to_last();
            while iter.valid() {
                assert!(matched < forward.len());
                assert_eq!(iter_to_string(&iter), forward[forward.len() - matched - 1]);
                iter.prev();
                matched += 1;
            }
            assert_eq!(matched, forward.len());
            result
        }

        fn all_entries_for(&self, user_key: &str) -> String {
            let mut iter = self.db().test_new_internal_iterator();
            let target = InternalKey::new(
                user_key.as_bytes(),
                MAX_SEQUENCE_NUMBER,
                ValueType::TypeValue,
            );
            iter.seek(target.encode());

            let mut result;
            if !iter.status().is_ok() {
                result = "ERROR".to_string();
            } else {
                result = "[ ".to_string();
                let mut first = true;
                while iter.valid() {
                    let mut ikey = ParsedInternalKey::default();
                    if !parse_internal_key(iter.key(), &mut ikey) {
                        result = format!("{}CORRUPTED", result)
                    } else {
                        if self
                            .last_option
                            .as_ref()
                            .unwrap()
                            .comparator
                            .ne(ikey.user_key(), user_key.as_bytes())
                        {
                            break;
                        }
                        if !first {
                            result = format!("{}, ", result)
                        }
                        first = false;
                        match ikey.value_type() {
                            ValueType::TypeValue => unsafe {
                                result = format!(
                                    "{}{}",
                                    result,
                                    std::str::from_utf8_unchecked(iter.value())
                                )
                            },
                            ValueType::TypeDeletion => {
                                result = format!("{}DEL", result);
                            }
                        }
                    }
                    iter.next();
                }
                if !first {
                    result = format!("{} ", result)
                }
                result = format!("{}]", result)
            }
            result
        }

        fn num_table_files_at_level(&self, level: u64) -> u64 {
            let mut stats = String::default();
            assert!(self.db().get_property(
                format!("{}num-files-at-level{}", PROPERTY_PREFIX, level),
                &mut stats,
            ));
            stats.parse::<u64>().unwrap()
        }

        fn total_table_files(&self) -> u64 {
            let mut ret = 0;
            for level in 0..LEVEL_NUMBER {
                ret += self.num_table_files_at_level(level as u64);
            }
            ret
        }

        fn files_per_level(&self) -> String {
            let mut result = "".to_string();
            let mut last_non_zero_offset = 0;
            for level in 0..LEVEL_NUMBER {
                let f = self.num_table_files_at_level(level as u64);
                result = format!("{}{}{}", result, if level > 0 { "," } else { "" }, f);
                if f > 0 {
                    last_non_zero_offset = result.len();
                }
            }
            result.drain(last_non_zero_offset..);
            result
        }

        fn count_files(&self) -> usize {
            let files = self.env.get_children(&self.name).unwrap();
            files.len()
        }

        fn size(&self, s: &str, l: &str) -> u64 {
            let r = Range::new(s.as_bytes().to_vec(), l.as_bytes().to_vec());
            let sizes = self.db.as_ref().unwrap().get_approximate_sizes(vec![r]);
            sizes[0]
        }

        fn compact(&self, s: &str, l: &str) {
            self.db()
                .compact_range(Some(s.as_bytes()), Some(l.as_bytes()));
        }

        fn make_tables(&mut self, level: usize, small: &str, large: &str) {
            for _i in 0..level {
                self.put(small, "begin").unwrap();
                self.put(large, "end").unwrap();
                self.db().test_compact_mem_table().unwrap();
            }
        }

        // Prevent pushing of new table files into deeper levels by adding
        // tables that cover a specified range to all levels.
        fn fill_levels(&mut self, small: &str, large: &str) {
            self.make_tables(LEVEL_NUMBER, small, large);
        }

        #[allow(unused)]
        fn dump_file_counts(&self) {}

        #[allow(unused)]
        fn iter_status(&self) {}

        fn delete_an_table_file(&self) -> bool {
            let files = self.env.get_children(&self.name).unwrap();
            let mut number = 0;
            let mut typ = FileType::TempFile;
            for i in files {
                if parse_file_name(&i, &mut number, &mut typ) && typ == FileType::TableFile {
                    assert!(self
                        .env
                        .delete_file(&table_file_name(&self.name, number))
                        .is_ok());
                    return true;
                }
            }
            false
        }
    }

    fn between(val: u64, low: u64, high: u64) -> bool {
        let result = val >= low && val <= high;
        if !result {
            eprintln!("value {} is not in range [{}, {}]", val, low, high);
        }
        result
    }

    fn key(i: u64) -> String {
        format!("key{:06}", i)
    }

    fn random_key(rnd: &impl RandomGenerator) -> String {
        let len = if rnd.one_in(3) {
            1
        } else {
            if rnd.one_in(100) {
                rnd.skewed(10)
            } else {
                rnd.uniform(10)
            }
        };
        let chars: Vec<u8> = vec![0, 1, 97, 98, 99, 100, 101, 253, 254, 255];
        let mut result = vec![];
        for _ in 0..len {
            result.push(chars[rnd.uniform(chars.len() as u32) as usize])
        }
        unsafe { String::from_utf8_unchecked(result) }
    }

    fn random_string(rnd: &impl RandomGenerator, len: u32) -> String {
        let mut dst = vec![0; len as usize];
        for i in 0..len {
            dst[i as usize] = (' ' as u32 + rnd.uniform(95)) as u8;
        }
        unsafe { String::from_utf8_unchecked(dst) }
    }

    #[test]
    fn test_empty() {
        let mut db_test = DBTest::new("test_empty");
        // HACK: do while loop
        while {
            assert!(db_test.db.is_some());
            assert_eq!(db_test.get("foo"), "NotFound");
            db_test.change_options()
        } {}
    }

    #[test]
    fn test_empty_key() {
        let mut db_test = DBTest::new("test_empty_key");
        while {
            assert!(db_test.db.is_some());
            assert_eq!(db_test.get("foo"), "NotFound");
            db_test.change_options()
        } {}
    }

    #[test]
    fn test_empty_value() {
        let mut db_test = DBTest::new("test_empty_value");
        while {
            assert!(db_test.put("v1", "").is_ok());
            assert_eq!(db_test.get("v1"), "");
            assert!(db_test.put("v2", "").is_ok());
            assert_eq!(db_test.get("v2"), "");
            db_test.change_options()
        } {}
    }

    #[test]
    fn test_read_write() {
        let mut db_test = DBTest::new("test_read_write");
        while {
            assert!(db_test.put("foo", "v1").is_ok());
            assert_eq!(db_test.get("foo"), "v1");
            assert!(db_test.put("bar", "v2").is_ok());
            assert!(db_test.put("foo", "v3").is_ok());
            assert_eq!(db_test.get("bar"), "v2");
            assert_eq!(db_test.get("foo"), "v3");
            db_test.change_options()
        } {}
    }

    #[test]
    fn test_put_delete_get() {
        let mut db_test = DBTest::new("test_put_delete_get");
        while {
            assert!(db_test.put("foo", "v1").is_ok());
            assert_eq!(db_test.get("foo"), "v1");
            assert!(db_test.put("foo", "v2").is_ok());
            assert_eq!(db_test.get("foo"), "v2");
            assert!(db_test.delete("foo").is_ok());
            assert_eq!(db_test.get("foo"), "NotFound");
            db_test.change_options()
        } {}
    }

    #[test]
    #[ignore]
    fn test_get_from_immutable_layer() {
        // kasi

        let mut db_test = DBTest::new("test_get_from_immutable_layer");
        while {
            println!("__test_get_from_immutable_layer");
            let mut opt = db_test.current_options();
            opt.env = db_test.env.clone();
            opt.write_buffer_size = 100000; // Small write buffer
            db_test.reopen(Some(opt));

            assert!(db_test.put("foo", "v1").is_ok());
            assert_eq!(db_test.get("foo"), "v1");

            {
                let e = db_test.env.clone();
                e.delay_data_sync
                    .store(true, std::sync::atomic::Ordering::Release)
            }

            db_test.put("k1", "x".repeat(100000).as_str()).unwrap();
            db_test.put("k2", "y".repeat(100000).as_str()).unwrap();
            assert_eq!(db_test.get("foo"), "v1");

            {
                let e = db_test.env.clone();
                e.delay_data_sync
                    .store(false, std::sync::atomic::Ordering::Release)
            }
            db_test.change_options()
        } {}
    }

    #[test]
    fn test_get_from_versions() {
        let mut db_test = DBTest::new("test_get_from_versions");
        while {
            assert!(db_test.put("foo", "v1").is_ok());
            db_test.db().test_compact_mem_table().unwrap();
            assert_eq!("v1", db_test.get("foo").as_str());
            db_test.change_options()
        } {}
    }

    #[test]
    fn test_get_mem_usage() {
        let mut db_test = DBTest::new("test_get_mem_usage");
        let name = format!("{}approximate-memory-usage", PROPERTY_PREFIX);
        let mut tmp = String::default();
        assert_eq!(
            db_test
                .db()
                .get_property(name.clone().drain(..1).collect(), &mut tmp),
            false
        );

        while {
            assert!(db_test.put("foo", "v1").is_ok());
            let mut data = String::default();
            assert!(db_test.db().get_property(name.clone(), &mut data));
            let mem_usage = data.parse::<i32>().unwrap();
            assert!(mem_usage > 0);
            assert!(mem_usage < 5 * 1024 * 1024);

            db_test.change_options()
        } {}
    }

    #[test]
    fn test_get_snapshot() {
        let mut db_test = DBTest::new("test_get_snapshot");
        while {
            for i in 0..2 {
                let key = if i == 0 {
                    String::from("foo")
                } else {
                    String::from("x".repeat(200))
                };
                assert!(db_test.put(&key, "v1").is_ok());
                let s1 = db_test.db().get_snapshot();
                assert!(db_test.put(&key, "v2").is_ok());
                assert_eq!(db_test.get(&key).as_str(), "v2");
                assert_eq!(db_test.get_with_snap(&key, s1.clone()).as_str(), "v1");
                ignore!(db_test.db().test_compact_mem_table());
                assert_eq!(db_test.get(&key).as_str(), "v2");
                assert_eq!(db_test.get_with_snap(&key, s1.clone()).as_str(), "v1");
                db_test.db().release_snapshot(&mut *s1.write().unwrap());
            }
            db_test.change_options()
        } {}
    }

    #[test]
    fn test_get_identical_snapshots() {
        let mut db_test = DBTest::new("test_get_identical_snapshots");
        while {
            for i in 0..2 {
                let key = if i == 0 {
                    String::from("foo")
                } else {
                    String::from("x".repeat(200))
                };
                assert!(db_test.put(&key, "v1").is_ok());

                let s1 = db_test.db().get_snapshot();
                let s2 = db_test.db().get_snapshot();
                let s3 = db_test.db().get_snapshot();

                assert!(db_test.put(&key, "v2").is_ok());
                assert_eq!(db_test.get_with_snap(&key, s1.clone()).as_str(), "v1");
                assert_eq!(db_test.get_with_snap(&key, s2.clone()).as_str(), "v1");
                assert_eq!(db_test.get_with_snap(&key, s3.clone()).as_str(), "v1");
                db_test.db().release_snapshot(&mut *s1.write().unwrap());
                ignore!(db_test.db().test_compact_mem_table());
                assert!(db_test.put(&key, "v2").is_ok());
                assert_eq!(db_test.get_with_snap(&key, s2.clone()).as_str(), "v1");
                assert_eq!(db_test.get_with_snap(&key, s3.clone()).as_str(), "v1");
            }
            db_test.change_options()
        } {}
    }

    #[test]
    fn test_iterate_over_empty_snapshot() {
        let mut db_test = DBTest::new("test_iterate_over_empty_snapshot");
        while {
            let snap = db_test.db().get_snapshot();

            assert!(db_test.put("foo", "v1").is_ok());
            assert!(db_test.put("foo", "v2").is_ok());

            let mut read_opt = ReadOptions::new();
            read_opt.snapshot = Some(snap.clone());
            let read_opt = read_opt;

            {
                let mut iter1 = db_test.db().new_iter(read_opt.clone());
                iter1.seek_to_first();
                assert!(!iter1.valid());
            }
            ignore!(db_test.db().test_compact_mem_table());
            {
                let mut iter2 = db_test.db().new_iter(read_opt);
                iter2.seek_to_first();
                assert!(!iter2.valid());
            }
            db_test.db().release_snapshot(&mut *snap.write().unwrap());
            db_test.change_options()
        } {}
    }

    #[test]
    fn test_get_level0ordering() {
        let mut db_test = DBTest::new("test_get_level0ordering");
        while {
            assert!(db_test.put("bar", "b").is_ok());
            assert!(db_test.put("foo", "v1").is_ok());
            ignore!(db_test.db().test_compact_mem_table());
            assert!(db_test.put("foo", "v2").is_ok());
            ignore!(db_test.db().test_compact_mem_table());
            assert_eq!(db_test.get("foo"), "v2");

            db_test.change_options()
        } {}
    }

    #[test]
    fn test_get_ordered_by_levels() {
        let mut db_test = DBTest::new("test_get_ordered_by_levels");
        while {
            assert!(db_test.put("foo", "v1").is_ok());
            db_test.compact("a", "z");
            assert_eq!(db_test.get("foo"), "v1");

            assert!(db_test.put("foo", "v2").is_ok());
            ignore!(db_test.db().test_compact_mem_table());
            assert_eq!(db_test.get("foo"), "v2");

            db_test.change_options()
        } {}
    }

    #[test]
    fn test_get_picks_correct_file() {
        let mut db_test = DBTest::new("test_get_picks_correct_file");
        while {
            assert!(db_test.put("a", "va").is_ok());
            db_test.compact("a", "b");
            assert!(db_test.put("x", "vx").is_ok());
            db_test.compact("x", "y");
            assert!(db_test.put("f", "vf").is_ok());
            db_test.compact("f", "g");

            assert_eq!(db_test.get("a"), "va");
            assert_eq!(db_test.get("f"), "vf");
            assert_eq!(db_test.get("x"), "vx");

            db_test.change_options()
        } {}
    }

    #[test]
    #[ignore]
    fn test_get_encounters_empty_level() {
        let mut db_test = DBTest::new("test_get_encounters_empty_level");
        while {
            let mut compaction_count = 0;

            while db_test.num_table_files_at_level(0) == 0
                || db_test.num_table_files_at_level(2) == 0
            {
                assert!(compaction_count <= 100);
                compaction_count += 1;
                db_test.put("a", "begin").unwrap();
                db_test.put("z", "end").unwrap();
                ignore!(db_test.db().test_compact_mem_table());
            }

            db_test.db().test_compact_range(1, None, None);
            assert_eq!(db_test.num_table_files_at_level(0), 1);
            assert_eq!(db_test.num_table_files_at_level(1), 0);
            assert_eq!(db_test.num_table_files_at_level(2), 1);

            for _ in 0..1000 {
                assert_eq!(db_test.get("missing"), "NotFound");
            }
            delay(2000);
            assert_eq!(db_test.num_table_files_at_level(0), 0);

            db_test.change_options()
        } {}
    }

    #[test]
    fn test_iter_empty() {
        let db_test = DBTest::new("test_iter_empty");
        let mut iter = db_test.db().new_iter(ReadOptions::new());

        iter.seek_to_first();
        assert!(!iter.valid());

        iter.seek_to_last();
        assert!(!iter.valid());

        iter.seek(b"foo");
        assert!(!iter.valid());
    }

    fn iter_to_string(iter: &impl Iter) -> String {
        return if !iter.valid() {
            String::from("END")
        } else {
            unsafe {
                format!(
                    "{}->{}",
                    std::str::from_utf8_unchecked(iter.key()),
                    std::str::from_utf8_unchecked(iter.value())
                )
            }
        };
    }

    #[test]
    fn test_iter_single() {
        let mut db_test = DBTest::new("test_iter_single");
        assert!(db_test.put("a", "va").is_ok());

        let mut iter = db_test.db().new_iter(ReadOptions::new());
        {
            iter.seek_to_first();
            assert_eq!(iter_to_string(&iter), "a->va");
            iter.next();
            assert_eq!(iter_to_string(&iter), "END");

            iter.seek_to_first();
            assert_eq!(iter_to_string(&iter), "a->va");
            iter.prev();
            assert_eq!(iter_to_string(&iter), "END");
        }
        {
            iter.seek_to_last();
            assert_eq!(iter_to_string(&iter), "a->va");
            iter.next();
            assert_eq!(iter_to_string(&iter), "END");

            iter.seek_to_last();
            assert_eq!(iter_to_string(&iter), "a->va");
            iter.prev();
            assert_eq!(iter_to_string(&iter), "END");
        }
        iter.seek(b"");
        assert_eq!(iter_to_string(&iter), "a->va");
        iter.next();
        assert_eq!(iter_to_string(&iter), "END");

        iter.seek(b"a");
        assert_eq!(iter_to_string(&iter), "a->va");
        iter.next();
        assert_eq!(iter_to_string(&iter), "END");

        iter.seek(b"b");
        assert_eq!(iter_to_string(&iter), "END");
    }

    #[test]
    fn test_iter_multi() {
        let mut db_test = DBTest::new("test_iter_multi");
        assert!(db_test.put("a", "va").is_ok());
        assert!(db_test.put("b", "vb").is_ok());
        assert!(db_test.put("c", "vc").is_ok());

        let mut iter = db_test.db().new_iter(ReadOptions::new());
        {
            iter.seek_to_first();
            assert_eq!(iter_to_string(&iter), "a->va");
            iter.next();
            assert_eq!(iter_to_string(&iter), "b->vb");
            iter.next();
            assert_eq!(iter_to_string(&iter), "c->vc");
            iter.next();
            assert_eq!(iter_to_string(&iter), "END");
            iter.seek_to_first();
            assert_eq!(iter_to_string(&iter), "a->va");
            iter.prev();
            assert_eq!(iter_to_string(&iter), "END");
        }
        {
            iter.seek_to_last();
            assert_eq!(iter_to_string(&iter), "c->vc");
            iter.prev();
            assert_eq!(iter_to_string(&iter), "b->vb");
            iter.prev();
            assert_eq!(iter_to_string(&iter), "a->va");
            iter.prev();
            assert_eq!(iter_to_string(&iter), "END");
            iter.seek_to_last();
            assert_eq!(iter_to_string(&iter), "c->vc");
            iter.next();
            assert_eq!(iter_to_string(&iter), "END");
        }
        iter.seek(b"");
        assert_eq!(iter_to_string(&iter), "a->va");
        iter.seek(b"a");
        assert_eq!(iter_to_string(&iter), "a->va");
        iter.seek(b"ax");
        assert_eq!(iter_to_string(&iter), "b->vb");
        iter.seek(b"b");
        assert_eq!(iter_to_string(&iter), "b->vb");
        iter.seek(b"z");
        assert_eq!(iter_to_string(&iter), "END");
        // Switch from reverse to forward
        iter.seek_to_last();
        iter.prev();
        iter.prev();
        iter.next();
        assert_eq!(iter_to_string(&iter), "b->vb");
        // Switch from forward to reverse
        iter.seek_to_first();
        iter.next();
        iter.next();
        iter.prev();
        assert_eq!(iter_to_string(&iter), "b->vb");
        // Make sure iter stays at snapshot
        assert!(db_test.put("a", "va2").is_ok());
        assert!(db_test.put("a2", "va3").is_ok());
        assert!(db_test.put("b", "vb2").is_ok());
        assert!(db_test.put("c", "vc2").is_ok());
        assert!(db_test.delete("b").is_ok());

        iter.seek_to_first();
        assert_eq!(iter_to_string(&iter), "a->va");
        iter.next();
        assert_eq!(iter_to_string(&iter), "b->vb");
        iter.next();
        assert_eq!(iter_to_string(&iter), "c->vc");
        iter.next();
        assert_eq!(iter_to_string(&iter), "END");
        iter.seek_to_last();
        assert_eq!(iter_to_string(&iter), "c->vc");
        iter.prev();
        assert_eq!(iter_to_string(&iter), "b->vb");
        iter.prev();
        assert_eq!(iter_to_string(&iter), "a->va");
        iter.prev();
        assert_eq!(iter_to_string(&iter), "END");
    }

    #[test]
    fn test_iter_small_and_large_mix() {
        let mut db_test = DBTest::new("test_iter_small_and_large_mix");
        let long_b = "b".repeat(100000);
        let long_d = "d".repeat(100000);
        let long_e = "e".repeat(100000);

        assert!(db_test.put("a", "va").is_ok());
        assert!(db_test.put("b", &long_b).is_ok());
        assert!(db_test.put("c", "vc").is_ok());
        assert!(db_test.put("d", &long_d).is_ok());
        assert!(db_test.put("e", &long_e).is_ok());

        let mut iter = db_test.db().new_iter(ReadOptions::new());

        iter.seek_to_first();
        assert_eq!(iter_to_string(&iter), "a->va");
        iter.next();
        assert_eq!(iter_to_string(&iter), format!("b->{}", long_b));
        iter.next();
        assert_eq!(iter_to_string(&iter), "c->vc");
        iter.next();
        assert_eq!(iter_to_string(&iter), format!("d->{}", long_d));
        iter.next();
        assert_eq!(iter_to_string(&iter), format!("e->{}", long_e));
        iter.next();
        assert_eq!(iter_to_string(&iter), "END");

        iter.seek_to_last();
        assert_eq!(iter_to_string(&iter), format!("e->{}", long_e));
        iter.prev();
        assert_eq!(iter_to_string(&iter), format!("d->{}", long_d));
        iter.prev();
        assert_eq!(iter_to_string(&iter), "c->vc");
        iter.prev();
        assert_eq!(iter_to_string(&iter), format!("b->{}", long_b));
        iter.prev();
        assert_eq!(iter_to_string(&iter), "a->va");
        iter.prev();
        assert_eq!(iter_to_string(&iter), "END");
    }

    #[test]
    fn test_iter_multi_with_delete() {
        let mut db_test = DBTest::new("test_iter_multi_with_delete");
        while {
            assert!(db_test.put("a", "va").is_ok());
            assert!(db_test.put("b", "vb").is_ok());
            assert!(db_test.put("c", "vc").is_ok());
            assert!(db_test.delete("b").is_ok());
            assert_eq!(db_test.get("b"), "NotFound");

            let mut iter = db_test.db().new_iter(ReadOptions::new());
            iter.seek(b"c");
            assert_eq!(iter_to_string(&iter), "c->vc");
            iter.prev();
            assert_eq!(iter_to_string(&iter), "a->va");

            db_test.change_options()
        } {}
    }

    // #[test]
    // fn test_iter_multi_with_delete_and_compaction() {
    //     let mut db_test = DBTest::new("test_iter_multi_with_delete_and_compaction");
    // }

    #[test]
    fn test_recover() {
        let mut db_test = DBTest::new("test_recover");
        while {
            assert!(db_test.put("foo", "v1").is_ok());
            assert!(db_test.put("baz", "v5").is_ok());

            db_test.reopen(None);

            assert_eq!(db_test.get("foo").as_str(), "v1");
            assert_eq!(db_test.get("baz").as_str(), "v5");

            assert!(db_test.put("foo", "v3").is_ok());
            assert!(db_test.put("bar", "v2").is_ok());

            db_test.reopen(None);

            assert_eq!(db_test.get("foo").as_str(), "v3");
            assert!(db_test.put("foo", "v4").is_ok());
            assert_eq!(db_test.get("foo").as_str(), "v4");
            assert_eq!(db_test.get("bar").as_str(), "v2");
            assert_eq!(db_test.get("baz").as_str(), "v5");
            db_test.change_options()
        } {}
    }

    #[test]
    fn test_recovery_with_empty_log() {
        let mut db_test = DBTest::new("test_recovery_with_empty_log");
        while {
            assert!(db_test.put("foo", "v1").is_ok());
            assert!(db_test.put("foo", "v2").is_ok());
            db_test.reopen(None);
            db_test.reopen(None);

            assert!(db_test.put("foo", "v3").is_ok());
            db_test.reopen(None);
            assert_eq!(db_test.get("foo").as_str(), "v3");

            db_test.change_options()
        } {}
    }

    #[test]
    fn test_recover_during_memtable_compaction() {
        let mut db_test = DBTest::new("test_recover_during_memtable_compaction");
        while {
            let mut opt = db_test.current_options();
            opt.env = db_test.env.clone();
            opt.write_buffer_size = 1000000;

            db_test.reopen(Some(opt.clone()));

            assert!(db_test.put("foo", "v1").is_ok());
            assert!(db_test.put("big1", &"x".repeat(10000000)).is_ok());
            assert!(db_test.put("big2", &"y".repeat(1000)).is_ok());
            assert!(db_test.put("bar", "v2").is_ok());

            db_test.reopen(Some(opt.clone()));
            assert_eq!(db_test.get("foo").as_str(), "v1");
            assert_eq!(db_test.get("bar").as_str(), "v2");
            assert_eq!(db_test.get("big1").as_str(), &"x".repeat(10000000));
            assert_eq!(db_test.get("big2").as_str(), &"y".repeat(1000));

            db_test.change_options()
        } {}
    }

    #[test]
    fn test_minor_compactions_happen() {
        let mut db_test = DBTest::new("test_minor_compactions_happen");
        while {
            let mut opt = db_test.current_options();
            opt.write_buffer_size = 10000;

            db_test.reopen(Some(opt.clone()));

            let starting_num_tables = db_test.total_table_files();

            let n = 500;
            for i in 0..n {
                assert!(db_test
                    .put(&key(i), &format!("{}{}", key(i), "v".repeat(1000)))
                    .is_ok())
            }

            let ending_num_tables = db_test.total_table_files();
            assert!(ending_num_tables > starting_num_tables);
            for i in 0..n {
                assert_eq!(
                    db_test.get(&key(i)),
                    format!("{}{}", key(i), "v".repeat(1000))
                );
            }
            db_test.reopen(None);

            for i in 0..n {
                assert_eq!(
                    db_test.get(&key(i)),
                    format!("{}{}", key(i), "v".repeat(1000))
                );
            }
            db_test.change_options()
        } {}
    }

    #[test]
    fn test_recover_with_large_log() {
        let mut db_test = DBTest::new("test_recover_with_large_log");
        {
            let opt = db_test.current_options();
            db_test.reopen(Some(opt));
            assert!(db_test.put("big1", &"1".repeat(200000)).is_ok());
            assert!(db_test.put("big2", &"2".repeat(200000)).is_ok());
            assert!(db_test.put("small3", &"3".repeat(10)).is_ok());
            assert!(db_test.put("small4", &"4".repeat(10)).is_ok());
            assert_eq!(db_test.num_table_files_at_level(0), 0);
        }

        let mut opt = db_test.current_options();
        opt.write_buffer_size = 100000;
        db_test.reopen(Some(opt));
        assert_eq!(db_test.num_table_files_at_level(0), 3);
        assert_eq!(db_test.get("big1"), "1".repeat(200000));
        assert_eq!(db_test.get("big2"), "2".repeat(200000));
        assert_eq!(db_test.get("small3"), "3".repeat(10));
        assert_eq!(db_test.get("small4"), "4".repeat(10));
        assert!(db_test.num_table_files_at_level(0) > 1);
    }

    #[test]
    fn test_compactions_generate_multiple_files() {
        let mut db_test = DBTest::new("test_compactions_generate_multiple_files");
        let mut opt = db_test.current_options();
        opt.write_buffer_size = 100000000;

        db_test.reopen(Some(opt.clone()));

        // Write 8MB (80 values, each 100K)
        let mut rnd = Random::new(301);
        assert_eq!(db_test.num_table_files_at_level(0), 0);
        let mut values = vec![];
        for i in 0..80 {
            values.push(random_string(&mut rnd, 100000));
            assert!(db_test.put(&key(i), &values[i as usize]).is_ok());
        }
        // Reopening moves updates to level-0
        db_test.reopen(Some(opt.clone()));
        db_test.db().test_compact_range(0, None, None);

        assert_eq!(db_test.num_table_files_at_level(0), 0);
        assert!(db_test.num_table_files_at_level(1) > 1);

        for i in 0..80 {
            assert_eq!(db_test.get(&key(i)), values[i as usize]);
        }
    }

    #[test]
    fn test_repeated_writes_to_same_key() {
        let mut db_test = DBTest::new("test_repeated_writes_to_same_key");
        let mut opt = db_test.current_options();
        opt.env = db_test.env.clone();
        opt.write_buffer_size = 100000; // Small write buffer

        db_test.reopen(Some(opt.clone()));
        // We must have at most one file per level except for level-0,
        // which may have up to kL0_StopWritesTrigger files.
        let max_files = LEVEL_NUMBER + L0_STOP_WRITES_TRIGGER;

        let mut rnd = Random::new(301);
        let value = random_string(&mut rnd, (2 * opt.write_buffer_size) as u32);
        for _ in 0..5 * max_files {
            db_test.put("key", &value).unwrap();
            assert!(db_test.total_table_files() <= max_files as u64);
        }
    }

    #[test]
    fn test_sparse_merge() {
        let mut db_test = DBTest::new("test_sparse_merge");
        let mut opt = db_test.current_options();
        opt.compression = CompressionType::NoCompress;
        db_test.reopen(Some(opt));

        db_test.fill_levels("A", "Z");

        // Suppose there is:
        //    small amount of data with prefix A
        //    large amount of data with prefix B
        //    small amount of data with prefix C
        // and that recent updates have made small changes to all three prefixes.
        // Check that we do not do a compaction that merges all of B in one shot.
        let value = "x".repeat(1000);
        db_test.put("A", "va").unwrap();
        // Write approximately 100MB of "B" values
        for i in 0..100000 {
            let key = format!("B{:010}", i);
            db_test.put(key.as_str(), &value).unwrap();
        }
        db_test.put("C", "vc").unwrap();
        ignore!(db_test.db().test_compact_mem_table());
        db_test.db().test_compact_range(0, None, None);
        // Make sparse update
        db_test.put("A", "va2").unwrap();
        db_test.put("B100", "bvalue2").unwrap();
        db_test.put("C", "vc2").unwrap();
        ignore!(db_test.db().test_compact_mem_table());
        // Compactions should not cause us to create a situation where
        // a file overlaps too much data at the next level.
        assert!(db_test.db().test_max_next_level_overlapping_bytes() <= 20 * 1024 * 1024);
        db_test.db().test_compact_range(0, None, None);
        assert!(db_test.db().test_max_next_level_overlapping_bytes() <= 25 * 1024 * 1024);
        db_test.db().test_compact_range(1, None, None);
        assert!(db_test.db().test_max_next_level_overlapping_bytes() <= 20 * 1024 * 1024);
    }

    #[test]
    #[allow(unused_labels)]
    fn test_approximate_sizes() {
        let mut db_test = DBTest::new("test_approximate_sizes");
        'do_while: while {
            let mut opt = db_test.current_options();
            opt.write_buffer_size = 100000000;
            opt.compression = CompressionType::NoCompress;

            db_test.destroy_and_reopen(None);

            assert!(between(db_test.size("", "xyz"), 0, 0));
            db_test.reopen(Some(opt.clone()));
            assert!(between(db_test.size("", "xyz"), 0, 0));

            // Write 8MB (80 values, each 100K)
            assert_eq!(db_test.num_table_files_at_level(0), 0);
            let n = 80;
            let s1 = 100000;
            let s2 = 105000;
            let mut rnd = Random::new(301);
            for i in 0..n {
                assert!(db_test
                    .put(&key(i), &random_string(&mut rnd, s1 as u32))
                    .is_ok());
            }

            assert!(between(db_test.size("", &key(50)), 0, 0));

            if opt.reuse_logs {
                db_test.reopen(Some(opt.clone()));
                assert!(between(db_test.size("", &key(50)), 0, 0));
                // continue 'do_while;
            } else {
                for _run in 0..3 {
                    db_test.reopen(Some(opt.clone()));
                    for compact_start in (0..n).step_by(10) {
                        for i in (0..n).step_by(10) {
                            assert!(between(db_test.size("", &key(i)), s1 * i, s2 * i));
                            assert!(between(
                                db_test.size("", &format!("{}.suffix", key(i))),
                                s1 * (i + 1),
                                s2 * (i + 1),
                            ));
                            assert!(between(
                                db_test.size(&key(i), &key(i + 10)),
                                s1 * 10,
                                s2 * 10,
                            ));
                        }
                        let start_str = key(compact_start);
                        let end_str = key(compact_start + 9);
                        db_test.db().test_compact_range(
                            0,
                            Some(start_str.as_bytes()),
                            Some(end_str.as_bytes()),
                        );
                    }
                    assert_eq!(db_test.num_table_files_at_level(0), 0);
                    assert!(db_test.num_table_files_at_level(1) > 0);
                }
            }

            db_test.change_options()
        } {}
    }

    #[test]
    fn test_approximate_sizes_mix_of_small_and_large() {
        let mut db_test = DBTest::new("test_approximate_sizes_mix_of_small_and_large");
        while {
            let mut opt = db_test.current_options();
            opt.compression = CompressionType::NoCompress;
            db_test.reopen(None);

            let mut rnd = Random::new(301);
            let big1 = random_string(&mut rnd, 100000);
            assert!(db_test
                .put(&key(0), &random_string(&mut rnd, 10000))
                .is_ok());
            assert!(db_test
                .put(&key(1), &random_string(&mut rnd, 10000))
                .is_ok());
            assert!(db_test.put(&key(2), &big1).is_ok());
            assert!(db_test
                .put(&key(3), &random_string(&mut rnd, 10000))
                .is_ok());
            assert!(db_test.put(&key(4), &big1).is_ok());
            assert!(db_test
                .put(&key(5), &random_string(&mut rnd, 10000))
                .is_ok());
            assert!(db_test
                .put(&key(6), &random_string(&mut rnd, 300000))
                .is_ok());
            assert!(db_test
                .put(&key(7), &random_string(&mut rnd, 10000))
                .is_ok());

            if opt.reuse_logs {
                assert!(db_test.db().test_compact_mem_table().is_ok());
            }

            for _run in 0..3 {
                db_test.reopen(Some(opt.clone()));
                assert!(between(db_test.size("", &key(0)), 0, 0));
                assert!(between(db_test.size("", &key(1)), 10000, 11000));
                assert!(between(db_test.size("", &key(2)), 20000, 21000));
                assert!(between(db_test.size("", &key(3)), 120000, 121000));
                assert!(between(db_test.size("", &key(4)), 130000, 131000));
                assert!(between(db_test.size("", &key(5)), 230000, 231000));
                assert!(between(db_test.size("", &key(6)), 240000, 241000));
                assert!(between(db_test.size("", &key(7)), 540000, 541000));
                assert!(between(db_test.size("", &key(8)), 550000, 560000));

                assert!(between(db_test.size(&key(3), &key(5)), 110000, 111000));
                db_test.db().test_compact_range(0, None, None);
            }
            db_test.change_options()
        } {}
    }

    #[test]
    fn test_iterator_pins_ref() {
        let mut db_test = DBTest::new("test_iterator_pins_ref");
        db_test.put("foo", "hello").unwrap();
        let mut iter = db_test.db().new_iter(ReadOptions::new());

        db_test.put("foo", "new_value1").unwrap();
        for i in 0..100 {
            assert!(db_test
                .put(&key(i), &format!("{}{}", key(i), "v".repeat(100000)))
                .is_ok());
        }
        db_test.put("foo", "new_value2").unwrap();

        iter.seek_to_first();
        while iter.valid() {
            iter.next();
        }

        iter.seek_to_first();
        assert!(iter.valid());
        assert_eq!(b"foo", iter.key());
        assert_eq!(b"hello", iter.value());
        iter.next();
        assert!(!iter.valid());
    }

    #[test]
    fn test_snapshot() {
        let mut db_test = DBTest::new("test_snapshot");
        while {
            assert!(db_test.put("foo", "v1").is_ok());
            let s1 = db_test.db().get_snapshot();
            assert!(db_test.put("foo", "v2").is_ok());
            let s2 = db_test.db().get_snapshot();
            assert!(db_test.put("foo", "v3").is_ok());
            let s3 = db_test.db().get_snapshot();

            assert!(db_test.put("foo", "v4").is_ok());
            assert_eq!(db_test.get_with_snap("foo", s1.clone()), "v1");
            assert_eq!(db_test.get_with_snap("foo", s2.clone()), "v2");
            assert_eq!(db_test.get_with_snap("foo", s3.clone()), "v3");
            assert_eq!(db_test.get("foo"), "v4");

            db_test.db().release_snapshot(&mut *s3.write().unwrap());
            assert_eq!(db_test.get_with_snap("foo", s1.clone()), "v1");
            assert_eq!(db_test.get_with_snap("foo", s2.clone()), "v2");
            assert_eq!(db_test.get("foo"), "v4");
            db_test.db().release_snapshot(&mut *s1.write().unwrap());
            assert_eq!(db_test.get_with_snap("foo", s2.clone()), "v2");
            assert_eq!(db_test.get("foo"), "v4");
            db_test.db().release_snapshot(&mut *s2.write().unwrap());
            assert_eq!(db_test.get("foo"), "v4");

            db_test.change_options()
        } {}
    }

    #[test]
    fn test_hidden_values_are_removed() {
        let mut db_test = DBTest::new("test_hidden_values_are_removed");
        while {
            let mut rnd = Random::new(301);
            db_test.fill_levels("a", "z");
            let big = random_string(&mut rnd, 50000);
            db_test.put("foo", &big).unwrap();
            db_test.put("pastfoo", "v").unwrap();
            let s1 = db_test.db().get_snapshot();
            db_test.put("foo", "tiny").unwrap();
            db_test.put("pastfoo2", "v2").unwrap();

            assert!(db_test.db().test_compact_mem_table().is_ok());
            assert!(db_test.num_table_files_at_level(0) > 0);

            assert_eq!(db_test.get_with_snap("foo", s1.clone()), big);
            assert!(between(db_test.size("", "pastfoo"), 50000, 60000));
            db_test.db().release_snapshot(&mut *s1.write().unwrap());
            assert_eq!(
                db_test.all_entries_for("foo"),
                format!("[ tiny, {} ]", &big)
            );

            db_test.db().test_compact_range(0, None, Some(b"x"));
            assert_eq!(db_test.all_entries_for("foo"), "[ tiny ]");

            assert_eq!(db_test.num_table_files_at_level(0), 0);
            assert!(db_test.num_table_files_at_level(1) >= 1);
            db_test.db().test_compact_range(1, None, Some(b"x"));
            assert_eq!(db_test.all_entries_for("foo"), "[ tiny ]");

            assert!(between(db_test.size("", "pastfoo"), 0, 1000));

            db_test.change_options()
        } {}
    }

    #[test]
    fn test_deletion_markers1() {
        let mut db_test = DBTest::new("test_deletion_markers1");

        db_test.put("foo", "v1").unwrap();
        assert!(db_test.db().test_compact_mem_table().is_ok());
        let last = MAX_MEM_COMPACT_LEVEL;
        assert_eq!(db_test.num_table_files_at_level(last as u64), 1);

        db_test.put("a", "begin").unwrap();
        db_test.put("z", "end").unwrap();
        ignore!(db_test.db().test_compact_mem_table());
        assert_eq!(db_test.num_table_files_at_level(last as u64), 1);
        assert_eq!(db_test.num_table_files_at_level((last - 1) as u64), 1);

        db_test.delete("foo").unwrap();
        db_test.put("foo", "v2").unwrap();
        assert_eq!(db_test.all_entries_for("foo"), "[ v2, DEL, v1 ]");
        assert!(db_test.db().test_compact_mem_table().is_ok());
        assert_eq!(db_test.all_entries_for("foo"), "[ v2, DEL, v1 ]");
        db_test.db().test_compact_range(last - 2, None, Some(b"z"));
        // DEL eliminated, but v1 remains because we aren't compacting that level
        // (DEL can be eliminated because v2 hides v1).
        assert_eq!(db_test.all_entries_for("foo"), "[ v2, v1 ]");
        db_test.db().test_compact_range(last - 1, None, None);
        // Merging last-1 w/ last, so we are the base level for "foo", so
        // DEL is removed.  (as is v1).
        assert_eq!(db_test.all_entries_for("foo"), "[ v2 ]");
    }

    #[test]
    fn test_deletion_markers2() {
        // is_base_level_for_key
        let mut db_test = DBTest::new("test_deletion_markers2");

        db_test.put("foo", "v1").unwrap();
        assert!(db_test.db().test_compact_mem_table().is_ok());
        let last = MAX_MEM_COMPACT_LEVEL;
        assert_eq!(db_test.num_table_files_at_level(last as u64), 1);

        db_test.put("a", "begin").unwrap();
        db_test.put("z", "end").unwrap();
        ignore!(db_test.db().test_compact_mem_table());
        assert_eq!(db_test.num_table_files_at_level(last as u64), 1);
        assert_eq!(db_test.num_table_files_at_level((last - 1) as u64), 1);

        db_test.delete("foo").unwrap();
        assert_eq!(db_test.all_entries_for("foo"), "[ DEL, v1 ]");
        assert!(db_test.db().test_compact_mem_table().is_ok());
        assert_eq!(db_test.all_entries_for("foo"), "[ DEL, v1 ]");
        db_test.db().test_compact_range(last - 2, None, None);
        // DEL kept: "last" file overlaps
        assert_eq!(db_test.all_entries_for("foo"), "[ DEL, v1 ]");
        db_test.db().test_compact_range(last - 1, None, None);
        // Merging last-1 w/ last, so we are the base level for "foo", so
        // DEL is removed.  (as is v1).
        assert_eq!(db_test.all_entries_for("foo"), "[ ]");
    }

    #[test]
    fn test_overlap_in_level0() {
        let mut db_test = DBTest::new("test_overlap_in_level0");
        while {
            // Fill levels 1 and 2 to disable the pushing of new memtables to levels > 0.
            assert!(db_test.put("100", "v100").is_ok());
            assert!(db_test.put("999", "v999").is_ok());
            ignore!(db_test.db().test_compact_mem_table());
            assert!(db_test.delete("100").is_ok());
            assert!(db_test.delete("999").is_ok());
            ignore!(db_test.db().test_compact_mem_table());
            assert_eq!(db_test.files_per_level(), "0,1,1");
            // Make files spanning the following ranges in level-0:
            //  files[0]  200 .. 900
            //  files[1]  300 .. 500
            // Note that files are sorted by smallest key.
            assert!(db_test.put("300", "v300").is_ok());
            assert!(db_test.put("500", "v500").is_ok());
            ignore!(db_test.db().test_compact_mem_table());
            assert!(db_test.put("200", "v200").is_ok());
            assert!(db_test.put("600", "v600").is_ok());
            assert!(db_test.put("900", "v900").is_ok());
            ignore!(db_test.db().test_compact_mem_table());
            assert_eq!(db_test.files_per_level(), "2,1,1");

            // Compact away the placeholder files we created initially
            db_test.db().test_compact_range(1, None, None);
            db_test.db().test_compact_range(2, None, None);
            assert_eq!(db_test.files_per_level(), "2");

            assert!(db_test.delete("600").is_ok());
            ignore!(db_test.db().test_compact_mem_table());
            assert_eq!(db_test.files_per_level(), "3");
            assert_eq!(db_test.get("600"), "NotFound");

            db_test.change_options()
        } {}
    }

    #[test]
    fn test_l0_compaction_bug_issue44_a() {
        let mut db_test = DBTest::new("test_l0_compaction_bug_issue44_a");
        db_test.reopen(None);
        assert!(db_test.put("b", "v").is_ok());
        db_test.reopen(None);
        assert!(db_test.delete("b").is_ok());
        assert!(db_test.delete("a").is_ok());
        db_test.reopen(None);
        assert!(db_test.delete("a").is_ok());
        db_test.reopen(None);
        assert!(db_test.put("a", "v").is_ok());
        db_test.reopen(None);
        db_test.reopen(None);
        assert_eq!(db_test.contents(), "(a->v)");
        delay(1000);
        assert_eq!(db_test.contents(), "(a->v)");
    }

    #[test]
    fn test_l0_compaction_bug_issue44_b() {
        let mut db_test = DBTest::new("test_l0_compaction_bug_issue44_b");
        db_test.reopen(None);
        db_test.put("", "").unwrap();
        db_test.reopen(None);
        db_test.delete("e").unwrap();
        db_test.put("", "").unwrap();
        db_test.reopen(None);
        db_test.put("c", "cv").unwrap();
        db_test.reopen(None);
        db_test.put("", "").unwrap();
        db_test.reopen(None);
        db_test.put("", "").unwrap();
        delay(1000);
        db_test.reopen(None);
        db_test.put("d", "dv").unwrap();
        db_test.reopen(None);
        db_test.put("", "").unwrap();
        db_test.reopen(None);
        db_test.delete("d").unwrap();
        db_test.delete("b").unwrap();
        db_test.reopen(None);

        assert_eq!(db_test.contents(), "(->)(c->cv)");
        delay(1000);
        assert_eq!(db_test.contents(), "(->)(c->cv)");
    }

    #[test]
    fn test_comparator_check() {
        struct NewComparator;
        impl BaseComparator for NewComparator {
            fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
                BytewiseComparatorImpl::new().compare(a, b)
            }
        }
        impl Comparator for NewComparator {
            fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
                BytewiseComparatorImpl::new().find_shortest_separator(start, limit)
            }

            fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
                BytewiseComparatorImpl::new().find_short_successor(key)
            }

            fn name(&self) -> &'static str {
                "NewComparator"
            }
        }
        let cmp = NewComparator;
        let mut db_test = DBTest::new("test_comparator_check");
        let mut opt = db_test.current_options();
        opt.comparator = Arc::new(cmp);
        let ret = db_test.try_reopen(Some(opt));
        assert!(ret.is_err());
        assert_eq!(
            &format!("{}", ret.err().unwrap()),
            "Comparator does not match"
        );
        ignore!(DB::destroy_db(&db_test.name, Options::default()));
    }

    // use static variable to optimize the comparator performance.
    lazy_static! {
        static ref RE: Regex = Regex::new(r"\[(?P<s>.*?)\]").unwrap();
    }

    fn to_number(x: &[u8]) -> u64 {
        let s = unsafe { std::str::from_utf8_unchecked(x).to_string() };

        let mut val = "".to_string();
        for cap in RE.captures_iter(&s) {
            val = cap["s"].to_string();
        }

        if val.starts_with("0x") {
            let without_prefix = val.trim_start_matches("0x");
            let z = u64::from_str_radix(without_prefix, 16);
            z.unwrap()
        } else {
            val.parse::<u64>().unwrap()
        }
    }

    #[test]
    fn test_custom_comparator() {
        struct NumberComparator;
        impl BaseComparator for NumberComparator {
            fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
                to_number(a).cmp(&to_number(b))
            }
        }
        impl Comparator for NumberComparator {
            fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
                to_number(start);
                to_number(limit);
                start.to_owned()
            }

            fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
                to_number(key);
                key.to_owned()
            }

            fn name(&self) -> &'static str {
                "NumberComparator"
            }
        }
        let cmp = NumberComparator;
        let mut db_test = DBTest::new("test_custom_comparator");
        let mut opt = db_test.current_options();
        opt.create_if_missing = true;
        opt.comparator = Arc::new(cmp);
        opt.filter_policy = None;
        opt.write_buffer_size = 1000;
        db_test.destroy_and_reopen(Some(opt));
        assert!(db_test.put("[10]", "ten").is_ok());
        assert!(db_test.put("[0x14]", "twenty").is_ok());
        for _i in 0..2 {
            assert_eq!(db_test.get("[10]"), "ten");
            assert_eq!(db_test.get("[0xa]"), "ten");
            assert_eq!(db_test.get("[20]"), "twenty");
            assert_eq!(db_test.get("[0x14]"), "twenty");
            assert_eq!(db_test.get("[15]"), "NotFound");
            assert_eq!(db_test.get("[0xf]"), "NotFound");
            db_test.compact("[0]", "[9999]");
        }
        for _run in 0..2 {
            for i in 0..1000 {
                let s = format!("[{}]", i * 10);
                assert!(db_test.put(&s, &s).is_ok());
            }
            db_test.compact("[0]", "[1000000]");
        }
    }

    #[test]
    fn test_manual_compaction() {
        let mut db_test = DBTest::new("test_manual_compaction");
        db_test.make_tables(3, "p", "q");
        db_test.compact("", "c");
        assert_eq!(db_test.files_per_level(), "1,1,1");

        db_test.compact("r", "z");
        assert_eq!(db_test.files_per_level(), "1,1,1");

        db_test.compact("p1", "p9");
        assert_eq!(db_test.files_per_level(), "0,0,1");

        db_test.make_tables(3, "c", "e");
        assert_eq!(db_test.files_per_level(), "1,1,2");

        db_test.compact("b", "f");
        assert_eq!(db_test.files_per_level(), "0,0,2");

        db_test.make_tables(1, "a", "z");
        assert_eq!(db_test.files_per_level(), "0,1,2");
        db_test.db().compact_range(None, None);
        assert_eq!(db_test.files_per_level(), "0,0,1");
    }

    #[test]
    fn test_db_open_options() {
        let name = "/tmp/test_data_db_options_test".to_string();
        ignore!(DB::destroy_db(&name, Options::default()));

        let mut opt = Options::default();
        opt.create_if_missing = false;
        let ret = DB::open(name.clone(), opt.clone());
        assert_eq!(
            format!("{}", ret.err().unwrap()),
            "db does not exist (create_if_missing is false)"
        );
        ignore!(DB::destroy_db(&name, Options::default()));

        let mut opt = Options::default();
        opt.create_if_missing = true;
        DB::open(name.clone(), opt.clone()).unwrap();

        let mut opt = Options::default();
        opt.create_if_missing = false;
        opt.error_if_exists = true;
        let ret = DB::open(name.clone(), opt.clone());
        assert_eq!(
            format!("{}", ret.err().unwrap()),
            "db exists (error_if_exists is true)"
        );
        ignore!(DB::destroy_db(&name, Options::default()));

        let mut opt = Options::default();
        opt.create_if_missing = true;
        opt.error_if_exists = false;
        DB::open(name.clone(), opt.clone()).unwrap();
        ignore!(DB::destroy_db(&name, Options::default()));
    }

    #[test]
    #[ignore]
    fn test_destroy_open_db() {
        let name = "/tmp/open_db_dir".to_string();
        let mut opt = Options::default();
        opt.create_if_missing = true;
        DB::open(name.clone(), opt.clone()).unwrap();
        assert!(DB::destroy_db(&name, Options::default()).is_err());
    }

    #[test]
    fn test_locking() {
        // todo
    }

    #[test]
    fn test_no_space() {
        let mut db_test = DBTest::new("test_no_space");
        let mut opt = db_test.current_options();
        opt.env = db_test.env.clone();
        db_test.reopen(Some(opt));

        assert!(db_test.put("foo", "v1").is_ok());
        assert_eq!(db_test.get("foo"), "v1");
        db_test.compact("a", "z");
        let num_files = db_test.count_files();
        db_test
            .env
            .no_space
            .store(true, std::sync::atomic::Ordering::Release);
        for _i in 0..10 {
            for level in 0..LEVEL_NUMBER - 1 {
                db_test.db().test_compact_range(level, None, None);
            }
        }
        db_test
            .env
            .no_space
            .store(false, std::sync::atomic::Ordering::Release);
        assert!(db_test.count_files() < num_files + 3);
    }

    #[test]
    fn test_non_writable_file_system() {
        let mut db_test = DBTest::new("test_non_writable_file_system");
        let mut opt = db_test.current_options();
        opt.write_buffer_size = 1000;
        opt.env = db_test.env.clone();
        db_test.reopen(Some(opt));

        assert!(db_test.put("foo", "v1").is_ok());
        db_test
            .env
            .non_writable
            .store(true, std::sync::atomic::Ordering::Release);

        let big = "x".repeat(100000);
        let mut errors = 0;
        for _i in 0..20 {
            if db_test.put("foo", &big).is_err() {
                errors += 1;
                delay(100);
            }
        }
        assert!(errors > 0);
    }

    #[test]
    fn test_write_sync_error() {
        // Check that log sync errors cause the DB to disallow future writes.

        // (a) Cause log sync calls to fail
        let mut db_test = DBTest::new("test_write_sync_error");
        let mut opt = db_test.current_options();
        opt.env = db_test.env.clone();
        db_test.reopen(Some(opt));

        db_test
            .env
            .data_sync_error
            .store(true, std::sync::atomic::Ordering::Release);

        // (b) Normal write should succeed
        assert!(db_test.put("k1", "v1").is_ok());
        assert_eq!(db_test.get("k1"), "v1");

        // (c) Do a sync write; should fail
        assert!(db_test
            .db()
            .put(b"k2", b"v2", WriteOptions { sync: true })
            .is_err());
        assert_eq!(db_test.get("k1"), "v1");
        assert_eq!(db_test.get("k2"), "NotFound");

        // (d) make sync behave normally
        db_test
            .env
            .data_sync_error
            .store(false, std::sync::atomic::Ordering::Release);

        // (e) Do a non-sync write; should fail
        assert!(db_test.put("k3", "v3").is_err());
        assert_eq!(db_test.get("k1"), "v1");
        assert_eq!(db_test.get("k2"), "NotFound");
        assert_eq!(db_test.get("k3"), "NotFound");
    }

    #[test]
    fn test_manifest_write_error() {
        // Test for the following problem:
        // (a) Compaction produces file F
        // (b) Log record containing F is written to MANIFEST file, but Sync() fails
        // (c) GC deletes F
        // (d) After reopening DB, reads fail since deleted F is named in log record

        // We iterate twice.  In the second iteration, everything is the
        // same except the log record never makes it to the MANIFEST file.
        let mut db_test = DBTest::new("test_manifest_write_error");
        for iter in 0..2 {
            let error_type = if iter == 0 {
                db_test.env.manifest_sync_error.clone()
            } else {
                db_test.env.manifest_write_error.clone()
            };
            let mut opt = db_test.current_options();
            opt.env = db_test.env.clone();
            opt.create_if_missing = true;
            opt.error_if_exists = false;
            let opt = Some(opt);
            db_test.destroy_and_reopen(opt.clone());
            assert!(db_test.put("foo", "bar").is_ok());
            assert_eq!(db_test.get("foo"), "bar");
            // Memtable compaction (will succeed)
            ignore!(db_test.db().test_compact_mem_table());
            assert_eq!(db_test.get("foo"), "bar");
            let last = MAX_MEM_COMPACT_LEVEL;
            assert_eq!(db_test.num_table_files_at_level(last as u64), 1);
            // Merging compaction (will fail)
            error_type.store(true, std::sync::atomic::Ordering::Release);
            db_test.db().test_compact_range(last, None, None);
            assert_eq!(db_test.get("foo"), "bar");
            // Recovery: should not lose data
            error_type.store(false, std::sync::atomic::Ordering::Release);
            db_test.reopen(opt.clone());
            assert_eq!(db_test.get("foo"), "bar");
        }
    }

    #[test]
    fn test_missing_table_file() {
        let mut db_test = DBTest::new("test_missing_table_file");
        assert!(db_test.put("foo", "bar").is_ok());
        assert_eq!(db_test.get("foo"), "bar");
        // Dump the memtable to disk.
        ignore!(db_test.db().test_compact_mem_table());
        assert_eq!(db_test.get("foo"), "bar");

        // db_test.close();
        assert!(db_test.delete_an_table_file());
        let mut opt = db_test.current_options();
        opt.paranoid_checks = true;
        let ret = db_test.try_reopen(Some(opt));
        assert!(ret.is_err());
        assert_eq!(format!("{}", ret.err().unwrap()), "missing log files");
        ignore!(DB::destroy_db(&db_test.name, Options::default()));
    }

    #[test]
    #[ignore]
    fn test_files_deleted_after_compaction() {
        let mut db_test = DBTest::new("test_files_deleted_after_compaction");
        assert!(db_test.put("foo", "v2").is_ok());
        db_test.compact("a", "z");
        let num_files = db_test.count_files();
        for _i in 0..10 {
            assert!(db_test.put("foo", "v2").is_ok());
            db_test.compact("a", "z");
        }
        assert_eq!(db_test.count_files(), num_files);
    }

    #[test]
    fn test_bloom_filter() {
        let mut db_test = DBTest::new("test_bloom_filter");
        db_test
            .env
            .count_random_reads
            .store(true, std::sync::atomic::Ordering::Release);
        let mut opt = db_test.current_options();
        opt.env = db_test.env.clone();
        opt.block_cache = Some(Arc::new(Mutex::new(SharedLRUCache::new(0))));
        opt.filter_policy = Some(Arc::new(BloomFilterPolicy::new(10)));
        db_test.reopen(Some(opt));

        let n = 10000;
        for i in 0..n {
            assert!(db_test.put(&key(i), &key(i)).is_ok());
        }
        db_test.compact("a", "z");
        ignore!(db_test.db().test_compact_mem_table());
        db_test
            .env
            .delay_data_sync
            .store(true, std::sync::atomic::Ordering::Release);
        db_test.env.random_read_counter.reset();

        for i in 0..n {
            assert_eq!(&db_test.get(&key(i)), &key(i));
        }
        let reads = db_test.env.random_read_counter.read();
        println!("{} present => {} reads", n, reads);
        assert!(reads >= n);
        assert!(reads <= n + 2 * n / 100);

        db_test.env.random_read_counter.reset();
        for i in 0..n {
            assert_eq!(db_test.get(&format!("{}.missing", key(i))), "NotFound");
        }
        let reads = db_test.env.random_read_counter.read();
        println!("{} missing => {} reads", n, reads);
        assert!(reads <= 3 * n / 100);
        db_test
            .env
            .delay_data_sync
            .store(false, std::sync::atomic::Ordering::Release);
    }

    #[test]
    fn test_multi_threaded() {}

    struct ModelDB {
        map: RBMap<Vec<u8>, Vec<u8>>,
    }

    struct ModelIter<'a> {
        pub map: &'a RBMap<Vec<u8>, Vec<u8>>,
        index: usize,
        key: Vec<u8>,
        value: Vec<u8>,
    }

    impl<'a> Iter for ModelIter<'a> {
        fn valid(&self) -> bool {
            self.index != self.map.len()
        }

        fn seek_to_first(&mut self) {
            self.index = 0;
            if self.valid() {
                if let Some(tmp) = self.map.iter().skip(self.index).next() {
                    self.key = tmp.0.clone();
                    self.value = tmp.1.clone();
                }
            }
        }

        fn seek_to_last(&mut self) {
            // if self.map.is_empty() {
            //     self.index = self.map.len();
            // } else {
            //     self.index = self.map.len() - 1;
            // }
            // if self.valid() {
            //     if let Some(tmp) = self.map.iter().skip(self.index).next() {
            //         self.key = tmp.0.as_bytes().to_vec();
            //         self.value = tmp.1.as_bytes().to_vec();
            //     }
            // }
        }

        fn seek(&mut self, _target: &[u8]) {
            // let mut list = vec![];
            // for i in self.map.iter() {
            //     list.push((i.0.clone(), i.1.clone()));
            // }
            // self.index = list.lower_bound_by(
            //     |a, b| a.0.cmp(&b.0),
            //     &(
            //         unsafe { String::from_utf8_unchecked(target.to_vec()) },
            //         String::default(),
            //     ),
            // );
            // if self.valid() {
            //     if let Some(tmp) = self.map.iter().skip(self.index).next() {
            //         self.key = tmp.0.as_bytes().to_vec();
            //         self.value = tmp.1.as_bytes().to_vec();
            //     }
            // }
        }

        fn next(&mut self) {
            self.index += 1;
            if self.valid() {
                if let Some(tmp) = self.map.iter().skip(self.index).next() {
                    self.key = tmp.0.clone();
                    self.value = tmp.1.clone();
                }
            }
        }

        fn prev(&mut self) {
            // self.index -= 1;
            // if self.valid() {
            //     if let Some(tmp) = self.map.iter().skip(self.index).next() {
            //         self.key = tmp.0.as_bytes().to_vec();
            //         self.value = tmp.1.as_bytes().to_vec();
            //     }
            // }
        }

        fn key(&self) -> &[u8] {
            self.key.as_slice()
        }

        fn value(&self) -> &[u8] {
            self.value.as_slice()
        }
    }

    #[derive(Clone)]
    struct ModelSnapshot {
        map: RBMap<Vec<u8>, Vec<u8>>,
    }

    impl ModelDB {
        fn put(&mut self, key: &[u8], value: &[u8], opt: WriteOptions) -> DResult<()> {
            let mut batch = WriteBatch::new();
            batch.put(key, value);
            self.write(Some(batch), opt)
        }

        fn delete(&mut self, key: &[u8], opt: WriteOptions) -> DResult<()> {
            let mut batch = WriteBatch::new();
            batch.delete(key);
            self.write(Some(batch), opt)
        }

        fn write(&mut self, batch: Option<WriteBatch>, _opt: WriteOptions) -> DResult<()> {
            let b = batch.unwrap();
            struct MockHandler<'a> {
                map: &'a mut RBMap<Vec<u8>, Vec<u8>>,
            }

            impl<'a> TableInserter for MockHandler<'a> {
                fn put(&mut self, key: &[u8], val: &[u8]) {
                    self.map.insert(key.to_vec(), val.to_vec());
                }

                fn delete(&mut self, key: &[u8]) {
                    self.map.remove(&key.to_vec());
                }
            }

            let mut handler = MockHandler { map: &mut self.map };
            b.iterate(&mut handler)
        }

        fn new_iter<'a>(&'a self, snapshot: Option<&'a ModelSnapshot>) -> ModelIter<'a> {
            if let Some(snap) = &snapshot {
                ModelIter {
                    map: &snap.map,
                    index: 0,
                    key: vec![],
                    value: vec![],
                }
            } else {
                // let saved = self.map;
                ModelIter {
                    map: &self.map,
                    index: 0,
                    key: vec![],
                    value: vec![],
                }
            }
        }

        fn get_snapshot(&self) -> ModelSnapshot {
            ModelSnapshot {
                map: self.map.clone(),
            }
        }
    }

    fn compare_iterators(
        model: &ModelDB,
        db: &DB,
        step: u64,
        model_snap: Option<&ModelSnapshot>,
        db_snap: Option<Arc<RwLock<SnapNode>>>,
    ) -> bool {
        let mut ok = true;
        let mut model_iter = model.new_iter(model_snap);
        let mut read_opt = ReadOptions::new();
        read_opt.snapshot = db_snap.clone();
        let mut db_iter = db.new_iter(read_opt);
        let mut count = 0;
        model_iter.seek_to_first();
        db_iter.seek_to_first();

        while ok && model_iter.valid() && db_iter.valid() {
            count += 1;
            if model_iter.key() != db_iter.key() {
                #[rustfmt::skip]
                eprintln!("step {}: Key mismatch: '{:?}' vs '{:?}'", step, model_iter.key(), db_iter.key());
                ok = false;
                break;
            }
            if model_iter.value() != db_iter.value() {
                #[rustfmt::skip]
                eprintln!("step {}: Value mismatch: '{:?}' vs '{:?}'", step, model_iter.value(), db_iter.value()
                );
                ok = false;
            }
            model_iter.next();
            db_iter.next();
        }

        if ok {
            if model_iter.valid() != db_iter.valid() {
                #[rustfmt::skip]
                eprintln!("step {}: Mismatch at end of iterators: {} vs {}", step, model_iter.valid(), db_iter.valid()
                );
                ok = false;
            }
        }
        println!("{} entries compared: ok={}", count, ok);
        ok
    }

    struct Wrapper {
        name: String,
        db: Option<DB>,
        option_config: u64,
        filter_policy: Option<Arc<dyn FilterPolicy + Send + Sync>>,
    }

    impl Wrapper {
        fn new(name: &str) -> Self {
            let mut db = Self {
                db: None,
                filter_policy: Some(Arc::new(BloomFilterPolicy::new(10))),
                option_config: OptionConfig::Default as u64,
                name: format!("{}{}{}", TEST_DB, name, get_micro()),
            };
            ignore!(DB::destroy_db(&db.name, Options::default()));
            db.try_reopen(None).unwrap();
            db
        }

        fn db(&self) -> &DB {
            self.db.as_ref().unwrap()
        }

        fn try_reopen(&mut self, opt: Option<Options>) -> DResult<()> {
            // stop the thread.
            if let Some(db) = &self.db {
                ignore!(db.close());
            }

            self.db = None;
            let option;
            if let Some(o) = opt {
                option = o;
            } else {
                let mut opt = self.current_options();
                opt.create_if_missing = true;
                option = opt
            }
            match DB::open(self.name.clone(), option) {
                Ok(db) => self.db = Some(db),
                Err(err) => return Err(err),
            }
            Ok(())
        }

        fn current_options(&self) -> Options {
            let mut option = Options::default();
            // mock file system
            option.reuse_logs = false;
            match self.option_config.into() {
                OptionConfig::Reuse => {
                    option.reuse_logs = true;
                }
                OptionConfig::Filter => {
                    option.filter_policy = self.filter_policy.clone();
                }
                OptionConfig::Uncompressed => {
                    option.compression = CompressionType::NoCompress;
                }
                _ => {}
            }
            option
        }

        fn destroy_and_reopen(&mut self, opt: Option<Options>) {
            // stop the thread.
            if let Some(db) = &self.db {
                ignore!(db.close());
            }

            ignore!(DB::destroy_db(&self.name, Options::default()));
            self.db = None;
            assert!(self.try_reopen(opt).is_ok());
        }

        fn change_options(&mut self) -> bool {
            self.option_config += 1;
            if self.option_config >= OptionConfig::End as u64 {
                false
            } else {
                self.destroy_and_reopen(None);
                true
            }
        }
    }

    #[test]
    fn test_randomized() {
        let rnd = Random::new(301);
        let mut w = Wrapper::new("test_randomized");

        let mut tmp;

        let w_opt = WriteOptions::default();
        while {
            let mut model = ModelDB { map: RBMap::new() };
            let n = 5000; // 10000
            let mut k = String::default();
            let mut v;

            let mut model_snap: Option<&ModelSnapshot> = None;
            let mut db_snap: Option<Arc<RwLock<SnapNode>>> = None;

            for step in 0..n {
                if step % 100 == 0 {
                    println!("step {} of {}", step, n);
                }
                let p = rnd.uniform(100);
                if p < 45 {
                    k = random_key(&rnd);
                    v = random_string(
                        &rnd,
                        if rnd.one_in(20) {
                            100 + rnd.uniform(100)
                        } else {
                            rnd.uniform(8)
                        },
                    );
                    assert!(model.put(k.as_bytes(), v.as_bytes(), w_opt.clone()).is_ok());
                    assert!(w
                        .db()
                        .put(k.as_bytes(), v.as_bytes(), w_opt.clone())
                        .is_ok())
                } else if p < 90 {
                    k = random_key(&rnd);
                    assert!(model.delete(k.as_bytes(), w_opt.clone()).is_ok());
                    assert!(w.db().delete(k.as_bytes(), w_opt.clone()).is_ok())
                } else {
                    let mut b = WriteBatch::new();
                    let num = rnd.uniform(8);
                    for i in 0..num {
                        if i == 0 || !rnd.one_in(10) {
                            k = random_key(&rnd);
                        }
                        if rnd.one_in(2) {
                            v = random_string(&rnd, rnd.uniform(10));
                            b.put(k.as_bytes(), v.as_bytes());
                        } else {
                            b.delete(k.as_bytes());
                        }
                    }
                    assert!(model
                        .write(Some(WriteBatch { rep: b.rep.clone() }), w_opt.clone())
                        .is_ok());
                    assert!(w.db().write(Some(b), w_opt.clone()).is_ok());
                }

                if step % 100 == 0 {
                    assert!(compare_iterators(&model, w.db(), step, None, None));
                    assert!(compare_iterators(
                        &model,
                        w.db(),
                        step,
                        model_snap,
                        db_snap.clone(),
                    ));
                    // if let Some(s) = &mut model_snap {
                    //     model.release_snapshot(s)
                    // }
                    if let Some(d) = &mut db_snap {
                        w.db().release_snapshot(&mut *d.write().unwrap());
                    }

                    w.try_reopen(None).unwrap();
                    assert!(compare_iterators(&model, w.db(), step, None, None));
                    tmp = model.get_snapshot();
                    model_snap = Some(&tmp);
                    db_snap = Some(w.db().get_snapshot());
                }
            }
            // if let Some(s) = &mut model_snap {
            //     model.release_snapshot(s)
            // }
            if let Some(d) = &mut db_snap {
                w.db().release_snapshot(&mut *d.write().unwrap());
            }

            w.change_options()
        } {}
        ignore!(DB::destroy_db(&w.name, Options::default()));
    }

    // #[bench]
    // fn test_BM_LogAndApply() {}
}
