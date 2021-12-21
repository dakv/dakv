#[cfg(test)]
mod test {
    use crate::db::{append_internal_key, DError, MemoryTable, ParsedInternalKey, ValueType};
    use crate::db::{
        CompressionType, DResult, Database, Options, ReadOptions, WriteBatch, WriteOptions,
        LEVEL_NUMBER, MAX_SEQUENCE_NUMBER,
    };
    use crate::env::TestEnv;
    use crate::env::{RandomAccessFile, WritableFile};
    use crate::table::block::{Block, BlockContents};
    use crate::table::block_builder::BlockBuilder;
    use crate::table::table_reader::Table;
    use crate::table::TableBuilder;
    use crate::utils::algorithm::LowerBound;
    use crate::utils::cmp::{
        BaseComparator, BytewiseComparatorImpl, Comparator, InternalKeyComparator,
        InternalKeyComparatorImpl,
    };
    use crate::utils::coding::decode_u64_le;
    use crate::utils::constants::PROPERTY_PREFIX;
    use crate::utils::iter::Iter;
    use crate::utils::random::{Random, RandomGenerator};
    use crate::utils::time::get_micro;
    use crate::DB;
    use lazy_static::lazy_static;
    use rb_tree::{Comparator as MapComparator, RBMapWithCmp};
    use regex::Regex;
    use std::cell::RefCell;
    use std::cmp::Ordering;
    use std::mem;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    fn reverse(data: &[u8]) -> Vec<u8> {
        let len = data.len();
        let mut result = Vec::with_capacity(len);
        for i in (0..len).rev() {
            result.push(data[i]);
        }
        result
    }

    struct StringSource {
        contents: Rc<RefCell<Vec<u8>>>,
    }

    impl StringSource {
        pub fn new(contents: Rc<RefCell<Vec<u8>>>) -> Self {
            Self { contents }
        }
    }

    impl RandomAccessFile for StringSource {
        fn read(&self, offset: usize, result: &mut [u8]) -> DResult<usize> {
            if offset > self.contents.borrow().len() {
                panic!();
            }
            let n = result.len();
            result.copy_from_slice(&self.contents.borrow().as_slice()[offset..offset + n]);
            Ok(n)
        }
    }

    struct StringSink {
        content: Rc<RefCell<Vec<u8>>>,
    }

    impl StringSink {
        pub fn new(content: Rc<RefCell<Vec<u8>>>) -> Self {
            Self { content }
        }
    }

    impl WritableFile for StringSink {
        fn append(&self, buf: &[u8]) -> DResult<()> {
            self.content.borrow_mut().extend_from_slice(buf);
            Ok(())
        }

        fn flush(&self) -> DResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_reverse() {
        assert_eq!(reverse(b"da").as_slice(), b"ad");
        assert_eq!(reverse(b"").as_slice(), b"");
        assert_eq!(reverse(b"reverse").as_slice(), b"esrever");
    }

    struct ReverseKeyComparatorImpl;

    impl ReverseKeyComparatorImpl {
        fn new() -> Self {
            Self
        }
    }

    impl BaseComparator for ReverseKeyComparatorImpl {
        fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
            BytewiseComparatorImpl::new().compare(&reverse(a), &reverse(b))
        }
    }

    impl Comparator for ReverseKeyComparatorImpl {
        fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
            let mut s = reverse(start);
            let l = reverse(limit);
            s = BytewiseComparatorImpl::new().find_shortest_separator(&s, &l);
            reverse(&s)
        }

        fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
            let mut s = reverse(key);
            s = BytewiseComparatorImpl::new().find_short_successor(&s);
            reverse(&s)
        }

        fn name(&self) -> &'static str {
            "ReverseBytewiseComparator"
        }
    }

    fn increment(cmp: Arc<dyn Comparator>, key: &mut Vec<u8>) {
        if cmp.name() == "BytewiseComparator" {
            key.push(0);
        } else {
            let mut rev = reverse(key.as_slice());
            rev.push(0);
            *key = reverse(&rev);
        }
    }

    trait Constructor {
        fn add(&mut self, key: &str, value: &str) {
            self.data()
                .borrow_mut()
                .insert(key.as_bytes().to_vec(), value.as_bytes().to_vec());
        }

        fn finish(&mut self, options: Options);
        fn new_iter(&mut self) -> Box<dyn Iter>;

        // use Rc<RefCell<KVMap>> to make the compiler happy.
        fn data(&self) -> Rc<RefCell<KVMap>>;
        fn db(&mut self) -> &mut DB;
        fn cmp(&self) -> Arc<dyn Comparator>;
    }

    struct BlockConstructor {
        content: Vec<u8>,
        data: Rc<RefCell<KVMap>>,
        block: Option<Block>,
        cmp: Arc<dyn Comparator>,
    }

    impl BlockConstructor {
        pub fn new(cmp: Arc<dyn Comparator>) -> Self {
            Self {
                content: Default::default(),
                data: Rc::new(RefCell::new(KVMap::new(LessThan::new(cmp.clone())))),
                block: None,
                cmp,
            }
        }
    }

    impl Constructor for BlockConstructor {
        fn finish(&mut self, options: Options) {
            let mut builder = BlockBuilder::new(options.clone());
            for (k, v) in self.data().borrow().iter() {
                builder.add(k.as_slice(), v.as_slice());
            }
            self.content = builder.finish().clone();

            // Open the block
            let mut contents = BlockContents::new();
            contents.data = self.content.clone();
            contents.heap_allocated = false;
            contents.cacheable = false;
            self.block = Some(Block::new(Arc::new(contents)));
        }

        fn new_iter(&mut self) -> Box<dyn Iter> {
            self.block.as_ref().unwrap().new_iter(self.cmp.clone())
        }

        fn data(&self) -> Rc<RefCell<KVMap>> {
            self.data.clone()
        }

        fn db(&mut self) -> &mut DB {
            unimplemented!()
        }

        fn cmp(&self) -> Arc<dyn Comparator> {
            self.cmp.clone()
        }
    }

    struct TableConstructor {
        table: Option<Table>,
        source: Option<Arc<dyn RandomAccessFile>>,
        data: Rc<RefCell<KVMap>>,
        cmp: Arc<dyn Comparator>,
    }

    impl TableConstructor {
        pub fn new(cmp: Arc<dyn Comparator>) -> Self {
            Self {
                table: None,
                source: None,
                data: Rc::new(RefCell::new(KVMap::new(LessThan::new(cmp.clone())))),
                cmp,
            }
        }

        pub fn reset(&mut self) {
            self.table = None;
            self.source = None;
        }

        pub fn approximate_offset_of(&self, key: &str) -> u64 {
            self.table
                .as_ref()
                .unwrap()
                .approximate_offset_of(key.as_bytes())
        }
    }

    impl Constructor for TableConstructor {
        fn finish(&mut self, options: Options) {
            self.reset();
            let store = Rc::new(RefCell::new(vec![]));

            let sink = Arc::new(StringSink::new(store.clone()));
            let mut builder = TableBuilder::new(options.clone(), sink);

            for (k, v) in self.data().borrow().iter() {
                builder.add(k.as_slice(), v.as_slice());
            }
            builder.finish().unwrap();

            let size = store.borrow().len();
            assert_eq!(size, builder.file_size() as usize);

            self.source = Some(Arc::new(StringSource::new(store.clone())));

            let mut table_opt = Options::default();
            table_opt.comparator = options.comparator.clone();

            self.table =
                Some(Table::open(table_opt, self.source.as_ref().unwrap().clone(), size).unwrap());
        }

        fn new_iter(&mut self) -> Box<dyn Iter> {
            Box::new(self.table.as_ref().unwrap().new_iter(ReadOptions::new()))
        }

        fn data(&self) -> Rc<RefCell<KVMap>> {
            self.data.clone()
        }

        fn db(&mut self) -> &mut DB {
            unimplemented!()
        }

        fn cmp(&self) -> Arc<dyn Comparator> {
            self.cmp.clone()
        }
    }

    // A helper class that converts internal format keys into user keys
    struct KeyConvertingIterator {
        iter: Box<dyn Iter>,
        status: RefCell<DResult<()>>,
    }

    impl KeyConvertingIterator {
        pub fn new(iter: Box<dyn Iter>) -> Self {
            Self {
                iter,
                status: RefCell::new(Ok(())),
            }
        }
    }

    impl Iter for KeyConvertingIterator {
        fn valid(&self) -> bool {
            self.iter.valid()
        }

        fn seek_to_first(&mut self) {
            self.iter.seek_to_first()
        }

        fn seek_to_last(&mut self) {
            self.iter.seek_to_last()
        }

        fn seek(&mut self, target: &[u8]) {
            let ikey = ParsedInternalKey::new(target, MAX_SEQUENCE_NUMBER, ValueType::TypeValue);
            let mut encoded = vec![];
            append_internal_key(&mut encoded, &ikey);
            self.iter.seek(&encoded);
        }

        fn next(&mut self) {
            self.iter.next()
        }

        fn prev(&mut self) {
            self.iter.prev()
        }

        fn key(&self) -> &[u8] {
            assert!(self.valid());
            let mut key: &[u8] = &[];
            if !parse_internal_key(self.iter.key(), &mut key) {
                *self.status.borrow_mut() = Err(DError::CustomError("malformed internal key"));
                b"corrupted key"
            } else {
                key
            }
        }

        fn value(&self) -> &[u8] {
            self.iter.value()
        }

        fn status(&self) -> DResult<()> {
            if self.status.borrow().is_ok() {
                self.iter.status()
            } else {
                let s = self.status.borrow();
                s.clone()
            }
        }
    }

    fn parse_internal_key<'a>(internal_key: &'a [u8], user_key: &mut &'a [u8]) -> bool {
        let n = internal_key.len();
        if n < 8 {
            return false;
        }
        // [n - 8..n] is the SequenceNumber | ValueType portion.
        let num = decode_u64_le(&internal_key[n - 8..n]);
        let c = num & 0xff;

        *user_key = &internal_key[0..n - 8];
        c <= ValueType::TypeValue.into()
    }

    struct MemTableConstructor {
        data: Rc<RefCell<KVMap>>,
        memtable: MemoryTable,
        internal_comparator: Arc<dyn InternalKeyComparator + Send + Sync>,
    }

    impl MemTableConstructor {
        pub fn new(cmp: Arc<dyn InternalKeyComparator + Send + Sync>) -> Self {
            let mem = MemoryTable::new(cmp.clone());
            Self {
                data: Rc::new(RefCell::new(KVMap::new(LessThan::new(
                    cmp.user_comparator().clone(),
                )))),
                memtable: mem,
                internal_comparator: cmp.clone(),
            }
        }
    }

    impl Constructor for MemTableConstructor {
        fn finish(&mut self, _: Options) {
            self.memtable = MemoryTable::new(self.internal_comparator.clone());
            let mut seq = 1;
            for (k, v) in self.data().borrow().iter() {
                self.memtable
                    .set(seq, ValueType::TypeValue, k.as_slice(), v.as_slice());
                seq += 1;
            }
        }

        fn new_iter(&mut self) -> Box<dyn Iter> {
            Box::new(KeyConvertingIterator::new(Box::new(
                self.memtable.new_iter(),
            )))
        }

        fn data(&self) -> Rc<RefCell<KVMap>> {
            self.data.clone()
        }

        fn db(&mut self) -> &mut DB {
            unimplemented!()
        }

        fn cmp(&self) -> Arc<dyn Comparator> {
            self.internal_comparator.user_comparator()
        }
    }

    struct DBConstructor {
        data: Rc<RefCell<KVMap>>,
        db: Option<DB>,
        cmp: Arc<dyn Comparator + Send + Sync>,
    }

    impl DBConstructor {
        pub fn new(cmp: Arc<dyn Comparator + Send + Sync>) -> Self {
            let db = Self::new_db(cmp.clone());
            Self {
                db: Some(db),
                data: Rc::new(RefCell::new(KVMap::new(LessThan::new(cmp.clone())))),
                cmp,
            }
        }

        fn new_db(cmp: Arc<dyn Comparator + Send + Sync>) -> DB {
            let name = format!("test_for_dakv_{}", get_micro());
            let mut opt = Options::default();
            opt.comparator = cmp.clone();
            // we use TestEnv to mock the file system, so actually don't need to destroy_db.
            ignore!(DB::destroy_db(&name, opt.clone()));

            opt.create_if_missing = true;
            opt.error_if_exists = true;
            opt.env = Arc::new(TestEnv::default());
            opt.write_buffer_size = 10000;
            DB::open(name.clone(), opt).unwrap()
        }
    }

    impl Constructor for DBConstructor {
        fn finish(&mut self, _: Options) {
            if let Some(d) = &mut self.db {
                ignore!(d.close());
            }

            let db = Self::new_db(self.cmp.clone());
            self.db = Some(db);
            for (k, v) in self.data().borrow().iter() {
                let mut batch = WriteBatch::new();
                batch.put(k.as_slice(), v.as_slice());
                if let Some(db) = &mut self.db {
                    db.write(Some(batch), WriteOptions::default()).unwrap();
                }
            }
        }

        fn new_iter(&mut self) -> Box<dyn Iter> {
            Box::new(self.db.as_mut().unwrap().new_iter(ReadOptions::new()))
        }

        fn data(&self) -> Rc<RefCell<KVMap>> {
            self.data.clone()
        }

        fn db(&mut self) -> &mut DB {
            self.db.as_mut().unwrap()
        }

        fn cmp(&self) -> Arc<dyn Comparator> {
            self.cmp.clone()
        }
    }

    enum TestType {
        TableTest,
        BlockTest,
        MemtableTest,
        DbTest,
    }

    struct TestArgs {
        type_: TestType,
        reverse_compare: bool,
        restart_interval: i64,
    }

    impl TestArgs {
        fn new(type_: TestType, reverse_compare: bool, restart_interval: i64) -> Self {
            Self {
                type_,
                reverse_compare,
                restart_interval,
            }
        }
    }

    // KVMap is ordered.
    #[derive(Clone)]
    struct LessThan {
        cmp: Arc<dyn Comparator>,
    }

    impl Default for LessThan {
        fn default() -> Self {
            Self {
                cmp: Arc::new(BytewiseComparatorImpl::new()),
            }
        }
    }

    impl LessThan {
        pub fn new(cmp: Arc<dyn Comparator>) -> Self {
            Self { cmp }
        }
    }

    impl MapComparator<Vec<u8>> for LessThan {
        fn cmp(&self) -> Box<dyn Fn(&Vec<u8>, &Vec<u8>) -> Ordering> {
            let cmp = self.cmp.clone();
            Box::new(move |a: &Vec<u8>, b: &Vec<u8>| cmp.compare(&a.as_slice(), &b.as_slice()))
        }
    }

    type KVMap = RBMapWithCmp<Vec<u8>, Vec<u8>, LessThan>;

    lazy_static! {
        static ref K_TEST_ARG_LIST: Vec<TestArgs> = vec![
            // 0 - 5 table test
            TestArgs::new(TestType::TableTest, false, 16),
            TestArgs::new(TestType::TableTest, false, 1),
            TestArgs::new(TestType::TableTest, false, 1024),
            TestArgs::new(TestType::TableTest, true, 16),
            TestArgs::new(TestType::TableTest, true, 1),
            TestArgs::new(TestType::TableTest, true, 1024),
            // 6 - 11 block test
            TestArgs::new(TestType::BlockTest, false, 16),
            TestArgs::new(TestType::BlockTest, false, 1),
            TestArgs::new(TestType::BlockTest, false, 1024),
            TestArgs::new(TestType::BlockTest, true, 16),
            TestArgs::new(TestType::BlockTest, true, 1),
            TestArgs::new(TestType::BlockTest, true, 1024),
            // Restart interval does not matter for memtables
            // 12-13 mem test
            TestArgs::new(TestType::MemtableTest, false, 16),
            TestArgs::new(TestType::MemtableTest, true, 16),
            // Do not bother with restart interval variations for DB
            // 14-15 db test
            TestArgs::new(TestType::DbTest, false, 16),
            TestArgs::new(TestType::DbTest, true, 16),
        ];
        static ref K_TEST_ARG_NUM: usize = K_TEST_ARG_LIST.len();
    }

    fn between(val: u64, low: u64, high: u64) -> bool {
        let result = (val >= low) && (val <= high);
        if !result {
            println!("Value {} is not in range [{}, {}]", val, low, high);
        } else {
            println!("Value {} is not in range [{}, {}]", val, low, high);
        }
        result
    }

    struct Harness {
        constructor: Option<Box<dyn Constructor>>,
        opt: Options,
    }

    impl Harness {
        fn new() -> Self {
            Self {
                constructor: None,
                opt: Options::default(),
            }
        }

        fn init(&mut self, args: &TestArgs) {
            self.constructor = None;
            let mut opt = Options::default();
            opt.block_restart_interval = args.restart_interval;
            // Use shorter block size for tests to exercise block boundary
            // conditions more.
            opt.block_size = 256;
            if args.reverse_compare {
                opt.comparator = Arc::new(ReverseKeyComparatorImpl::new());
            }
            self.opt = opt;
            match args.type_ {
                TestType::TableTest => {
                    self.constructor =
                        Some(Box::new(TableConstructor::new(self.opt.comparator.clone())));
                }
                TestType::BlockTest => {
                    self.constructor =
                        Some(Box::new(BlockConstructor::new(self.opt.comparator.clone())));
                }
                TestType::MemtableTest => {
                    self.constructor = Some(Box::new(MemTableConstructor::new(Arc::new(
                        InternalKeyComparatorImpl::new(self.opt.comparator.clone()),
                    ))));
                }
                TestType::DbTest => {
                    self.constructor =
                        Some(Box::new(DBConstructor::new(self.opt.comparator.clone())));
                }
            }
        }

        fn add(&mut self, key: &str, value: &str) {
            self.constructor.as_mut().unwrap().add(key, value);
        }

        fn db(&mut self) -> &mut DB {
            self.constructor.as_mut().unwrap().db()
        }

        fn test(&mut self, rnd: &impl RandomGenerator) {
            self.constructor.as_mut().unwrap().finish(self.opt.clone());
            self.test_forward_scan();
            self.test_backward_scan();
            self.test_random_access(rnd);
            self.constructor
                .as_ref()
                .unwrap()
                .data()
                .borrow_mut()
                .clear();
        }

        fn test_forward_scan(&mut self) {
            let data = self.constructor.as_ref().unwrap().data();
            let data = data.borrow();
            let data = &data.ordered();
            let mut iter = self.constructor.as_mut().unwrap().new_iter();
            assert!(!iter.valid());
            iter.seek_to_first();

            for (index, _) in data.iter().enumerate() {
                assert_eq!(
                    to_string(data, index).as_bytes(),
                    iter_to_string(&iter).as_bytes()
                );
                iter.next();
            }
            assert!(!iter.valid());
        }

        fn test_backward_scan(&mut self) {
            let data = self.constructor.as_ref().unwrap().data();
            let data = data.borrow();
            let data = &data.ordered();

            let mut iter = self.constructor.as_mut().unwrap().new_iter();
            assert!(!iter.valid());
            iter.seek_to_last();

            for (index, _) in data.iter().rev().enumerate() {
                assert_eq!(
                    to_string(data, data.len() - 1 - index).as_bytes(),
                    iter_to_string(&iter).as_bytes()
                );
                iter.prev();
            }
            assert!(!iter.valid());
        }

        fn test_random_access(&mut self, rnd: &impl RandomGenerator) {
            let data = self.constructor.as_ref().unwrap().data();
            let data = data.borrow();
            let data = &data.ordered();

            let mut iter = self.constructor.as_mut().unwrap().new_iter();
            assert!(!iter.valid());
            let mut model_iter = 0usize;
            for _count in 0..200 {
                let toss = rnd.uniform(5);
                match toss {
                    0 => {
                        if iter.valid() {
                            iter.next();
                            model_iter += 1;
                            assert_eq!(
                                to_string(data, model_iter).as_bytes(),
                                iter_to_string(&iter).as_bytes()
                            );
                        }
                    }
                    1 => {
                        iter.seek_to_first();
                        model_iter = 0;
                        assert_eq!(
                            to_string(data, model_iter).as_bytes(),
                            iter_to_string(&iter).as_bytes()
                        );
                    }
                    // Use data.len() to present iter.end()
                    2 => {
                        let key = pick_random_key(self.opt.comparator.clone(), rnd, data);
                        iter.seek(key.as_slice());

                        let tmp_slice: Vec<_> = data.iter().map(|r| r.0.clone()).collect();
                        let index = tmp_slice.as_slice().lower_bound_by(
                            |a, b| {
                                self.constructor
                                    .as_ref()
                                    .unwrap()
                                    .cmp()
                                    .compare(a.as_slice(), b.as_slice())
                            },
                            &key,
                        );
                        model_iter = index;
                        assert_eq!(
                            to_string(data, model_iter).as_bytes(),
                            iter_to_string(&iter).as_bytes()
                        );
                    }
                    3 => {
                        if iter.valid() {
                            iter.prev();
                            if model_iter == 0 {
                                model_iter = data.len();
                            } else {
                                model_iter -= 1;
                            }
                            assert_eq!(
                                to_string(data, model_iter).as_bytes(),
                                iter_to_string(&iter).as_bytes()
                            );
                        }
                    }
                    4 => {
                        iter.seek_to_last();
                        if data.is_empty() {
                            model_iter = data.len();
                        } else {
                            let key = data[data.len() - 1].0.clone();

                            let tmp_slice: Vec<_> = data.iter().map(|r| r.0.clone()).collect();
                            model_iter = tmp_slice.as_slice().lower_bound_by(
                                |a, b| {
                                    self.constructor
                                        .as_ref()
                                        .unwrap()
                                        .cmp()
                                        .compare(a.as_slice(), b.as_slice())
                                },
                                &key,
                            );
                        }
                        assert_eq!(
                            to_string(data, model_iter).as_bytes(),
                            iter_to_string(&iter).as_bytes()
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    fn pick_random_key(
        cmp: Arc<dyn Comparator>,
        rnd: &impl RandomGenerator,
        keys: &Vec<(&Vec<u8>, &Vec<u8>)>,
    ) -> Vec<u8> {
        if keys.is_empty() {
            b"foo".to_vec()
        } else {
            let index = rnd.uniform(keys.len() as u32) as usize;
            let mut result = keys[index].0.clone();
            match rnd.uniform(3) {
                1 => {
                    let len = result.len();
                    if len > 0 && result[len - 1] > 0 {
                        result[len - 1] -= 1;
                    }
                }
                2 => {
                    increment(cmp, &mut result);
                }
                _ => {}
            }
            result
        }
    }

    fn to_string(data: &Vec<(&Vec<u8>, &Vec<u8>)>, index: usize) -> String {
        return if data.is_empty() || index >= data.len() {
            String::from("END")
        } else {
            format!(
                "'{:?}->{:?}'",
                data[index].0.as_slice(),
                data[index].1.as_slice()
            )
        };
    }

    fn iter_to_string(iter: &Box<dyn Iter>) -> String {
        return if !iter.valid() {
            String::from("END")
        } else {
            let k = iter.key();
            let v = iter.value();
            format!("'{:?}->{:?}'", k, v)
        };
    }

    fn random_key(rnd: &impl RandomGenerator, len: u32) -> String {
        let chars: Vec<u8> = vec![0, 1, 97, 98, 99, 100, 101, 253, 254, 255];
        let mut result = String::default();
        for _ in 0..len {
            result.push(chars[rnd.uniform(chars.len() as u32) as usize] as char)
        }
        result
    }

    fn random_string(rnd: &impl RandomGenerator, len: u32) -> String {
        let mut dst = vec![0; len as usize];
        for i in 0..len {
            dst[i as usize] = (' ' as u32 + rnd.uniform(95)) as u8;
        }
        unsafe { String::from_utf8_unchecked(dst) }
    }

    #[test]
    fn test_harness_empty() {
        let mut h = Harness::new();
        for i in 0..*K_TEST_ARG_NUM {
            let rnd = Random::new(301 + 1);
            h.init(&K_TEST_ARG_LIST[i as usize]);
            h.test(&rnd);
        }
    }

    #[test]
    fn test_harness_zero_restart_points_in_block() {
        let data = Vec::with_capacity(mem::size_of::<u32>());
        let mut contents = BlockContents::new();
        contents.data = Vec::from(data);
        contents.cacheable = false;
        contents.heap_allocated = false;
        let block = Block::new(Arc::new(contents));
        let mut iter = block.new_iter(Arc::new(BytewiseComparatorImpl::new()));
        iter.seek_to_first();
        assert!(!iter.valid());
        iter.seek_to_last();
        assert!(!iter.valid());
        iter.seek(b"foo");
        assert!(!iter.valid());
    }

    #[test]
    fn test_harness_simple_empty_key() {
        let mut h = Harness::new();
        for i in 0..*K_TEST_ARG_NUM {
            let rnd = Random::new(301 + 1);
            h.init(&K_TEST_ARG_LIST[i as usize]);
            h.add("", "v");
            h.test(&rnd);
        }
    }

    #[test]
    fn test_harness_simple_single() {
        let mut h = Harness::new();
        for i in 0..*K_TEST_ARG_NUM {
            let rnd = Random::new(301 + 2);
            h.init(&K_TEST_ARG_LIST[i as usize]);
            h.add("abc", "v");
            h.test(&rnd);
        }
    }

    #[test]
    fn test_harness_simple_multi() {
        let mut h = Harness::new();
        for i in 0..*K_TEST_ARG_NUM {
            let rnd = Random::new(301 + 3);
            h.init(&K_TEST_ARG_LIST[i as usize]);
            h.add("abc", "v");
            h.add("abcd", "v");
            h.add("ac", "v2");

            h.test(&rnd);
        }
    }

    #[test]
    fn test_harness_simple_special_key() {
        let mut h = Harness::new();
        for i in 0..*K_TEST_ARG_NUM {
            let rnd = Random::new(301 + 4);
            h.init(&K_TEST_ARG_LIST[i as usize]);
            h.add(unsafe { std::str::from_utf8_unchecked(&[255, 255]) }, "v3");
            h.test(&rnd);
        }
    }

    // if stuck, maybe the compact thread panics.
    // This test should not convert mem -> imm after DBTest 14-15
    #[test]
    fn test_harness_randomized() {
        let mut h = Harness::new();
        for i in 0..*K_TEST_ARG_NUM {
            h.init(&K_TEST_ARG_LIST[i as usize]);
            let rnd = Random::new(301 + 5);
            let mut num = 0;
            while num < 2000 {
                if num % 10 == 0 {
                    // println!("ERROR")
                }
                // 25
                for _ in 0..num {
                    // avoid borrow check error
                    let p1 = rnd.skewed(4);
                    let p = random_key(&rnd, p1);
                    let a1 = rnd.skewed(5);
                    let v = random_string(&rnd, a1);

                    h.add(p.as_str(), v.as_str());
                }
                h.test(&rnd);
                num += if num < 50 { 1 } else { 200 }
            }
        }
    }

    /// This test should trigger minor compaction(multiple compact mem table).
    /// level 0 files number should > 0.
    #[test]
    fn test_randomized_long_db() {
        let rnd = Random::new(301);
        let args = TestArgs {
            type_: TestType::DbTest,
            reverse_compare: false,
            restart_interval: 16,
        };
        let mut h = Harness::new();
        h.init(&args);
        for _i in 0..100000 {
            let r1 = rnd.skewed(4);
            let key = random_key(&rnd, r1);
            let r2 = rnd.skewed(5);
            let v = random_string(&rnd, r2);
            h.add(key.as_str(), v.as_str());
        }
        h.test(&rnd);

        let mut files = 0;
        for i in 0..LEVEL_NUMBER {
            let mut value = String::default();
            let name = format!("{}num-files-at-level{}", PROPERTY_PREFIX, i);
            assert!(h.db().get_property(name, &mut value));
            files += value.parse::<i32>().unwrap();
        }
        assert!(files > 0);

        let mut stats = String::default();
        assert!(h
            .db()
            .get_property(format!("{}stats", PROPERTY_PREFIX), &mut stats));

        let reg_str = r" {0,12}(\d+\.*\d*)";
        let reg = reg_str.repeat(6);
        let re = Regex::new(&reg).unwrap();
        //     2       9   0.480276 152.520418   0.000000    0.480276
        for cap in re.captures_iter(&stats) {
            // Level   Files   Size(MB)   Time(ms)   Read(MB)   Write(MB)
            let level = &cap[1];
            let files = &cap[2];
            let size = &cap[3];
            let time = &cap[4];
            let read_bytes = &cap[5];
            let write_bytes = &cap[6];
            assert!(level.parse::<u64>().unwrap() > 0);
            assert!(files.parse::<u64>().unwrap() > 0);
            assert!(size.parse::<f64>().unwrap() > 0f64);
            assert!(time.parse::<f64>().unwrap() > 0f64);
            assert!(read_bytes.parse::<f64>().unwrap() >= 0f64);
            assert!(write_bytes.parse::<f64>().unwrap() > 0f64);
        }

        if let Some(d) = &mut h.constructor {
            ignore!(d.db().close());
        }
    }

    #[test]
    fn test_mem_simple() {
        let cmp = Arc::new(InternalKeyComparatorImpl::new(Arc::new(
            BytewiseComparatorImpl::new(),
        )));
        let mem = Arc::new(Mutex::new(MemoryTable::new(cmp)));

        let mut batch = WriteBatch::new();
        batch.set_sequence(100);
        batch.put(b"k1", b"v1");
        batch.put(b"k2", b"v2");
        batch.put(b"k3", b"v3");
        batch.put(b"largekey", b"vlarge");
        assert!(WriteBatch::insert_batch(&batch, &mut *mem.lock().unwrap()).is_ok());

        let mut_mem = mem.lock().unwrap();
        let mut iter = Box::new(mut_mem.new_iter());
        iter.seek_to_first();
        while iter.valid() {
            iter.next();
        }
    }

    #[test]
    fn test_table_approximate_offset_of_plain() {
        let mut c = TableConstructor::new(Arc::new(BytewiseComparatorImpl::new()));
        c.add("k01", "hello");
        c.add("k02", "hello2");
        c.add("k03", "x".repeat(10000).as_str());
        c.add("k04", "x".repeat(200000).as_str());
        c.add("k05", "x".repeat(300000).as_str());
        c.add("k06", "hello3");
        c.add("k07", "x".repeat(100000).as_str());

        let mut opt = Options::default();
        opt.block_size = 1024;
        c.finish(opt);

        assert!(between(c.approximate_offset_of("abc"), 0, 0));
        assert!(between(c.approximate_offset_of("k01"), 0, 0));
        assert!(between(c.approximate_offset_of("k01a"), 0, 0));
        assert!(between(c.approximate_offset_of("k02"), 0, 0));
        assert!(between(c.approximate_offset_of("k03"), 0, 0));
        assert!(between(c.approximate_offset_of("k04"), 10000, 11000));
        assert!(between(c.approximate_offset_of("k04a"), 210000, 211000));
        assert!(between(c.approximate_offset_of("k05"), 210000, 211000));
        assert!(between(c.approximate_offset_of("k06"), 510000, 511000));
        assert!(between(c.approximate_offset_of("k07"), 510000, 511000));
        assert!(between(c.approximate_offset_of("xyz"), 610000, 612000));

        assert_eq!(format!("{:?}", c.table.as_ref().unwrap()), "0 62 [0, 3, 3, 107, 48, 51, 0, 178, 78, 0, 3, 5, 107, 48, 52, 183, 78, 208, 154, 12] 42 true false false 610112:8");
    }

    fn compressible_string(
        rnd: &impl RandomGenerator,
        compressed_fraction: f64,
        len: usize,
        dst: &mut Vec<u8>,
    ) -> String {
        let raw = (len as f64 * compressed_fraction) as u32;
        let raw_data = random_string(rnd, raw);
        dst.clear();
        while dst.len() < len {
            dst.extend_from_slice(raw_data.as_bytes());
        }
        dst.resize(len, 0);
        unsafe { String::from_utf8_unchecked(dst.clone()) }
    }

    #[test]
    fn test_table_approximate_offset_of_compressed() {
        let rnd = Random::new(301);
        let mut c = TableConstructor::new(Arc::new(BytewiseComparatorImpl::new()));
        let mut tmp = Default::default();
        c.add("k01", "hello");
        c.add(
            "k02",
            compressible_string(&rnd, 0.25, 10000, &mut tmp).as_str(),
        );

        c.add("k03", "hello3");
        c.add(
            "k04",
            compressible_string(&rnd, 0.25, 10000, &mut tmp).as_str(),
        );

        let mut opt = Options::default();
        opt.block_size = 1024;
        opt.compression = CompressionType::Snappy;
        c.finish(opt);

        const SLOP: u64 = 1000;
        let expected = 2500;
        let min_z = expected - SLOP;
        let max_z = expected + SLOP;
        assert!(between(c.approximate_offset_of("abc"), 0, SLOP));
        assert!(between(c.approximate_offset_of("k01"), 0, SLOP));
        assert!(between(c.approximate_offset_of("k02"), 0, SLOP));
        // Have now emitted a large compressible string, so adjust expected offset.
        assert!(between(c.approximate_offset_of("k03"), min_z, max_z));
        assert!(between(c.approximate_offset_of("k04"), min_z, max_z));
        // Have now emitted two large compressible strings, so adjust expected offset.
        assert!(between(
            c.approximate_offset_of("xyz"),
            2 * min_z,
            2 * max_z,
        ));
    }
}
