#[cfg(test)]
mod test {
    use crate::db::database::Inner;
    use crate::db::file_meta::FileMetaData;
    use crate::db::format::{SequenceNumber, ValueType};
    use crate::db::key::InternalKey;
    use crate::db::version::has_overlapped_files_in_range;
    use crate::db::version_edit::VersionEdit;
    use crate::db::version_manager::{find_file, VersionManager};
    use crate::db::{MemoryTable, Options, NUM_NON_TABLE_CACHE_FILES};
    use crate::table::TableCache;
    use crate::utils::cmp::{BytewiseComparatorImpl, InternalKeyComparatorImpl};
    use crate::utils::random::{Random, RandomGenerator};
    use crate::TestEnv;
    use std::sync::Arc;

    struct VersionManagerTest {
        disjoint_sorted_files: bool,
        files: Vec<Arc<FileMetaData>>,
    }

    impl VersionManagerTest {
        pub fn new() -> Self {
            Self {
                disjoint_sorted_files: true,
                files: vec![],
            }
        }

        fn find(&self, key: &[u8]) -> usize {
            let target = InternalKey::new(key, 100, ValueType::TypeValue);
            let bcmp = BytewiseComparatorImpl::new();
            let cmp = InternalKeyComparatorImpl::new(Arc::new(bcmp));
            find_file(Arc::new(cmp), &self.files, target.encode())
        }

        // str make test easier..
        fn add(
            &mut self,
            smallest: &str,
            largest: &str,
            smallest_seq: SequenceNumber,
            largest_seq: SequenceNumber,
        ) {
            let mut f = FileMetaData::new();
            f.set_file_number(self.files.len() as u64 + 1);
            f.set_smallest(InternalKey::new(
                smallest.as_bytes(),
                smallest_seq,
                ValueType::TypeValue,
            ));
            f.set_largest(InternalKey::new(
                largest.as_bytes(),
                largest_seq,
                ValueType::TypeValue,
            ));
            self.files.push(Arc::new(f));
        }

        // for test only.
        fn overlaps(&mut self, smallest: Option<&str>, largest: Option<&str>) -> bool {
            let bcmp = BytewiseComparatorImpl::new();
            let cmp = InternalKeyComparatorImpl::new(Arc::new(bcmp));
            let empty = "";
            let s = if smallest.is_some() {
                smallest
            } else {
                Some(empty)
            };
            let l = if largest.is_some() {
                largest
            } else {
                Some(empty)
            };
            dbg!(has_overlapped_files_in_range(
                Arc::new(cmp),
                self.disjoint_sorted_files,
                &self.files,
                if smallest.is_some() {
                    Some(s.unwrap().as_bytes())
                } else {
                    None
                },
                if largest.is_some() {
                    Some(l.unwrap().as_bytes())
                } else {
                    None
                },
            ))
        }
    }

    #[test]
    fn test_empty() {
        let mut s = VersionManagerTest::new();
        assert_eq!(0, s.find(b"foo"));
        assert!(!s.overlaps(Some("a"), Some("z")));
        assert!(!s.overlaps(Some("a"), None));
        assert!(!s.overlaps(None, Some("z")));
        assert!(!s.overlaps(None, None));
    }

    #[test]
    fn test_single() {
        let mut s = VersionManagerTest::new();
        s.add("p", "q", 100, 100);
        assert_eq!(0, s.find(b"a"));
        assert_eq!(0, s.find(b"p"));
        assert_eq!(0, s.find(b"p1"));
        assert_eq!(0, s.find(b"q"));
        assert_eq!(1, s.find(b"q1"));
        assert_eq!(1, s.find(b"z"));

        assert!(!s.overlaps(Some("a"), Some("b")));
        assert!(!s.overlaps(Some("z1"), Some("z2")));
        assert!(s.overlaps(Some("a"), Some("p")));
        assert!(s.overlaps(Some("a"), Some("q")));
        assert!(s.overlaps(Some("a"), Some("z")));
        assert!(s.overlaps(Some("p"), Some("p1")));
        assert!(s.overlaps(Some("p"), Some("q")));
        assert!(s.overlaps(Some("p"), Some("z")));
        assert!(s.overlaps(Some("p1"), Some("p2")));
        assert!(s.overlaps(Some("p1"), Some("z")));
        assert!(s.overlaps(Some("q"), Some("q")));
        assert!(s.overlaps(Some("q"), Some("q1")));

        assert!(!s.overlaps(None, Some("j")));
        assert!(!s.overlaps(Some("r"), None));

        assert!(s.overlaps(None, Some("p")));
        assert!(s.overlaps(None, Some("p1")));

        assert!(s.overlaps(Some("q"), None));
        assert!(s.overlaps(None, None));
    }

    #[test]
    fn test_multiple() {
        let mut s = VersionManagerTest::new();
        s.add("150", "200", 100, 100);
        s.add("200", "250", 100, 100);
        s.add("300", "350", 100, 100);
        s.add("400", "450", 100, 100);
        assert_eq!(0, s.find(b"100"));
        assert_eq!(0, s.find(b"150"));
        assert_eq!(0, s.find(b"151"));
        assert_eq!(0, s.find(b"199"));
        assert_eq!(0, s.find(b"200"));
        assert_eq!(1, s.find(b"201"));
        assert_eq!(1, s.find(b"249"));
        assert_eq!(1, s.find(b"250"));
        assert_eq!(2, s.find(b"251"));
        assert_eq!(2, s.find(b"299"));
        assert_eq!(2, s.find(b"300"));
        assert_eq!(2, s.find(b"349"));
        assert_eq!(2, s.find(b"350"));
        assert_eq!(3, s.find(b"351"));
        assert_eq!(3, s.find(b"400"));
        assert_eq!(3, s.find(b"450"));
        assert_eq!(4, s.find(b"451"));

        assert!(!s.overlaps(Some("100"), Some("149")));
        assert!(!s.overlaps(Some("251"), Some("299")));
        assert!(!s.overlaps(Some("451"), Some("500")));
        assert!(!s.overlaps(Some("351"), Some("399")));

        assert!(s.overlaps(Some("100"), Some("150")));
        assert!(s.overlaps(Some("100"), Some("200")));
        assert!(s.overlaps(Some("100"), Some("300")));
        assert!(s.overlaps(Some("100"), Some("400")));
        assert!(s.overlaps(Some("100"), Some("500")));
        assert!(s.overlaps(Some("375"), Some("400")));
        assert!(s.overlaps(Some("450"), Some("450")));
        assert!(s.overlaps(Some("450"), Some("500")));
    }

    #[test]
    fn test_multiple_null_boundaries() {
        let mut s = VersionManagerTest::new();
        s.add("150", "200", 100, 100);
        s.add("200", "250", 100, 100);
        s.add("300", "350", 100, 100);
        s.add("400", "450", 100, 100);

        assert!(!s.overlaps(None, Some("149")));
        assert!(!s.overlaps(Some("451"), None));

        assert!(s.overlaps(None, None));
        assert!(s.overlaps(None, Some("150")));
        assert!(s.overlaps(None, Some("199")));
        assert!(s.overlaps(None, Some("200")));
        assert!(s.overlaps(None, Some("201")));
        assert!(s.overlaps(None, Some("400")));
        assert!(s.overlaps(None, Some("800")));
        assert!(s.overlaps(Some("100"), None));
        assert!(s.overlaps(Some("200"), None));
        assert!(s.overlaps(Some("449"), None));
        assert!(s.overlaps(Some("450"), None));
    }

    #[test]
    fn test_overlap_sequence_checks() {
        let mut s = VersionManagerTest::new();
        s.add("200", "200", 5000, 3000);

        assert!(!s.overlaps(Some("199"), Some("199")));
        assert!(!s.overlaps(Some("201"), Some("300")));
        assert!(s.overlaps(Some("200"), Some("200")));
        assert!(s.overlaps(Some("190"), Some("200")));
        assert!(s.overlaps(Some("200"), Some("210")));
    }

    #[test]
    fn test_overlapping_files() {
        let mut s = VersionManagerTest::new();
        s.add("150", "600", 100, 100);
        s.add("400", "500", 100, 100);
        s.disjoint_sorted_files = false;

        assert!(!s.overlaps(Some("100"), Some("149")));
        assert!(!s.overlaps(Some("601"), Some("700")));

        assert!(s.overlaps(Some("100"), Some("150")));
        assert!(s.overlaps(Some("100"), Some("200")));
        assert!(s.overlaps(Some("100"), Some("300")));
        assert!(s.overlaps(Some("100"), Some("400")));
        assert!(s.overlaps(Some("100"), Some("500")));

        assert!(s.overlaps(Some("375"), Some("400")));
        assert!(s.overlaps(Some("450"), Some("450")));
        assert!(s.overlaps(Some("450"), Some("500")));
        assert!(s.overlaps(Some("450"), Some("700")));
        assert!(s.overlaps(Some("600"), Some("700")));
    }

    fn random_str(rnd: &Random, len: u32) -> Vec<u8> {
        let mut dst = vec![0; len as usize];
        for i in 0..len {
            dst[i as usize] = (' ' as u32 + rnd.uniform(95)) as u8;
        }
        dst
    }

    #[test]
    fn test_log_and_apply() {
        // options for test
        let mut opt = Options::default();
        opt.env = Arc::new(TestEnv::default());

        // cache
        let cache = Arc::new(TableCache::new(
            String::from("da"),
            opt.clone(),
            opt.max_open_files - NUM_NON_TABLE_CACHE_FILES,
        ));
        let internal_comparator = Arc::new(InternalKeyComparatorImpl::new(opt.comparator.clone()));
        // version set
        let mut version_manager = VersionManager::new(
            cache,
            opt.clone(),
            internal_comparator.clone(),
            String::from(""),
        );
        version_manager.set_manifest_num(2);
        // insert into mem table.
        let mut mem = MemoryTable::new(internal_comparator.clone());
        let r = Random::new(301);
        for i in 0..100 {
            mem.set(
                i,
                ValueType::TypeValue,
                random_str(&r, 1).as_slice(),
                random_str(&r, 3).as_slice(),
            );
        }
        // write level 0
        let inner = Inner::new("test".to_string(), opt);
        let mut edit = VersionEdit::new();
        let s = inner.write_level0_table(
            mem.new_iter(),
            &mut edit,
            Some(version_manager.current_version()),
        );

        // test log and apply
        if s.is_ok() {
            edit.set_log_number(1);
            ignore!(version_manager.log_and_append_version(&mut edit));
        }
    }
}
