#[cfg(test)]
mod tests {
    use crate::table::filter_block::{FilterBlockReader, FilterBlockWriter};
    use crate::table::{BloomFilterPolicy, FilterPolicy};
    use crate::utils::coding::{decode_u32_le, encode_u32_le, put_fixed_32};
    use crate::utils::hash::hash;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;

    struct TestHashFilter;

    impl FilterPolicy for TestHashFilter {
        fn name(&self) -> &'static str {
            "ForTest"
        }

        fn key_may_exist(&self, key: &[u8], filter: &[u8]) -> bool {
            let h = hash(key, key.len(), 1);
            let mut i = 0;
            while i + 4 <= filter.len() {
                if h == decode_u32_le(&filter[i..]) {
                    return true;
                }
                i += 4;
            }
            false
        }

        fn create_filter(&self, keys: Vec<&[u8]>, dst: &mut Vec<u8>, n: usize) {
            for i in 0..n {
                let h = hash(keys[i], keys[i].len(), 1);
                put_fixed_32(dst, h);
            }
        }
    }

    #[test]
    fn test_empty_builder() {
        let policy = Arc::new(TestHashFilter);
        let mut builder = FilterBlockWriter::new(policy.clone());
        let b = builder.finish();
        assert_eq!(b.as_slice(), &[0, 0, 0, 0, 11]);

        let reader = FilterBlockReader::new(policy.clone(), b);
        assert!(reader.key_may_exist(0, b"foo"));
        assert!(reader.key_may_exist(100000, b"foo"));
    }

    #[test]
    fn test_simple_chunk() {
        let policy = Arc::new(TestHashFilter);
        let mut builder = FilterBlockWriter::new(policy.clone());
        builder.start_block(100);
        builder.add_key(b"foo");
        builder.add_key(b"bar");
        builder.add_key(b"box");
        builder.start_block(200);
        builder.add_key(b"box");
        builder.start_block(300);
        builder.add_key(b"hello");
    }

    #[test]
    fn test_multi_chunk() {
        let policy = Arc::new(TestHashFilter);
        let mut builder = FilterBlockWriter::new(policy.clone());

        builder.start_block(0);
        builder.add_key(b"foo");
        builder.start_block(2000);
        builder.add_key(b"bar");

        builder.start_block(3100);
        builder.add_key(b"box");

        builder.start_block(9000);
        builder.add_key(b"box");
        builder.add_key(b"hello");

        let b = builder.finish();
        let reader = FilterBlockReader::new(policy.clone(), b);

        assert!(reader.key_may_exist(0, b"foo"));
        assert!(reader.key_may_exist(2000, b"bar"));
        assert!(!reader.key_may_exist(0, b"box"));
        assert!(!reader.key_may_exist(0, b"hello"));

        assert!(reader.key_may_exist(3100, b"box"));
        assert!(!reader.key_may_exist(3100, b"foo"));
        assert!(!reader.key_may_exist(3100, b"bar"));
        assert!(!reader.key_may_exist(3100, b"hello"));

        assert!(!reader.key_may_exist(4100, b"box"));
        assert!(!reader.key_may_exist(4100, b"foo"));
        assert!(!reader.key_may_exist(4100, b"bar"));
        assert!(!reader.key_may_exist(4100, b"hello"));

        assert!(reader.key_may_exist(9000, b"box"));
        assert!(!reader.key_may_exist(9000, b"foo"));
        assert!(!reader.key_may_exist(9000, b"bar"));
        assert!(reader.key_may_exist(9000, b"hello"));
    }

    #[test]
    fn test_hash() {
        let data1 = &[0x62];
        let data2 = &[0xc3, 0x97];
        let data3 = &[0xe2, 0x99, 0xa5];
        let data4 = &[0xe1, 0x80, 0xb9, 0x32];
        let data5 = &[
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14,
            0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        assert_eq!(hash(&[0, 0, 0, 0], 0, 0xbc9f1d34), 0xbc9f1d34);
        assert_eq!(hash(data1, data1.len(), 0xbc9f1d34), 0xef1345c4);
        assert_eq!(hash(data2, data2.len(), 0xbc9f1d34), 0x5b663814);
        assert_eq!(hash(data3, data3.len(), 0xbc9f1d34), 0x323c078f);
        assert_eq!(hash(data4, data4.len(), 0xbc9f1d34), 0xed21633a);
        assert_eq!(hash(data5, data5.len(), 0x12345678), 0xf333dabb);
    }

    struct BloomTest {
        policy: Rc<RefCell<dyn FilterPolicy>>,
        keys: Vec<Vec<u8>>,
        filter: Vec<u8>,
    }

    impl BloomTest {
        pub fn reset(&mut self) {
            self.keys.clear();
            self.filter.clear();
        }

        pub fn matches(&mut self, k: &[u8]) -> bool {
            if !self.keys.is_empty() {
                self.build();
            }
            self.policy
                .borrow_mut()
                .key_may_exist(k, self.filter.as_slice())
        }

        pub fn build(&mut self) {
            let mut s = vec![];
            for i in 0..self.keys.len() {
                s.push(self.keys[i].as_slice());
            }
            self.filter.clear();
            self.policy
                .borrow_mut()
                .create_filter(s, &mut self.filter, self.keys.len());
            self.keys.clear();
        }

        pub fn add(&mut self, s: &[u8]) {
            self.keys.push(s.to_owned());
        }

        pub fn encode_key(i: u32, buf: &mut [u8]) {
            encode_u32_le(buf, i);
        }

        pub fn false_positive_rate(&mut self) -> f64 {
            let buf = &mut vec![0; 4];
            let mut r = 0f64;
            for i in 0..10000 {
                BloomTest::encode_key(i + 1000000000, buf.as_mut_slice());
                if self.matches(buf.as_slice()) {
                    r += 1.0;
                }
            }
            r / 10000.0
        }
    }

    #[test]
    fn test_empty_bloom() {
        let policy = Rc::new(RefCell::new(BloomFilterPolicy::new(10)));
        let mut bloom = BloomTest {
            policy,
            keys: vec![],
            filter: vec![],
        };
        assert_eq!(false, bloom.matches(b"foo"));
        assert_eq!(false, bloom.matches(b"bar"));
    }

    #[test]
    fn test_bloom() {
        let policy = Rc::new(RefCell::new(BloomFilterPolicy::new(10)));
        let mut bloom = BloomTest {
            policy,
            keys: vec![],
            filter: vec![],
        };
        bloom.add(b"foo");
        bloom.add(b"bar");
        assert_eq!(true, bloom.matches(b"foo"));
        assert_eq!(true, bloom.matches(b"bar"));
        assert_eq!(false, bloom.matches(b"1"));
        assert_eq!(false, bloom.matches(b"2"));
    }

    fn next_length(len: u64) -> u64 {
        let mut len = len;
        if len < 10 {
            len += 1;
        } else if len < 100 {
            len += 10;
        } else if len < 1000 {
            len += 100;
        } else {
            len += 1000;
        }
        len
    }

    #[test]
    fn test_bloom2() {
        let policy = Rc::new(RefCell::new(BloomFilterPolicy::new(10)));
        let mut bloom = BloomTest {
            policy,
            keys: vec![],
            filter: vec![],
        };
        let mut mediocre_filters = 0;
        let mut good_filters = 0;

        let mut i = 1u64;
        let buf = &mut vec![0; 4];
        while i <= 10000 {
            bloom.reset();
            for j in 0..i {
                BloomTest::encode_key(j as u32, buf.as_mut_slice());
                bloom.add(buf.as_slice());
            }
            bloom.build();

            assert!(bloom.filter.len() < (i * 10 / 8) as usize + 40);
            for j in 0..i {
                BloomTest::encode_key(j as u32, buf.as_mut_slice());
                assert!(bloom.matches(buf.as_slice()));
            }

            let rate = bloom.false_positive_rate();
            assert!(rate < 0.02);
            if rate > 0.0125 {
                mediocre_filters += 1;
            } else {
                good_filters += 1;
            }
            i = next_length(i);
        }

        assert!(mediocre_filters < good_filters / 5);
    }
}
