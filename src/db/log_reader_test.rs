#[cfg(test)]
mod test {
    use crate::db::log_reader::Reader;
    use crate::db::log_writer::{Writer, BLOCK_SIZE, HEADER_SIZE};
    use crate::db::DResult;
    use crate::env::{SequentialFile, WritableFile};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;

    struct MockStorage {
        content: Rc<RefCell<Vec<u8>>>,
    }

    impl SequentialFile for MockStorage {
        fn read(&self, n: usize, result: &mut [u8]) -> DResult<usize> {
            let data_len = self.content.borrow().len();
            if n > data_len {
                result[..data_len].copy_from_slice(self.content.borrow().as_slice());
                self.content.borrow_mut().drain(..data_len);
                return Ok(data_len);
            }
            result.copy_from_slice(&self.content.borrow().as_slice()[..n]);
            self.content.borrow_mut().drain(..n);
            Ok(n)
        }

        fn skip(&self, _: u64) -> DResult<()> {
            unimplemented!()
        }
    }

    impl WritableFile for MockStorage {
        fn append(&self, buf: &[u8]) -> DResult<()> {
            self.content.borrow_mut().extend_from_slice(buf);
            Ok(())
        }

        fn flush(&self) -> DResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_write_and_read() {
        let store = Rc::new(RefCell::new(vec![]));
        let mock_write = MockStorage {
            content: store.clone(),
        };
        let mock_read = MockStorage {
            content: store.clone(),
        };
        let mut w = Writer::new(Arc::new(mock_write));
        let mut r = Reader::new(Arc::new(mock_read), true);

        w.write_record(b"foo").unwrap();
        w.write_record(b"bar").unwrap();
        w.write_record(b"").unwrap();

        let data = &mut vec![];
        r.read_record(data);
        assert_eq!(data.as_slice(), b"foo");
        r.read_record(data);
        assert_eq!(data.as_slice(), b"bar");
        r.read_record(data);
        assert_eq!(data.as_slice(), b"");
    }

    #[test]
    fn test_blocks() {
        let store = Rc::new(RefCell::new(vec![]));
        let mock_write = MockStorage {
            content: store.clone(),
        };
        let mock_read = MockStorage {
            content: store.clone(),
        };
        let mut w = Writer::new(Arc::new(mock_write));
        let mut r = Reader::new(Arc::new(mock_read), true);

        for i in 0..100000 {
            w.write_record(format!("{}", i).as_bytes()).unwrap();
        }
        let mut data = vec![];
        for i in 0..100000 {
            r.read_record(&mut data);
            let s: String = i.to_string();
            assert_eq!(&s.as_bytes()[..], data.as_slice());
        }
    }

    fn big_string(repeat: &str, n: usize) -> String {
        let mut s = String::default();
        while s.len() < n {
            s.push_str(repeat);
        }
        return s;
    }

    #[test]
    fn test_fragmentation() {
        let store = Rc::new(RefCell::new(vec![]));
        let mock_write = MockStorage {
            content: store.clone(),
        };
        let mock_read = MockStorage {
            content: store.clone(),
        };
        let mut w = Writer::new(Arc::new(mock_write));
        let mut r = Reader::new(Arc::new(mock_read), true);

        let medium = &big_string("medium", 50000);
        let large = &big_string("large", 100000);
        w.write_record(b"small").unwrap();

        w.write_record(medium.as_bytes()).unwrap();
        w.write_record(large.as_bytes()).unwrap();
        let mut data = vec![];
        r.read_record(&mut data);
        assert_eq!(data.as_slice(), b"small");
        r.read_record(&mut data);
        assert_eq!(data.as_slice(), medium.as_bytes());
        r.read_record(&mut data);
        assert_eq!(data.as_slice(), large.as_bytes());
    }

    #[test]
    fn test_marginal_trailer() {
        let store = Rc::new(RefCell::new(vec![]));
        let mock_write = MockStorage {
            content: store.clone(),
        };
        let mock_read = MockStorage {
            content: store.clone(),
        };
        let mut w = Writer::new(Arc::new(mock_write));
        let mut r = Reader::new(Arc::new(mock_read), true);

        let n = BLOCK_SIZE - 2 * HEADER_SIZE;
        let large = &big_string("foo", n);
        w.write_record(large.as_bytes()).unwrap();
        w.write_record(b"").unwrap();
        w.write_record(b"bar").unwrap();
        let mut data = vec![];
        r.read_record(&mut data);
        assert_eq!(data.as_slice(), large.as_bytes());
        r.read_record(&mut data);
        assert_eq!(data.as_slice(), b"");
        r.read_record(&mut data);
        assert_eq!(data.as_slice(), b"bar");
        assert_eq!(false, r.read_record(&mut data));
    }

    #[test]
    fn test_short_trailer() {
        let store = Rc::new(RefCell::new(vec![]));
        let mock_write = MockStorage {
            content: store.clone(),
        };
        let mock_read = MockStorage {
            content: store.clone(),
        };
        let mut w = Writer::new(Arc::new(mock_write));
        let mut r = Reader::new(Arc::new(mock_read), true);

        let n = BLOCK_SIZE - 2 * HEADER_SIZE + 4;
        let b = &big_string("foo", n);
        w.write_record(b.as_bytes()).unwrap();
        w.write_record(b"").unwrap();
        w.write_record(b"bar").unwrap();
        let mut data = vec![];
        r.read_record(&mut data);
        assert_eq!(data.as_slice(), b.as_bytes());
        r.read_record(&mut data);
        assert_eq!(data.as_slice(), b"");
        r.read_record(&mut data);
        assert_eq!(data.as_slice(), b"bar");
    }
}
