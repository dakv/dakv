#[cfg(test)]
mod test {
    use crate::utils::time::get_micro;
    use crate::{Database, Options, ReadOptions, WriteOptions, DB};

    // Simple test by using the real file not mock.
    const TEST_DB: &str = "/tmp/test_db_";

    #[test]
    fn test_put() {
        let db = DB::open(
            format!("{}{}test_put", TEST_DB, get_micro()),
            Options::default(),
        )
        .unwrap();
        db.put(b"1", b"abc", WriteOptions::default()).unwrap();
        assert_eq!(db.get(b"1", ReadOptions::new()).unwrap(), b"abc");
        db.put(
            b"3",
            b"This code is editable and runnable!",
            WriteOptions::default(),
        )
        .unwrap();
        assert_eq!(
            db.get(b"3", ReadOptions::new()).unwrap(),
            b"This code is editable and runnable!",
        );
    }

    #[test]
    fn test_delete() {
        let db = DB::open(
            format!("{}{}test_delete", TEST_DB, get_micro()),
            Options::default(),
        )
        .unwrap();
        db.put(b"2", b"c", WriteOptions::default()).unwrap();
        db.put(b"1", b"a", WriteOptions::default()).unwrap();
        db.put(b"3", b"b", WriteOptions::default()).unwrap();
        db.delete(b"4", WriteOptions::default()).unwrap(); // delete not exist key
        db.delete(b"1", WriteOptions::default()).unwrap();
        assert_eq!(db.get(b"3", ReadOptions::new()).unwrap(), b"b");
        assert_eq!(db.get(b"2", ReadOptions::new()).unwrap(), b"c");
        assert_eq!(
            format!("{:?}", db.get(b"1", ReadOptions::new()).err().unwrap()),
            "NotFound"
        );
        db.put(b"1", b"aa", WriteOptions::default()).unwrap();
        assert_eq!(db.get(b"1", ReadOptions::new()).unwrap(), b"aa");
    }
}
