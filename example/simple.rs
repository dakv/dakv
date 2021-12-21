use dakv::{Database, Options, ReadOptions, TestEnv, WriteOptions, DB};
use std::sync::Arc;

fn main() {
    let db = {
        let mut opt = Options::default();
        opt.env = Arc::new(TestEnv::default());
        DB::open("example_db".to_string(), opt).unwrap()
    };

    let write_opt = WriteOptions::default();
    db.put(
        b"Red",
        b"Let me tell you something my friend. Hope is a dangerous thing. Hope can drive a man insane.",
        write_opt.clone(),
    ).unwrap();
    {
        let opt = ReadOptions::new();
        assert!(db.get(b"Red", opt.clone()).is_ok());
        db.delete(b"Red", write_opt.clone()).unwrap();
        assert_eq!(db.get(b"Red", opt).unwrap_err().to_string(), "NotFound");
    }
    db.put(
        b"Andy Dufresne",
        b"Remember Red, hope is a good thing, maybe the best of things, and no good thing ever dies.",
        write_opt.clone(),
    ).unwrap();
}
