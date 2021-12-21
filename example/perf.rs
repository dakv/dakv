use dakv::{Database, Options, WriteOptions, DB};
use pprof;
use pprof::protos::Message;
use std::fs::File;
use std::io::Write;

fn main() {
    let guard = pprof::ProfilerGuard::new(100).unwrap();

    let w = WriteOptions::default();
    let opt = Options::default();
    let db = DB::open("example_db".to_string(), opt).unwrap();
    let value = "x".repeat(1000);
    db.put("A".as_bytes(), "va".as_bytes(), w.clone()).unwrap();
    for i in 0..200 {
        if i % 100 == 0 {
            println!("progress {}", i);
        }
        let key = format!("B{:10}", i);
        db.put(key.as_bytes(), value.as_bytes(), w.clone()).unwrap();
    }

    match guard.report().build() {
        Ok(report) => {
            let mut file = File::create("profile.pb").unwrap();
            let profile = report.pprof().unwrap();

            let mut content = Vec::new();
            profile.encode(&mut content).unwrap();
            file.write_all(&content).unwrap();
        }
        Err(_) => {}
    };
}
