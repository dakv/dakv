[![test](https://github.com/dakv/dakv/actions/workflows/test.yml/badge.svg)](https://github.com/dakv/dakv/actions/workflows/test.yml)
[![clippy](https://github.com/dakv/dakv/actions/workflows/clippy.yml/badge.svg)](https://github.com/dakv/dakv/actions/workflows/clippy.yml)
[![Build Status](https://dev.azure.com/dakv/dakv/_apis/build/status/dakv.dakv?branchName=master)](https://dev.azure.com/dakv/dakv/_build/latest?definitionId=2&branchName=master)
[![codecov](https://codecov.io/gh/dakv/dakv/branch/master/graph/badge.svg?token=T8NJJWNYM4)](https://codecov.io/gh/dakv/dakv)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/f6e5fba5959c4f4fa2624b502d325241)](https://www.codacy.com/gh/dakv/dakv/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=dakv/dakv&amp;utm_campaign=Badge_Grade)


### Example
```Rust
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
```

### Build

```
cd dakv
git submodule update --init --recursive
cargo build
```

### Features

- [Fast CRC](https://github.com/srijs/rust-crc32fast) (support see and PCLMULQDQ instructions)


## Future
- [ ] Support [io_uring](https://github.com/tokio-rs/io-uring)
- [ ] Implement WiscKey to improve performance.
- [ ] Use DB session ID as the cache key prefix to ensure uniqueness and repeatability
- [ ] Fuzz testing with [rust-fuzz/afl](https://github.com/rust-fuzz/afl.rs)
- [ ] Support multi get.
- [ ] YCSB Benchmark

---
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fdakv%2Fdakv.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fdakv%2Fdakv?ref=badge_large)
