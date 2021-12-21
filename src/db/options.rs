use crate::db::snapshot::SnapNode;
use crate::env::Env;
use crate::env::PosixEnv;
use crate::table::{Block, FilterPolicy};
use crate::utils::cmp::{BytewiseComparatorImpl, Comparator};
use crate::utils::lru_cache::Cache;
use slog::Logger;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Clone, Copy)]
#[non_exhaustive]
pub enum CompressionType {
    NoCompress,
    Snappy,
    Zstd,
}

impl From<CompressionType> for u8 {
    fn from(c: CompressionType) -> Self {
        match c {
            CompressionType::NoCompress => 0,
            CompressionType::Snappy => 1,
            CompressionType::Zstd => 2,
        }
    }
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::NoCompress
    }
}

impl From<u8> for CompressionType {
    fn from(a: u8) -> Self {
        match a {
            0 => CompressionType::NoCompress,
            1 => CompressionType::Snappy,
            2 => CompressionType::Zstd,
            _ => {
                panic!("Unknown compress type")
            }
        }
    }
}

// todo remove Clone and use Arc<Options>
#[derive(Clone)]
pub struct Options {
    /// Create database if specific database does not exist.
    /// If `create_if_missing` is false and db does not exist, return error.
    /// Default: true.
    /// See more details from `Inner.recover`.
    pub create_if_missing: bool,
    /// Return error if db already exists.
    /// Default: false.
    /// See more details from `Inner.recover`.
    pub error_if_exists: bool,
    /// max_open_files-NUM_NON_TABLE_CACHE_FILES is the maximum number of table cache files.
    /// Default: 1000
    pub max_open_files: u64,
    /// Default: 2 << 20  -> 2MB
    pub max_file_size: u64,
    /// Within this size range, the data is saved in mem table, when the approximate memory usage
    /// of mem table > `write_buffer_size`, minor compaction is triggered. Write data into level 0.
    /// Default: 4 << 20  -> 4MB
    pub write_buffer_size: usize,
    /// Compression algorithm.
    pub compression: CompressionType,
    /// Because the composition of `internal_key` is very regular, we can save space and
    /// improve storage and writing efficiency by prefix compression of keys.
    /// Restart prefix compression every `block_restart_interval` elements.
    /// Default: 16
    /// See more details from `table/block_builder.rs`
    pub block_restart_interval: i64,
    /// If non-null, use the specified filter policy to reduce disk reads.
    /// Many applications will benefit from passing the result of
    /// `BloomFilterPolicy` here.
    /// Default: None
    pub filter_policy: Option<Arc<dyn FilterPolicy + Send + Sync>>,
    /// Block size for `BlockBuilder`.
    /// Default: 1 << 12 -> 4k
    pub block_size: usize,
    /// Default: `BytewiseComparatorImpl`
    /// See more details from `utils/cmp.rs`
    pub comparator: Arc<dyn Comparator + Send + Sync>,
    /// Block cache.
    pub block_cache: Option<Arc<Mutex<dyn Cache<Vec<u8>, Block> + Send + Sync>>>,
    pub reuse_logs: bool,
    /// If true, the implementation will do aggressive checking of the
    /// data it is processing and will stop early if it detects any
    /// errors.  This may have unforeseen ramifications: for example, a
    /// corruption of one DB entry may cause a large number of entries to
    /// become unreadable or for the entire DB to become unopenable.
    /// Default: false
    pub paranoid_checks: bool,
    pub env: Arc<dyn Env + Send + Sync>,
    pub info_log: Option<Logger>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            max_open_files: 1000,
            max_file_size: 2 << 20,
            write_buffer_size: 4 << 20,
            create_if_missing: true,
            compression: CompressionType::NoCompress,
            block_restart_interval: 16,
            filter_policy: None,
            block_size: 1 << 12,
            comparator: Arc::new(BytewiseComparatorImpl::new()),
            block_cache: None,
            reuse_logs: false,
            paranoid_checks: false,
            error_if_exists: false,
            env: Arc::new(PosixEnv::new()),
            info_log: None,
        }
    }
}

#[derive(Clone)]
pub struct ReadOptions {
    /// All data read from blocks will be verified against corresponding checksums.
    /// Default: false
    pub verify_checksums: bool,
    /// Should the data read for this iteration be cached in memory?
    /// Callers may wish to set this field to false for bulk scans.
    /// Default: true
    pub fill_cache: bool,
    /// If `snapshot` is non-null, read as of the supplied snapshot.
    /// make sure the snapshot has not been released.
    /// Default: None
    pub snapshot: Option<Arc<RwLock<SnapNode>>>,
}

impl ReadOptions {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        ReadOptions {
            verify_checksums: false,
            fill_cache: true,
            snapshot: None,
        }
    }

    pub fn new_with_verify(verify: bool, fill_cache: bool) -> Self {
        let mut s = Self::new();
        s.set_verify(verify);
        s.set_fill_cache(fill_cache);
        s
    }

    pub fn set_verify(&mut self, v: bool) {
        self.verify_checksums = v;
    }

    pub fn set_fill_cache(&mut self, v: bool) {
        self.fill_cache = v;
    }
}

// todo remove clone
#[derive(Clone, Default)]
pub struct WriteOptions {
    /// If true, the write will be flushed from the operating system
    /// buffer cache before the write is considered complete.
    /// If this flag is true, writes will be slower.
    ///
    /// If this flag is false, and the machine crashes, some recent
    /// writes may be lost.  Note that if it is just the process that
    /// crashes (i.e., the machine does not reboot), no writes will be
    /// lost even if sync==false.
    ///
    /// In other words, a DB write with sync==false has similar
    /// crash semantics as the "write()" system call.  A DB write
    /// with sync==true has similar crash semantics to a "write()"
    /// system call followed by "fsync()".
    ///
    /// Default: false
    pub sync: bool,
}
