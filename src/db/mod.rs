use std::sync::{Arc, RwLock};

pub use crate::db::config::*;
use crate::db::database::Range;
pub use crate::db::database::DB;
pub use crate::db::db_iter::DBIter;
pub use crate::db::errors::{DError, DResult};
pub use crate::db::file::{table_file_name, FileType};
pub use crate::db::file_meta::FileMetaData;
pub use crate::db::format::{SequenceNumber, ValueType};
pub use crate::db::iterator::*;
pub use crate::db::key::{
    append_internal_key, extract_user_key, pack_sequence_and_type, parse_internal_key,
    InternalKey, ParsedInternalKey,
};
pub use crate::db::memory_table::MemoryTable;
pub use crate::db::options::{CompressionType, Options, ReadOptions, WriteOptions};
use crate::db::snapshot::SnapNode;
pub use crate::db::version::Saver;
pub use crate::db::write_batch::WriteBatch;
use crate::utils::iter::Iter;

#[macro_use]
mod errors;
mod builder;
mod compaction;
mod config;
mod database;
#[cfg(test)]
mod database_simple_test;
#[cfg(test)]
mod database_test;
mod db_iter;
mod file;
mod file_meta;
mod format;
mod iterator;
#[cfg(test)]
mod iterator_test;
mod key;
mod log_format;
mod log_reader;
#[cfg(test)]
mod log_reader_test;
mod log_writer;
mod memory_table;
mod options;
mod snapshot;
mod version;
mod version_edit;
mod version_manager;
mod version_manager_builder;
#[cfg(test)]
mod version_manager_test;
mod write_batch;

pub trait Database {
    type Iterator: Iter;

    /// Insert new key value pair into database.
    fn put(&self, key: &[u8], value: &[u8], opt: WriteOptions) -> DResult<()>;

    /// Remove the key value entry, returns ok if success.
    fn delete(&self, key: &[u8], opt: WriteOptions) -> DResult<()>;

    /// Return the value if database contains the key, else return NotFound Error
    fn get(&self, key: &[u8], read_opt: ReadOptions) -> DResult<Vec<u8>>;

    /// This MultiGet is a batched version, which may be faster than calling Get
    /// multiple times.
    /// TODO: fn multi_get(&self, key: &[u8], read_opt: ReadOptions) -> DResult<Vec<u8>>;

    /// Write multiple entries into database. By default, `get` and `delete` use this
    /// method to insert key value entry.
    fn write(&self, batch: Option<WriteBatch>, opt: WriteOptions) -> DResult<()>;

    /// Returns iterator which implements the `Iter` trait.
    /// See more implementation details in `db/db_iter.rs`
    fn new_iter(&self, opt: ReadOptions) -> Self::Iterator;

    /// Send shutdown signal to the channel and remove the db lock.
    fn close(&self) -> DResult<()>;

    /// Support exporting the metrics of the database, and fill the result into `value`,
    /// if the property parameter is invalid, returns false.
    ///
    /// Valid property names include:
    /// - dakv.num-files-at-level[N]
    /// - dakv.stats
    /// - dakv.approximate-memory-usage
    fn get_property(&self, property: String, value: &mut String) -> bool;

    /// Returns a new snapshot to observe current db state, snapshot can be
    /// used by `ReadOptions`.
    /// The caller must call `release_snapshot` when the
    /// snapshot is no longer needed.
    fn get_snapshot(&self) -> Arc<RwLock<SnapNode>>;

    /// Release a snapshot, after releasing, this snapshot can not be used anymore.
    fn release_snapshot(&self, snap: &mut SnapNode);

    /// Return the approximate sizes vector, this method uses the return value
    /// from `VersionManager.approximate_offset_of`, so the results may not include
    /// the sizes of recently written data.
    fn get_approximate_sizes(&self, ranges: Vec<Range>) -> Vec<u64>;
}
