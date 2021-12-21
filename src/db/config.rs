use crate::db::format::SequenceNumber;

pub const LEVEL_NUMBER: usize = 7;

/// Level-0 compaction is started when we hit this many files.
pub const L0_COMPACTION_TRIGGER: i32 = 4;

/// Soft limit on number of level-0 files.  We slow down writes at this point.
pub const L0_SLOWDOWN_WRITES_TRIGGER: usize = 8;

/// Maximum number of level-0 files.  We stop writes at this point.
pub const L0_STOP_WRITES_TRIGGER: usize = 32;

/// Maximum level to which a new compacted memory table is pushed if it
/// does not create overlap.  We try to push to level 2 to avoid the
/// relatively expensive level 0=>1 compactions and to avoid some
/// expensive manifest file operations.  We do not push all the way to
/// the largest level since that can generate a lot of wasted disk
/// space if the same key space is being repeatedly overwritten.
pub const MAX_MEM_COMPACT_LEVEL: usize = 2;

/// Approximate gap in bytes between samples of data read during iteration.
pub const READ_BYTES_PERIOD: u32 = 1048576;

/// We leave eight bits empty at the bottom so a type and sequence#
/// can be packed together into 64-bits.
pub const MAX_SEQUENCE_NUMBER: SequenceNumber = (0x1u64 << 56) - 1;

/// 1 byte + 32 bit CRC
pub const BLOCK_TRAILER_SIZE: usize = 1 + 4;

pub const NUM_NON_TABLE_CACHE_FILES: u64 = 10;
