use crate::db::errors::DResult;
use crate::db::log_format::RecordType;
use crate::env::WritableFile;
use crate::utils::coding::encode_u32_le;
use crate::utils::crc32c::extend;
use crate::utils::crc32c::{mask, value};
use std::cmp::min;
use std::sync::Arc;

fn init_type_crc(type_crc: &mut [u32]) {
    assert!(type_crc.len() >= 4);
    for i in 0..RecordType::Eof as u8 {
        type_crc[i as usize] = value(&[i]);
    }
}

/// The log file contents are a sequence of 32KB blocks.  The only
/// exception is that the tail of the file may contain a partial block.
/// A record never starts within the last six bytes of a block (since it
/// won't fit).  Any leftover bytes here form the trailer, which must
/// consist entirely of zero bytes and must be skipped by readers.
pub const BLOCK_SIZE: usize = 1 << 15; // 32kb

pub const HEADER_SIZE: usize = 4 + 2 + 1; // crc(4) + length(2) + type(1) Bytes

// WAL log file, manifest file persisted by Writer
pub struct Writer {
    // written data length
    written_len: usize,
    storage: Arc<dyn WritableFile>,
    type_crc: [u32; RecordType::Eof as usize],
}

unsafe impl Send for Writer {}
unsafe impl Sync for Writer {}

impl Writer {
    pub fn new(w: Arc<dyn WritableFile>) -> Self {
        let mut type_crc = [0; RecordType::Eof as usize];
        init_type_crc(&mut type_crc);
        Writer {
            written_len: 0,
            storage: w,
            type_crc,
        }
    }

    pub fn new_with_offset(w: Arc<dyn WritableFile>, dest_len: usize) -> Self {
        let mut type_crc = [0; RecordType::Eof as usize];
        init_type_crc(&mut type_crc);
        Writer {
            written_len: dest_len % BLOCK_SIZE,
            storage: w,
            type_crc,
        }
    }

    /// If the slice is empty, still need to iterate once to emit a single zero-length
    /// record.
    pub fn write_record(&mut self, slice: &[u8]) -> DResult<()> {
        // The amount of space left in the current block
        let mut left_size = slice.len() as isize;
        let mut begin = true;
        let mut start = 0;
        //'do_while:
        while {
            let leftover = BLOCK_SIZE as isize - self.written_len as isize;
            assert!(leftover >= 0);
            if leftover < HEADER_SIZE as isize {
                if leftover > 0 {
                    // Fill the trailer with \x00
                    self.storage.append(&[0; 6][..leftover as usize])?;
                }
                self.written_len = 0;
            }
            // available space = 32 * 1024 - block_offset - 7
            let available = BLOCK_SIZE as isize - self.written_len as isize - HEADER_SIZE as isize;
            assert!(available >= 0);
            let fragment_len = min(left_size, available);
            let end = left_size == fragment_len;
            let record_type = if begin && end {
                RecordType::FullType
            } else if begin {
                RecordType::FirstType
            } else if end {
                RecordType::LastType
            } else {
                RecordType::MiddleType
            };

            self.store_record(
                record_type,
                &slice[start as usize..(start + fragment_len) as usize],
            )?;
            start += fragment_len;
            left_size -= fragment_len;
            begin = false;
            left_size > 0
        } {}
        Ok(())
    }

    pub fn sync(&self) -> DResult<()> {
        self.storage.sync()
    }

    /// Log format:
    /// ```text
    /// ┌◄────────────────── Header ────────────────►┐◄────Data─────►┐
    /// │                                            │               │
    /// ├───────────┬──────────────┬─────────────────┼───────────────┤
    /// │  CRC (4B) │  length (2B) │ record_type(1B) │ data (n Bytes)│
    /// └───────────┴──────────────┴─────────────────┴───────────────┘
    /// ```
    fn store_record(&mut self, t: RecordType, data: &[u8]) -> DResult<()> {
        let len = data.len();
        assert!(len <= 0xffff);
        assert!(self.written_len + HEADER_SIZE + len <= BLOCK_SIZE);
        // header
        // |  crc 4B  | length 2B | type 1B |
        let mut header_buf = [0, 0, 0, 0, (len & 0xff) as u8, (len >> 8) as u8, t as u8];
        let crc = extend(self.type_crc[t as usize], data);
        encode_u32_le(&mut header_buf, mask(crc));
        // write header
        self.storage.append(&header_buf)?;
        // write data
        self.storage.append(data)?;
        self.storage.flush()?;
        self.written_len += HEADER_SIZE + len;
        Ok(())
    }
}
