use crate::db::log_format::RecordType;
use crate::db::log_writer::{BLOCK_SIZE, HEADER_SIZE};
use crate::env::SequentialFile;
use crate::utils::coding::decode_u32_le;
use crate::utils::crc32c::{unmask, value};
use std::sync::Arc;

/// Create reader from file, file must remain live while this Reader is in use.
/// Only use reader when we open the database
pub struct Reader {
    checksum: bool,
    /// Last Read() indicated EOF by returning < BlockSize
    eof: bool,
    storage: Arc<dyn SequentialFile>,
    /// Read the whole block and check the crc value and remove the header(length + type)
    buffer: Vec<u8>,
    // Offset of the first location past the end of buffer.
    end_of_read_offset: usize,
    // Offset of the last record returned by `read_record`.
    last_record_offset: usize,
    // Improve the memory.
    backing_store: [u8; BLOCK_SIZE],
}

impl Reader {
    pub fn new(r: Arc<dyn SequentialFile>, checksum: bool) -> Self {
        Reader {
            storage: r,
            checksum,
            eof: false,
            buffer: Default::default(),
            end_of_read_offset: 0,
            last_record_offset: 0,
            backing_store: [0; BLOCK_SIZE],
        }
    }

    pub fn read_record(&mut self, result: &mut Vec<u8>) -> bool {
        result.clear();
        let mut in_fragmented_record = false;
        let mut prospective_record_offset = 0;
        loop {
            let mut fragment = vec![];
            let record_type = self.read_record_from_file(&mut fragment);

            // assert!(self.end_of_read_offset >= (self.buffer.len() + HEADER_SIZE + fragment.len()));
            let current_record_offset = self
                .end_of_read_offset
                .wrapping_sub(self.buffer.len() + HEADER_SIZE + fragment.len());
            match record_type {
                RecordType::FullType => {
                    prospective_record_offset = current_record_offset;
                    self.last_record_offset = prospective_record_offset;
                    *result = fragment;
                    return true;
                }
                RecordType::FirstType => {
                    prospective_record_offset = current_record_offset;
                    in_fragmented_record = true;
                    *result = fragment;
                }
                RecordType::MiddleType => {
                    if in_fragmented_record {
                        result.extend_from_slice(fragment.as_slice());
                    }
                }
                RecordType::LastType => {
                    if in_fragmented_record {
                        result.extend_from_slice(fragment.as_slice());
                        self.last_record_offset = prospective_record_offset;
                        return true;
                    }
                }
                RecordType::Eof => {
                    if in_fragmented_record {
                        result.clear();
                    }
                    return false;
                }
                RecordType::BadRecord => {
                    if in_fragmented_record {
                        in_fragmented_record = false;
                        result.clear();
                    }
                }
            };
        }
    }

    pub fn read_record_from_file(&mut self, result: &mut Vec<u8>) -> RecordType {
        loop {
            // Skip the metadata to read a new record
            if self.buffer.len() < HEADER_SIZE {
                self.buffer.clear();
                if self.eof {
                    // Note that if buffer_ is non-empty, we have a truncated header at the
                    // end of the file, which can be caused by the writer crashing in the
                    // middle of writing the header. Instead of considering this an error,
                    // just report EOF.
                    return RecordType::Eof;
                } else {
                    match self.storage.read(BLOCK_SIZE, self.backing_store.as_mut()) {
                        Ok(n) => {
                            if n == 0 {
                                // Read to the EOF when rust reads empty file
                                self.eof = true;
                                self.buffer.clear();
                            }
                            // increase read offset
                            self.end_of_read_offset += n;
                            self.buffer.extend_from_slice(&self.backing_store[..n]);
                            self.eof = n < BLOCK_SIZE; // reached end of file
                        }
                        Err(_) => {
                            self.eof = true;
                            self.buffer.clear();
                            return RecordType::Eof;
                        }
                    }
                    continue;
                }
            }
            /// Log format:
            /// ```text
            /// ┌◄────────────────── Header ────────────────►┐◄────Data─────►┐
            /// │                                            │               │
            /// ├───────────┬──────────────┬─────────────────┼───────────────┤
            /// │  CRC (4B) │  length (2B) │ record_type(1B) │ data (n Bytes)│
            /// └───────────┴──────────────┴─────────────────┴───────────────┘
            /// ```
            let a = self.buffer.as_slice()[4] as u32 & 0xff;
            let b = self.buffer.as_slice()[5] as u32 & 0xff;
            let length = (a | (b << 8)) as usize;
            // Get record type
            let type_ = RecordType::from(&self.buffer.as_slice()[6]);

            if HEADER_SIZE + length > self.buffer.len() {
                self.buffer.clear();
                if self.eof {
                    return RecordType::Eof;
                }
                return RecordType::BadRecord;
            }
            if self.checksum {
                let expected_crc = unmask(decode_u32_le(self.buffer.as_slice()));
                let actual_crc = value(&self.buffer.as_slice()[6..7 + length]);
                if actual_crc != expected_crc {
                    self.buffer.clear();
                    // TODO: ReportCorruption
                    return RecordType::BadRecord;
                }
            }
            if (self.end_of_read_offset as isize) - (self.buffer.len() as isize) < 0 {
                return RecordType::BadRecord;
            }
            result.clear();
            result.extend_from_slice(&self.buffer.as_slice()[HEADER_SIZE..HEADER_SIZE + length]);
            // remove parsed fragment prefix
            self.buffer.drain(..HEADER_SIZE + length);

            return type_;
        }
    }
}
