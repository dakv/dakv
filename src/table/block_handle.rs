use crate::db::{DError, DResult};
use crate::utils::coding::{get_varint64, put_varint};
use std::fmt;

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
#[derive(Default, Clone, Copy)]
pub struct BlockHandle {
    offset: u64,
    size: u64,
}

impl BlockHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn encode(&self, dst: &mut Vec<u8>) {
        put_varint(dst, self.offset);
        put_varint(dst, self.size);
    }

    pub fn decode(&mut self, s: &mut &[u8]) -> DResult<()> {
        if get_varint64(s, &mut self.offset) && get_varint64(s, &mut self.size) {
            Ok(())
        } else {
            Err(DError::CustomError("Bad block handle"))
        }
    }

    pub fn set_offset(&mut self, offset: u64) {
        self.offset = offset;
    }

    // The offset of the block in the file.
    pub fn get_offset(&self) -> u64 {
        self.offset
    }

    pub fn set_size(&mut self, size: u64) {
        self.size = size;
    }

    // The size of the stored block
    pub fn get_size(&self) -> u64 {
        self.size
    }
}

impl fmt::Display for BlockHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.offset, self.size)
    }
}
