use crate::db::{DError, DResult};
use crate::table::block_handle::BlockHandle;
use crate::utils::coding::{decode_u32_le, put_fixed_32};

// TABLE_MAGIC_NUMBER was picked by running
//    echo dakv | sha1sum
// and taking the leading 64 bits.
const TABLE_MAGIC_NUMBER: u64 = 0x06e5a0e1a471c787;

/// Maximum encoding length of a BlockHandle is 10.
const MAX_ENCODED_LENGTH: usize = 10 + 10;

/// MetaIndex Handle + Index Handle <= 20 *2.
/// 8 bytes for magic number.
pub const ENCODED_LENGTH: usize = 2 * MAX_ENCODED_LENGTH + 8;

/// Footer encapsulates the fixed information stored at the tail
/// end of every table file.
///
/// Footer format:
/// ```text
/// ┌──────────────────┐
/// │ MetaIndex Handle │    ┌────────────────┬─────────────┐
/// ├──────────────────┼───►│offset (varint) │ size (varint│
/// │   Index Handle   │    └────────────────┴─────────────┘
/// ├──────────────────┤
/// │     padding      │
/// ├──────────────────┤
/// │ magic number (8B)│
/// └──────────────────┘
/// ```
#[derive(Default)]
pub struct Footer {
    meta_index_handle: BlockHandle,
    index_handle: BlockHandle,
}

impl Footer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_meta_index_handle(&mut self, handle: BlockHandle) {
        self.meta_index_handle = handle;
    }

    pub fn get_meta_index_handle(&self) -> BlockHandle {
        self.meta_index_handle
    }

    pub fn set_index_handle(&mut self, handle: BlockHandle) {
        self.index_handle = handle;
    }

    pub fn get_index_handle(&self) -> &BlockHandle {
        &self.index_handle
    }

    pub fn encode(&self, s: &mut Vec<u8>) {
        let size = s.len();
        self.meta_index_handle.encode(s);
        self.index_handle.encode(s);
        // fill to 40 bytes
        let padding = 2 * MAX_ENCODED_LENGTH - s.len();
        s.extend(vec![0; padding]);
        // Put magic number.
        put_fixed_32(s, (TABLE_MAGIC_NUMBER & 0xffffffff) as u32);
        put_fixed_32(s, (TABLE_MAGIC_NUMBER.wrapping_shr(32)) as u32);
        assert_eq!(s.len(), size + ENCODED_LENGTH);
    }

    // Verify the magic number and decode to `meta_index_handle` and `index_handle`
    pub fn decode(&mut self, input: &[u8]) -> DResult<()> {
        // Decode magic number from 40 to 48 bytes.
        let magic = &input[2 * MAX_ENCODED_LENGTH..ENCODED_LENGTH];
        let magic_low = decode_u32_le(magic);
        let magic_high = decode_u32_le(&magic[4..]) as u64;
        let magic = magic_high.wrapping_shl(32) | magic_low as u64;
        if magic != TABLE_MAGIC_NUMBER {
            return Err(DError::CustomError("Invalid magic number"));
        }
        // Decode `meta_index_handle` and `index_handle`
        let mut tmp = input;
        self.meta_index_handle.decode(&mut tmp)?;
        self.index_handle.decode(&mut tmp)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::table::block_handle::BlockHandle;
    use crate::table::footer::Footer;

    #[test]
    fn test_footer() {
        let mut data = Vec::new();
        {
            let mut footer = Footer::new();
            let mut meta_index_block_handle = BlockHandle::new();
            meta_index_block_handle.set_offset(12345);
            meta_index_block_handle.set_size(54321);

            let mut index_block_handle = BlockHandle::new();
            index_block_handle.set_offset(123);
            index_block_handle.set_size(321);

            footer.set_meta_index_handle(meta_index_block_handle);
            footer.set_index_handle(index_block_handle);

            footer.encode(&mut data);
        }
        let mut s = [
            185, 96, 177, 168, 3, 123, 193, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 135, 199, 113, 164, 225, 160, 229, 6,
        ];
        assert_eq!(data.as_slice(), &s);
        let mut expected = Footer::new();
        expected.decode(data.as_slice()).unwrap();
        assert_eq!(format!("{}", expected.index_handle), "123:321");
        assert_eq!(format!("{}", expected.meta_index_handle), "12345:54321");
        s[47] = 1;
        let mut expected = Footer::new();
        let ret = expected.decode(&s);
        assert!(ret.is_err());
        assert_eq!(ret.unwrap_err().to_string(), "Invalid magic number");
    }
}
