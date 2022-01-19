use crate::db::errors::{DError, DResult};
use crate::db::format::{SequenceNumber, ValueType};
use crate::db::log_writer::HEADER_SIZE;
use crate::db::memory_table::MemoryTable;
use crate::utils::coding::{
    decode_u32_le, decode_u64_le, encode_u32_le, encode_u64_le, get_length_prefixed_slice,
    put_length_prefixed_slice,
};
use crossbeam_channel::Sender;
use std::mem;

const BATCH_HEADER_SIZE: usize = mem::size_of::<SequenceNumber>() + mem::size_of::<u32>();

/// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
/// WriteBatch format:
/// ```text
///  ┌─────────────┬───────────────┬───────┬───────────────┬────────┐
///  │  value_type │ length_varint │  key  │ length_varint │  value │
///  └─────────────┴───────────────┴───────┴───────────────┴────────┘
///                                     ▲
/// ┌────────────────┬───────────┬──────┴─────┬────────────┐
/// │  sequence (8B) │ count (4B)│   record   │   record   │
/// └────────────────┴───────────┴────────────┴────────────┘
/// ```
pub struct WriteBatch {
    /// rep stores data from `put` or `delete`
    pub rep: Vec<u8>,
}

impl WriteBatch {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        WriteBatch { rep: vec![0; 12] }
    }

    // Clear all updates buffered in this batch.
    pub fn clear(&mut self) {
        self.rep.clear();
    }

    /// This number is tied to implementation details, and may change across releases.
    pub fn approximate_size(&self) -> usize {
        self.rep.len()
    }

    pub fn iterate(&self, insert: &mut impl TableInserter) -> DResult<()> {
        let mut input = self.rep.as_slice();
        let c = self.count();
        if input.len() < BATCH_HEADER_SIZE {
            return Err(DError::CustomError("Malformed WriteBatch"));
        }
        input = &input[BATCH_HEADER_SIZE..];

        let mut found = 0;
        let mut key: &[u8] = &[];
        let mut value: &[u8] = &[];

        while !input.is_empty() {
            found += 1;
            let tag: ValueType = input[0].into();
            input = &input[1..];

            match tag {
                ValueType::TypeValue => {
                    if get_length_prefixed_slice(&mut input, &mut key)
                        && get_length_prefixed_slice(&mut input, &mut value)
                    {
                        insert.put(key, value);
                    } else {
                        return Err(DError::CustomError("Bad WriteBatch Put"));
                    }
                }
                ValueType::TypeDeletion => {
                    if get_length_prefixed_slice(&mut input, &mut key) {
                        insert.delete(key);
                    } else {
                        return Err(DError::CustomError("Bad WriteBatch Delete"));
                    }
                }
            };
        }
        if found != c {
            return Err(DError::CustomError("WriteBatch has wrong count"));
        }
        Ok(())
    }
    /// decode last four byte to count
    /// skip sequence number 8 bytes.
    /// Return the number of entries in the batch.
    pub fn count(&self) -> u32 {
        decode_u32_le(&self.rep.as_slice()[8..])
    }

    /// skip sequence number 8 bytes.
    /// Set the count for the number of entries in the batch.
    fn set_count(&mut self, n: u32) {
        let c = &mut self.rep.as_mut_slice()[8..];
        encode_u32_le(c, n)
    }

    /// | key.size() | key.data() | value.size() | value.data() |
    // Store the mapping "key->value" in the database.
    pub fn put(&mut self, key: &[u8], val: &[u8]) {
        self.set_count(self.count() + 1); // update batch count
        self.rep.extend_from_slice(&[ValueType::TypeValue.into()]);

        put_length_prefixed_slice(&mut self.rep, key);
        put_length_prefixed_slice(&mut self.rep, val);
    }

    // If the database contains a mapping for "key", erase it.  Else do nothing.
    pub fn delete(&mut self, key: &[u8]) {
        self.set_count(self.count() + 1);
        self.rep
            .extend_from_slice(&[ValueType::TypeDeletion.into()]);
        put_length_prefixed_slice(&mut self.rep, key);
    }

    pub fn byte_size(&self) -> usize {
        self.rep.len()
    }

    pub fn contents(batch: &Self) -> &[u8] {
        batch.rep.as_slice()
    }

    pub fn set_contents(&mut self, contents: &[u8]) {
        assert!(contents.len() >= HEADER_SIZE);
        self.rep = contents.to_owned();
    }

    /// Return the sequence number for the start of this batch.
    pub fn sequence(&self) -> SequenceNumber {
        decode_u64_le(self.rep.as_slice())
    }

    /// Store the specified number as the sequence number for the start of
    /// this batch.
    pub fn set_sequence(&mut self, seq: SequenceNumber) {
        encode_u64_le(self.rep.as_mut_slice(), seq);
    }

    pub(crate) fn insert_batch(batch: &WriteBatch, mem: &mut MemoryTable) -> DResult<()> {
        let last_sequence = batch.sequence();
        let insert = &mut MemTableInserter::new(last_sequence, mem);
        batch.iterate(insert)
    }

    // TODO: improve the clone slice.
    pub fn append(&mut self, src: &WriteBatch) {
        self.set_count(self.count() + src.count());
        assert!(src.rep.len() >= BATCH_HEADER_SIZE);
        self.rep
            .extend_from_slice(&src.rep.as_slice()[BATCH_HEADER_SIZE..src.rep.len()]);
    }
}

// trait for mock test.
pub trait TableInserter {
    fn put(&mut self, key: &[u8], val: &[u8]);

    fn delete(&mut self, key: &[u8]);
}

pub struct MemTableInserter<'a> {
    seq: SequenceNumber,
    mem: &'a mut MemoryTable,
}

impl<'a> MemTableInserter<'a> {
    pub(crate) fn new(seq: SequenceNumber, mem: &'a mut MemoryTable) -> Self {
        MemTableInserter { seq, mem }
    }
}

impl<'a> TableInserter for MemTableInserter<'a> {
    fn put(&mut self, key: &[u8], val: &[u8]) {
        self.mem.set(self.seq, ValueType::TypeValue, key, val);
        self.seq += 1;
    }
    fn delete(&mut self, key: &[u8]) {
        self.mem.set(self.seq, ValueType::TypeDeletion, key, &[]);
        self.seq += 1;
    }
}

pub struct BatchWriter {
    pub(crate) batch: Option<WriteBatch>,
    pub(crate) sync: bool,
    pub(crate) sender: Sender<DResult<()>>,
}

unsafe impl Send for BatchWriter {}

unsafe impl Sync for BatchWriter {}

impl BatchWriter {
    pub(crate) fn new(sync: bool, batch: Option<WriteBatch>, sender: Sender<DResult<()>>) -> Self {
        Self {
            batch,
            sync,
            sender,
        }
    }
}
