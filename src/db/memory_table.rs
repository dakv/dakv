use std::ptr::copy_nonoverlapping;

use crate::db::errors::{DError, DResult};
use crate::db::format::{SequenceNumber, ValueType};
use crate::db::key::LookUpKey;
use crate::skiplist::{SkipList, SkipListIter};
use crate::utils::arena::{Arena, ArenaImpl};
use crate::utils::cmp::{BaseComparator, InternalKeyComparator};
use crate::utils::coding::{decode_u64_le, encode_u64_le, get_varint32_ptr, put_varint};
use crate::utils::iter::Iter;
use crate::utils::random::Random;
use crate::utils::slice::Slice;
use std::cmp::Ordering;
use std::sync::Arc;
use varint::EncodeVar;

type Table = SkipList<Random, KeyComparator, ArenaImpl>;

fn parse_value(input: &[u8]) -> &[u8] {
    let mut len = 0;
    let (p, _) = get_varint32_ptr(input, unsafe { input.as_ptr().add(5) }, &mut len);
    &p[..len as usize]
}

/// Since internal key and value are stored in the [data_length_varint] + [data] format,
/// we can parse varint of data length to retrieve the next data slice.
/// [internal_key_length (varint)] [internal_key]
/// [value_length (varint)] [value]
fn get_user_key_from_slice(input: &[u8]) -> &[u8] {
    let (value, length) = u32::decode_varint(input);
    &input[length..value as usize + length]
}

#[derive(Clone)]
pub struct KeyComparator {
    cmp: Arc<dyn InternalKeyComparator + Send + Sync>,
}

impl KeyComparator {
    pub fn new(cmp: Arc<dyn InternalKeyComparator + Send + Sync>) -> Self {
        Self { cmp }
    }
}

impl BaseComparator for KeyComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        // Internal keys are encoded as length-prefixed strings.
        let aa = get_user_key_from_slice(a);
        let bb = get_user_key_from_slice(b);
        self.cmp.compare(aa, bb)
    }
}

pub struct MemoryTable {
    table: Table,
    cmp: KeyComparator,
    // arena stores the key value entry.
    arena: ArenaImpl,
}

unsafe impl Send for MemoryTable {}

unsafe impl Sync for MemoryTable {}

impl MemoryTable {
    pub fn new(cmp: Arc<dyn InternalKeyComparator + Send + Sync>) -> Self {
        let key_cmp = KeyComparator::new(cmp.clone());
        let arena = ArenaImpl::new();
        MemoryTable {
            cmp: key_cmp.clone(),
            table: SkipList::new(Random::new(0xdead_beef), key_cmp, ArenaImpl::new()),
            arena,
        }
    }

    pub fn new_iter(&self) -> MemIter {
        MemIter::new(self.table.clone(), self.arena.clone())
    }

    pub fn approximate_memory_usage(&self) -> usize {
        self.arena.memory_usage()
    }

    /// Varint encode to save space and bandwidth.
    ///
    /// Key value entry format:
    /// ```text
    /// ┌◄──────────────────────────────── Key Value Entry ───────────────────────────────►┐
    /// │                                                                                  │
    /// ◄─────────────────────────LookUp Key───────────────────────────►                   │
    /// │                                                              │                   │
    /// │                 ┌◄─────────────Internal Key ────────────────►│                   │
    /// │                 │                                            │                   │
    /// ├─────────────────┼──────────┬───────────────┬─────────────────┼──────────┬────────┤
    /// │ internal_key_len│ user_key │ sequence (7B) │ value_type (1B) │ value_len│ value  │
    /// └─────────────────┴──────────┴───────────────┴─────────────────┴──────────┴────────┘
    ///     (varint)                                                      (varint)
    /// ```
    pub fn set(&mut self, s: SequenceNumber, t: ValueType, key: &[u8], value: &[u8]) {
        // generate varint and entry length
        let key_len = key.len();
        let value_len = value.len();
        let internal_key_len = key_len + 8;

        let internal_key_len_varint = internal_key_len.encode_varint();
        let value_len_varint = value_len.encode_varint();
        let encoded_len =
            internal_key_len_varint.len() + internal_key_len + value_len_varint.len() + value_len;
        // alloc buffer with encoded_len
        let buffer: &mut [u8] = self.arena.allocate(encoded_len);
        // fill data
        let mut sequence_value_type = [0; 8];
        encode_u64_le(&mut sequence_value_type, s << 8 | t as u64);
        unsafe {
            copy_nonoverlapping(
                internal_key_len_varint.as_ptr(),
                buffer.as_mut_ptr(),
                internal_key_len_varint.len(),
            );
            let mut offset = internal_key_len_varint.len();
            copy_nonoverlapping(key.as_ptr(), buffer[offset..].as_mut_ptr(), key.len());
            offset += key.len();
            copy_nonoverlapping(
                sequence_value_type.as_ptr(),
                buffer[offset..].as_mut_ptr(),
                sequence_value_type.len(),
            );
            offset += sequence_value_type.len();
            copy_nonoverlapping(
                value_len_varint.as_ptr(),
                buffer[offset..].as_mut_ptr(),
                value_len_varint.len(),
            );
            offset += value_len_varint.len();
            copy_nonoverlapping(value.as_ptr(), buffer[offset..].as_mut_ptr(), value.len());
        }
        self.table.insert(Slice::new(buffer.as_ptr(), buffer.len()));
    }

    pub fn get(&self, lookup_key: &LookUpKey) -> DResult<Vec<u8>> {
        let mem_key = lookup_key.memtable_key();
        let mut iter = SkipListIter::new(self.table.clone());
        iter.seek(mem_key.as_slice());
        if iter.valid() {
            let entry = iter.key();

            let mut key_len = 0;
            let (key_ptr, _) = get_varint32_ptr(entry, entry[5..].as_ptr(), &mut key_len);
            /// SkipListIter get the greater or equal value, so need to check the user_key.
            if self
                .cmp
                .cmp
                .user_comparator()
                .eq(&key_ptr[..key_len as usize - 8], lookup_key.user_key())
            {
                let seq_val = &key_ptr[key_len as usize - 8..];
                let tag = decode_u64_le(seq_val);
                return match ValueType::from(tag & 0xff_u64) {
                    // ignore sequence number
                    ValueType::TypeValue => {
                        let value_pack = &key_ptr[key_len as usize..];
                        let value = parse_value(value_pack);
                        Ok(value.to_owned())
                    }
                    // KeyIsDeleted
                    ValueType::TypeDeletion => Err(DError::NotFound),
                };
            }
        }
        Err(DError::NotFound)
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn encode_key(t: &mut Vec<u8>, s: &[u8]) {
    t.clear();
    put_varint(t, s.len());
    t.extend_from_slice(s);
}

pub struct MemIter {
    iter: SkipListIter<Random, KeyComparator, ArenaImpl>,
    // For passing to EncodeKey
    tmp: Vec<u8>,
    // make sure the kv data will not be released.
    _arena: ArenaImpl,
}

impl MemIter {
    pub fn new(mem: Table, _arena: ArenaImpl) -> Self {
        Self {
            tmp: Default::default(),
            iter: SkipListIter::new(mem),
            _arena,
        }
    }
}

impl Iter for MemIter {
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first()
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last()
    }

    fn seek(&mut self, target: &[u8]) {
        encode_key(&mut self.tmp, target);
        self.iter.seek(self.tmp.as_slice())
    }

    fn next(&mut self) {
        self.iter.next()
    }

    fn prev(&mut self) {
        self.iter.prev()
    }

    /// parse the key value entry
    /// skip the internal key varint, return the internal key.
    fn key(&self) -> &[u8] {
        // Key Value Entry
        let entry = self.iter.key();
        let (value, length) = u32::decode_varint(entry);
        // skip the internal_key_varint_length, return the internal_key
        &entry[length..value as usize + length]
    }

    /// parse the key value entry
    /// skip the value varint length
    fn value(&self) -> &[u8] {
        let entry = self.iter.key();
        let (v, l) = u32::decode_varint(entry);
        let (_, value) = u32::decode_varint(&entry[v as usize + l..]);
        &entry[v as usize + l + value..]
    }
}
