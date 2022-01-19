use crate::db::config::MAX_SEQUENCE_NUMBER;
use crate::db::format::VALUE_TYPE_FOR_SEEK;
use crate::db::format::{SequenceNumber, ValueType};
use crate::utils::coding::{decode_u64_le, encode_u64_le, put_fixed_64};
use crate::varint::EncodeVar;
use std::fmt;
use std::str::from_utf8;

/// Internal Key format:
/// ```text
/// ┌◄─────────────Internal Key ────────────────►│
/// │                                            │
/// ┼──────────┬───────────────┬─────────────────┼
/// │ user_key │ sequence (7B) │ value_type (1B) │
/// ┴──────────┴───────────────┴─────────────────┴
/// ```
#[derive(Default, Clone, Hash)]
pub struct InternalKey {
    pub rep: Vec<u8>,
}
// FIXME: rep clone

impl InternalKey {
    pub fn new(user_key: &[u8], sequence: SequenceNumber, value_type: ValueType) -> Self {
        let mut s = vec![];
        append_internal_key(
            &mut s,
            &ParsedInternalKey::new(user_key, sequence, value_type),
        );
        InternalKey { rep: s }
    }

    pub fn encode(&self) -> &[u8] {
        assert!(!self.rep.is_empty());
        &self.rep
    }

    pub fn decode(&mut self, s: &[u8]) {
        self.rep.clear();
        self.rep.extend_from_slice(s);
    }

    pub fn user_key(&self) -> &[u8] {
        extract_user_key(self.rep.as_slice())
    }

    pub fn clear(&mut self) {
        self.rep.clear()
    }
}

/// Append the parsed internal key into slice.
pub fn append_internal_key(result: &mut Vec<u8>, key: &ParsedInternalKey) {
    result.extend_from_slice(key.user_key);
    put_fixed_64(
        result,
        pack_sequence_and_type(key.sequence, &key.value_type),
    );
}

/// Pack the sequence number and value type
pub fn pack_sequence_and_type(seq: SequenceNumber, value_type: &ValueType) -> u64 {
    assert!(seq <= MAX_SEQUENCE_NUMBER);
    assert!(*value_type <= VALUE_TYPE_FOR_SEEK);
    (seq << 8) | *value_type as u64
}

/// Returns the user key portion of an internal key.
pub fn extract_user_key(internal_key: &[u8]) -> &[u8] {
    let length = internal_key.len();

    assert!(length >= 8);
    &internal_key[..length - 8]
}

fn format_key(rep: &[u8], f: &mut fmt::Formatter) -> fmt::Result {
    if rep.len() < 8 {
        write!(f, "Invalid Internal Key")
    } else {
        let num = decode_u64_le(&rep[(rep.len() - 8)..]);
        let seq = num >> 8;
        let value_type = ValueType::from(num & 0xff);
        match from_utf8(&rep[..(rep.len() - 8)]) {
            Ok(user_key) => {
                write!(f, "{:?},{:?},{:?}", user_key, seq, value_type)
            }
            Err(_) => {
                write!(
                    f,
                    "{:?},{:?},{:?}",
                    &rep[..(rep.len() - 8)],
                    seq,
                    value_type
                )
            }
        }
    }
}

impl fmt::Debug for InternalKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        format_key(self.rep.as_slice(), f)
    }
}

impl fmt::Display for InternalKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        format_key(self.rep.as_slice(), f)
    }
}

#[derive(Default)]
pub struct ParsedInternalKey<'a> {
    user_key: &'a [u8],
    sequence: SequenceNumber,
    value_type: ValueType,
}

impl<'a> ParsedInternalKey<'a> {
    pub fn new(user_key: &'a [u8], sequence: SequenceNumber, value_type: ValueType) -> Self {
        ParsedInternalKey {
            user_key,
            sequence,
            value_type,
        }
    }

    pub fn user_key(&self) -> &[u8] {
        self.user_key
    }

    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    pub fn value_type(&self) -> ValueType {
        self.value_type
    }
}

/// Attempt to parse an internal key from `internal_key`. On success,
/// stores the parsed data in "*result", and returns true.
/// On error, returns false, leaves `result` in an unchanged state.
pub fn parse_internal_key<'a>(internal_key: &'a [u8], result: &mut ParsedInternalKey<'a>) -> bool {
    let n = internal_key.len();
    if n < 8 {
        return false;
    }
    // [n - 8..n] is the SequenceNumber | ValueType portion.
    let num = decode_u64_le(&internal_key[n - 8..n]);
    let c = num & 0xff;

    result.sequence = num >> 8;
    result.value_type = c.into();
    result.user_key = &internal_key[0..n - 8];
    c <= ValueType::TypeValue.into()
}

/// LookUp Key format:
/// ```text
/// ◄─────────────────────────LookUp Key───────────────────────────►
/// │                                                              │
/// │                 ┌◄─────────────Internal Key ────────────────►│
/// │                 │                                            │
/// ├─────────────────┼──────────┬───────────────┬─────────────────┼
/// │ internal_key_len│ user_key │ sequence (7B) │ value_type (1B) │
/// └─────────────────┴──────────┴───────────────┴─────────────────┴
/// ```
pub struct LookUpKey {
    data: Vec<u8>,
    key_len_varint: usize,
    key_len: usize,
}

impl LookUpKey {
    pub fn new(user_key: &[u8], sequence: u64) -> Self {
        let size = user_key.len();
        let mut data = vec![];
        let key_len_varint: Vec<u8> = (size + 8).encode_varint();

        data.extend(&key_len_varint);
        data.extend_from_slice(user_key);
        let v = &mut [0; 8];
        // value_type of LookUpKey should always be VALUE_TYPE_FOR_SEEK (TypeValue)
        encode_u64_le(v, pack_sequence_and_type(sequence, &VALUE_TYPE_FOR_SEEK));
        data.extend_from_slice(v);

        LookUpKey {
            key_len_varint: key_len_varint.len(),
            key_len: size,
            data,
        }
    }

    pub fn memtable_key(&self) -> &Vec<u8> {
        &self.data
    }

    pub fn internal_key(&self) -> &[u8] {
        &self.data.as_slice()[self.key_len_varint..]
    }

    pub fn user_key(&self) -> &[u8] {
        &self.data.as_slice()[self.key_len_varint..][..self.key_len]
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_make_internal_key() {
        let c = String::from("test");
        let s = 2;
        let uk = ValueType::TypeValue;
        let key = InternalKey::new(c.as_bytes(), s, uk);
        assert_eq!(key.rep, &[116, 101, 115, 116, 1, 2, 0, 0, 0, 0, 0, 0]);
        assert_eq!(format!("{:?}", key), "\"test\",2,TypeValue");
    }

    #[test]
    fn test_make_look_up_key() {
        let k = LookUpKey::new(b"da", 1);
        assert_eq!(k.internal_key(), &[100, 97, 1, 1, 0, 0, 0, 0, 0, 0]);
        assert_eq!(k.user_key(), &[100, 97]);
        assert_eq!(
            k.memtable_key().as_slice(),
            &[10, 100, 97, 1, 1, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_parse_internal_key() {
        let s = &[254, 100, 101, 0, 1, 240, 84, 0, 0, 0, 0, 0];
        let mut p = ParsedInternalKey::default();
        parse_internal_key(s, &mut p);
        assert_eq!(p.user_key, &[254, 100, 101, 0]);
        assert_eq!(p.value_type, ValueType::TypeValue);
        assert_eq!(p.sequence, 21744);
    }
}
