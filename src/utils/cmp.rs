use crate::db::{extract_user_key, pack_sequence_and_type, MAX_SEQUENCE_NUMBER};
use crate::db::{InternalKey, ValueType};
use crate::utils::coding::{decode_u64_le, put_fixed_64};
use std::cmp::{min, Ordering};
use std::sync::Arc;

pub trait BaseComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    /// Less than
    fn lt(&self, a: &[u8], b: &[u8]) -> bool {
        self.compare(a, b) == Ordering::Less
    }

    /// Less than or equal
    fn le(&self, a: &[u8], b: &[u8]) -> bool {
        self.compare(a, b) != Ordering::Greater
    }

    /// Greater than
    fn gt(&self, a: &[u8], b: &[u8]) -> bool {
        self.compare(a, b) == Ordering::Greater
    }

    /// Greater than or equal
    fn ge(&self, a: &[u8], b: &[u8]) -> bool {
        self.compare(a, b) != Ordering::Less
    }

    /// Equal
    fn eq(&self, a: &[u8], b: &[u8]) -> bool {
        self.compare(a, b) == Ordering::Equal
    }

    /// Not equal
    fn ne(&self, a: &[u8], b: &[u8]) -> bool {
        self.compare(a, b) != Ordering::Equal
    }
}

#[derive(Default, Clone)]
pub struct DefaultComparator;

impl BaseComparator for DefaultComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        if a.eq(b) {
            Ordering::Equal
        } else if a.gt(b) {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

pub trait Comparator: BaseComparator {
    /// Advanced functions: these are used to reduce the space requirements
    /// for internal data structures like index blocks.
    /// Return the short string in [start,limit) if `start` < `limit`,
    /// else return `start`.
    /// More information on the tests can be found below.
    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8>;

    // Return the short string >= key.
    fn find_short_successor(&self, key: &[u8]) -> Vec<u8>;

    /// The name of the comparator. Used to check for comparator
    /// mismatches (i.e., a DB created with one comparator is
    /// accessed using a different comparator.
    ///
    /// The client of this package should switch to a new name whenever
    /// the comparator implementation changes in a way that will cause
    /// the relative ordering of any two keys to change.
    fn name(&self) -> &'static str;
}

pub trait InternalKeyComparator: Comparator {
    fn user_comparator(&self) -> Arc<dyn Comparator + Send + Sync>;
    fn into_cmp(self: Arc<Self>) -> Arc<dyn Comparator + Send + Sync>;
    fn compare_internal_key(&self, a: &InternalKey, b: &InternalKey) -> Ordering;
}

#[derive(Default)]
pub struct BytewiseComparatorImpl;

impl BytewiseComparatorImpl {
    pub fn new() -> Self {
        Self::default()
    }
}

impl BaseComparator for BytewiseComparatorImpl {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        // TODO: a.cmp(b)
        let min_len = min(a.len(), b.len());
        let mut r = a[..min_len].cmp(&b[..min_len]);
        if r == Ordering::Equal {
            r = a.len().cmp(&b.len())
        }
        r
    }
}

impl Comparator for BytewiseComparatorImpl {
    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
        let mut new_start = start.to_vec();

        let min_len = min(start.len(), limit.len());
        let mut diff_index = 0;

        while diff_index < min_len && (start[diff_index] == limit[diff_index]) {
            diff_index += 1;
        }

        if diff_index < min_len {
            let diff_byte = start[diff_index] as u8;
            if (diff_byte < 0xff_u8) && diff_byte + 1 < limit[diff_index] as u8 {
                new_start[diff_index] += 1;
                // cut the string
                new_start.resize(diff_index + 1, 0);
                assert_eq!(self.compare(start, limit), Ordering::Less);
            }
        }
        new_start
    }

    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        let mut new_key = key.to_vec();

        let n = key.len();
        for i in 0..n {
            let b = key[i];
            if b != 0xff_u8 {
                new_key[i] = b + 1;
                new_key.resize(i + 1, 0);
                return new_key;
            }
        }
        new_key
    }

    fn name(&self) -> &'static str {
        "BytewiseComparator"
    }
}

pub struct InternalKeyComparatorImpl {
    user_cmp: Arc<dyn Comparator + Send + Sync>,
}

/// Default BytewiseComparatorImpl
impl Default for InternalKeyComparatorImpl {
    fn default() -> Self {
        Self::new(Arc::new(BytewiseComparatorImpl::new()))
    }
}

impl InternalKeyComparatorImpl {
    pub fn new(user_cmp: Arc<dyn Comparator + Send + Sync>) -> Self {
        Self { user_cmp }
    }
}

impl BaseComparator for InternalKeyComparatorImpl {
    // Order by:
    //    increasing user key (according to user-supplied comparator)
    //    decreasing sequence number
    //    decreasing type (though sequence should be enough to disambiguate)
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        let mut r = self
            .user_cmp
            .compare(extract_user_key(a), extract_user_key(b));
        if r == Ordering::Equal {
            let a_len = a.len();
            let b_len = b.len();
            let a_num = decode_u64_le(&a[a_len - 8..]);
            let b_num = decode_u64_le(&b[b_len - 8..]);
            r = b_num.cmp(&a_num)
        }
        r
    }
}

impl Comparator for InternalKeyComparatorImpl {
    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
        let user_start = extract_user_key(start);
        let user_limit = extract_user_key(limit);
        // `tmp` and `user_start` can be modified independently.
        let mut tmp = user_start.to_vec();
        tmp = self.user_cmp.find_shortest_separator(&tmp, user_limit);

        if tmp.len() < user_start.len() && self.user_cmp.lt(user_start, &tmp) {
            put_fixed_64(
                &mut tmp,
                pack_sequence_and_type(MAX_SEQUENCE_NUMBER, &ValueType::TypeValue),
            );
            return tmp;
        }
        start.to_vec()
    }

    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        let user_key = extract_user_key(key);
        // `tmp` and `user_key` can be modified independently.
        let mut tmp = user_key.to_vec();

        tmp = self.user_cmp.find_short_successor(&tmp);
        if tmp.len() < user_key.len() && self.user_cmp.lt(user_key, &tmp) {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            put_fixed_64(
                &mut tmp,
                pack_sequence_and_type(MAX_SEQUENCE_NUMBER, &ValueType::TypeValue),
            );
            return tmp;
        }
        key.to_vec()
    }

    fn name(&self) -> &'static str {
        "InternalKeyComparator"
    }
}

impl InternalKeyComparator for InternalKeyComparatorImpl {
    fn user_comparator(&self) -> Arc<dyn Comparator + Send + Sync> {
        self.user_cmp.clone()
    }

    fn into_cmp(self: Arc<Self>) -> Arc<dyn Comparator + Send + Sync> {
        self
    }

    fn compare_internal_key(&self, a: &InternalKey, b: &InternalKey) -> Ordering {
        self.compare(a.encode(), b.encode())
    }
}

#[cfg(test)]
mod tests {
    use crate::db::{
        append_internal_key, parse_internal_key, ParsedInternalKey, MAX_SEQUENCE_NUMBER,
    };
    use crate::db::{SequenceNumber, ValueType};
    use crate::utils::cmp::{
        BaseComparator, BytewiseComparatorImpl, Comparator, DefaultComparator,
        InternalKeyComparatorImpl,
    };
    use std::cmp::Ordering;
    use std::cmp::Ordering::{Equal, Greater, Less};
    use std::sync::Arc;

    #[test]
    fn test_basic() {
        let cmp = DefaultComparator::default();
        assert_eq!(cmp.compare(&[1], &[2]), Ordering::Less);
        assert_eq!(cmp.compare(&[2], &[2]), Ordering::Equal);
        assert_eq!(cmp.compare(&[2], &[1]), Ordering::Greater);
    }

    #[test]
    fn test_simplify() {
        let cmp = DefaultComparator::default();
        assert!(cmp.lt(&[1], &[2]));
        assert!(cmp.le(&[2], &[2]));
        assert!(cmp.ge(&[2], &[2]));
        assert!(cmp.eq(&[2], &[2]));
        assert!(cmp.gt(&[2], &[1]));
        assert!(cmp.ne(&[2], &[1]));
    }

    #[test]
    fn test_name() {
        let c = BytewiseComparatorImpl::new();
        assert_eq!(c.name(), "BytewiseComparator");
    }

    #[test]
    fn test_simple() {
        let c: Box<dyn Comparator> = Box::new(BytewiseComparatorImpl::new());
        assert_eq!(c.compare(b"a", b"b"), Less);
        assert_eq!(c.compare(b"a", b"a"), Equal);
        assert_eq!(c.compare(b"b", b"a"), Greater);
    }

    #[test]
    fn test_find_shortest_separator() {
        let c: Box<dyn Comparator> = Box::new(BytewiseComparatorImpl::new());
        let mut start = b"request".to_vec();
        let limit = b"require";
        start = c.find_shortest_separator(&start, limit);
        assert_eq!(start, b"requf");

        let mut start = b"abcde".to_vec();
        let limit = b"abcdx";
        start = c.find_shortest_separator(&start, limit);
        assert_eq!(start, b"abcdf");

        let mut start = b"abcdx".to_vec();
        let limit = b"abcde";
        start = c.find_shortest_separator(&start, limit);
        assert_eq!(start, b"abcdx");

        let mut start = b"abcdex".to_vec();
        let limit = b"abcdfg";
        start = c.find_shortest_separator(&start, limit);
        assert_eq!(start, b"abcdex");
    }

    #[test]
    fn test_find_short_successor() {
        let c: Box<dyn Comparator> = Box::new(BytewiseComparatorImpl::new());
        let mut s = Vec::from("ace");
        s = c.find_short_successor(&s);
        assert_eq!(s, b"b");

        let mut s = vec![255, 119, 111, 114, 108, 100];
        s = c.find_short_successor(&mut s);
        assert_eq!(s, [255, 120]);
    }

    // use `str` to write test easily.
    fn ikey(user_key: &str, seq: SequenceNumber, vt: ValueType) -> Vec<u8> {
        let mut encoded = vec![];
        append_internal_key(
            &mut encoded,
            &ParsedInternalKey::new(user_key.as_bytes(), seq, vt),
        );
        encoded
    }

    fn shorten(s: &[u8], l: &[u8]) -> Vec<u8> {
        let user_cmp = BytewiseComparatorImpl::new();
        let cmp = InternalKeyComparatorImpl::new(Arc::new(user_cmp));
        cmp.find_shortest_separator(&s, l)
    }

    fn short_successor(s: &[u8]) -> Vec<u8> {
        let mut result = s.to_vec();
        let user_cmp = BytewiseComparatorImpl::new();
        let cmp = InternalKeyComparatorImpl::new(Arc::new(user_cmp));
        result = cmp.find_short_successor(&result);
        result
    }

    fn test_key(key: &str, seq: SequenceNumber, vt: ValueType) {
        let encoded = ikey(key, seq, vt);
        let mut decoded = ParsedInternalKey::default();

        assert!(parse_internal_key(&encoded, &mut decoded));
        assert_eq!(key.as_bytes(), decoded.user_key());
        assert_eq!(seq, decoded.sequence());
        assert_eq!(vt, decoded.value_type());
        assert!(!parse_internal_key(b"bar", &mut decoded));
    }

    #[test]
    fn test_encode_decode() {
        let keys = vec!["", "k", "hello", "longggggggggggggggggggggg"];
        let seq = vec![
            1u64,
            2u64,
            3u64,
            1u64 << 8 - 1,
            1u64 << 8,
            1u64 << 8 + 1,
            1u64 << 16 - 1,
            1u64 << 16,
            1u64 << 16 + 1,
            1u64 << 32 - 1,
            1u64 << 32,
            1u64 << 32 + 1,
        ];
        for i in keys {
            for j in seq.clone() {
                test_key(i, j, ValueType::TypeValue);
                test_key("hello", 1, ValueType::TypeDeletion);
            }
        }
    }

    #[test]
    fn test_find_short_separator() {
        let s = ikey("foo", 100, ValueType::TypeValue);
        let b1 = ikey("foo", 100, ValueType::TypeValue);
        let b2 = ikey("foo", 99, ValueType::TypeValue);

        let b = shorten(&b1.as_slice(), &b2.as_slice());
        // When user keys are same
        assert_eq!(s.as_slice(), b);
        assert_eq!(
            ikey("foo", 100, ValueType::TypeValue).as_slice(),
            shorten(
                &ikey("foo", 100, ValueType::TypeValue).as_slice(),
                &ikey("foo", 101, ValueType::TypeValue).as_slice(),
            )
        );
        assert_eq!(
            ikey("foo", 100, ValueType::TypeValue).as_slice(),
            shorten(
                &ikey("foo", 100, ValueType::TypeValue).as_slice(),
                &ikey("foo", 100, ValueType::TypeValue).as_slice(),
            )
        );
        // When user keys are misordered
        assert_eq!(
            ikey("foo", 100, ValueType::TypeValue).as_slice(),
            shorten(
                &ikey("foo", 100, ValueType::TypeValue).as_slice(),
                &ikey("bar", 99, ValueType::TypeValue).as_slice(),
            )
        );
        // When user keys are different, but correctly ordered
        assert_eq!(
            ikey("g", MAX_SEQUENCE_NUMBER, ValueType::TypeValue).as_slice(),
            shorten(
                &ikey("foo", 100, ValueType::TypeValue).as_slice(),
                &ikey("hello", 200, ValueType::TypeValue).as_slice(),
            )
        );
        // When start user key is prefix of limit user key
        assert_eq!(
            ikey("foo", 100, ValueType::TypeValue).as_slice(),
            shorten(
                &ikey("foo", 100, ValueType::TypeValue).as_slice(),
                &ikey("foobar", 200, ValueType::TypeValue).as_slice(),
            )
        );
        // When limit user key is prefix of start user key
        assert_eq!(
            ikey("foobar", 100, ValueType::TypeValue).as_slice(),
            shorten(
                &ikey("foobar", 100, ValueType::TypeValue).as_slice(),
                &ikey("foo", 200, ValueType::TypeValue).as_slice(),
            )
        );
    }

    #[test]
    fn test_shortest_successor() {
        assert_eq!(
            ikey("g", MAX_SEQUENCE_NUMBER, ValueType::TypeValue).as_slice(),
            short_successor(ikey("foo", 100, ValueType::TypeValue).as_slice()).as_slice()
        );
        assert_eq!(
            ikey(
                unsafe { std::str::from_utf8_unchecked(&[255, 255]) },
                100,
                ValueType::TypeValue,
            )
            .as_slice(),
            short_successor(
                ikey(
                    unsafe { std::str::from_utf8_unchecked(&[255, 255]) },
                    100,
                    ValueType::TypeValue,
                )
                .as_slice()
            )
            .as_slice()
        );
    }
}
