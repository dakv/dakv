use std::cmp::Ordering;
use std::ffi::OsStr;
use std::path::Path;
use std::ptr::null;
use std::slice;
use std::str;
use std::{fmt, ops, path};

#[allow(clippy::derive_hash_xor_eq)]
#[derive(Hash, Clone)]
pub struct Slice {
    data: *const u8,
    len: usize,
}

unsafe impl Send for Slice {}
unsafe impl Sync for Slice {}

impl Default for Slice {
    fn default() -> Self {
        Self {
            data: null(),
            len: 0,
        }
    }
}

impl Slice {
    pub fn new(s: *const u8, len: usize) -> Self {
        Self { data: s, len }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data, self.len) }
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        unsafe { str::from_utf8_unchecked(self.as_slice()) }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return true if the length of the referenced data is zero
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn clear(&mut self) {
        self.data = null();
        self.len = 0;
    }

    pub fn remove_prefix(&mut self, n: usize) {
        assert!(n <= self.len());
        self.data = unsafe { self.data.add(n) };
        self.len -= n;
    }

    pub fn remove_suffix(&mut self, n: usize) {
        assert!(n <= self.len());
        self.len -= n;
    }

    pub fn starts_with<'a, B>(&self, x: B) -> bool
    where
        B: Into<&'a [u8]>,
    {
        let a = self.as_slice();
        let b = x.into();
        (a.len() >= b.len()) && &a[..b.len()] == b
    }

    #[inline]
    pub fn compare(&self, b: &Slice) -> Ordering {
        self.as_slice().cmp(b.as_slice())
    }

    pub fn equal(&self, b: &Slice) -> bool {
        self.compare(b) == Ordering::Equal
    }
}

impl AsRef<path::Path> for Slice {
    fn as_ref(&self) -> &Path {
        Path::new(OsStr::new(self.as_str()))
    }
}

// Implement IntoIterator to be used in a for loop.
impl<'a> IntoIterator for &'a Slice {
    type Item = &'a u8;
    type IntoIter = std::slice::Iter<'a, u8>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

impl fmt::Debug for Slice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{:?}", self.as_slice())
    }
}

impl fmt::Display for Slice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let mut s = format!("{:?}", self.as_slice());
        s.drain(..1);
        s.drain(s.len() - 1..);
        write!(f, "{}", s)
    }
}

/// Implement indexing operations for Slice, return the nth data
impl ops::Index<usize> for Slice {
    type Output = u8;

    fn index(&self, n: usize) -> &Self::Output {
        assert!(n < self.len());
        &self.as_slice()[n]
    }
}

impl<'a> From<&'a Slice> for &'a [u8] {
    fn from(s: &'a Slice) -> Self {
        s.as_slice()
    }
}

impl From<&Vec<u8>> for Slice {
    fn from(a: &Vec<u8>) -> Self {
        Self::new(a.as_ptr(), a.len())
    }
}

impl From<&str> for Slice {
    fn from(a: &str) -> Self {
        Self::new(a.as_ptr(), a.len())
    }
}

impl From<&[u8]> for Slice {
    fn from(a: &[u8]) -> Self {
        Self::new(a.as_ptr(), a.len())
    }
}

impl From<&mut [u8]> for Slice {
    fn from(a: &mut [u8]) -> Self {
        Self::new(a.as_ptr(), a.len())
    }
}

impl PartialEq for Slice {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice().eq(other.as_slice())
    }
}

impl Eq for Slice {}

#[cfg(test)]
mod tests {
    use crate::utils::algorithm::{LowerBound, UpperBound};
    use crate::utils::slice::Slice;
    use std::cmp::Ordering;

    static TEST_STR: &str = "test";

    #[test]
    fn test_new() {
        let s: Slice = Slice::default();
        assert!(s.is_empty());
    }

    #[test]
    fn test_display_debug() {
        let a = vec![22, 44, 56];
        let s = Slice::from(&a);
        assert_eq!(format!("{}", s), "22, 44, 56");
        assert_eq!(format!("{:?}", s), "[22, 44, 56]");
    }

    #[test]
    fn test_from_str() {
        let s = Slice::from(TEST_STR);
        assert_eq!(s.len(), 4);

        let r = [116, 101, 115, 116];
        for i in 0..s.len() {
            assert_eq!(s[i], r[i]);
        }
    }

    #[test]
    fn test_from_bytes() {
        let a = vec![111];
        let s = Slice::from(a.as_slice());
        assert_eq!(s[0], 111);
    }

    #[test]
    fn test_from_vec() {
        let test_vec = vec![1, 2, 3];
        let s = Slice::from(&test_vec);
        assert_eq!(s[0], 1u8);
        assert_eq!(s[1], 2u8);
        assert_eq!(s[2], 3u8);
    }

    #[test]
    fn test_remove_prefix() {
        let prehistory = "prehistory";
        let mut s = Slice::from(prehistory);
        s.remove_prefix(3);
        assert_eq!(s.as_slice(), b"history");

        let test_vec = vec![1, 2, 3];
        let mut ss = Slice::from(&test_vec);
        ss.remove_prefix(1);
        assert_eq!(ss.as_slice(), [2, 3]);
    }

    #[test]
    fn test_remove_suffix() {
        let prehistory = "prehistory";

        let mut s = Slice::from(prehistory);
        s.remove_suffix(7);
        assert_eq!(s.as_slice(), b"pre");

        let test_vec = vec![1, 2, 3];
        let mut ss = Slice::from(&test_vec);
        ss.remove_suffix(2);
        assert_eq!(ss.as_slice(), [1]);
    }

    #[test]
    fn test_starts_with() {
        let a = vec![1, 2, 3];
        let b = vec![1, 2];
        let c = vec![1, 3];

        let long_str = Slice::from(&a);
        let sub_str = Slice::from(&b);
        let sub_str2 = Slice::from(&c);

        assert!(long_str.starts_with(&sub_str));
        assert!(!long_str.starts_with(&sub_str2));
    }

    #[test]
    fn test_compare() {
        let a = &vec![1, 2, 3];
        let b = &vec![1, 2, 3, 4];
        let c = &vec![1, 2, 4];

        assert!(Slice::from(a).eq(&Slice::from(a)));
        assert_eq!(Slice::from(a).compare(&Slice::from(b)), Ordering::Less);
        assert_eq!(Slice::from(a).compare(&Slice::from(c)), Ordering::Less);
        assert_eq!(Slice::from(c).compare(&Slice::from(a)), Ordering::Greater);
    }

    #[test]
    fn test_iter() {
        let a = &vec![1, 2, 3];
        let mut s = Slice::from(a);
        let mut c = 0;
        for i in s.into_iter() {
            c += i;
        }
        assert_eq!(c, 6);
        s.clear();
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn test_lower_bound() {
        let tmp = &vec![0, 97, 98, 99];

        let s1 = Slice::from("abc");
        let s2 = Slice::from("abcd");
        let s3 = Slice::from("ac");
        let s4 = Slice::from(tmp);
        assert_eq!([s1.clone()].lower_bound_by(|a, b| a.compare(&b), &s2), 1);
        assert_eq!([s1.clone()].lower_bound_by(|a, b| a.compare(&b), &s1), 0);
        assert_eq!([s1.clone()].lower_bound_by(|a, b| a.compare(&b), &s3), 1);
        assert_eq!([s1.clone()].lower_bound_by(|a, b| a.compare(&b), &s4), 0);
    }

    #[test]
    fn test_upper_bound() {
        let tmp = &vec![0, 97, 98, 99];

        let s1 = Slice::from("abc");
        let s2 = Slice::from("abcd");
        let s3 = Slice::from("ac");
        let s4 = Slice::from(tmp);

        impl Ord for Slice {
            fn cmp(&self, other: &Self) -> Ordering {
                self.compare(other)
            }
        }

        impl PartialOrd for Slice {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        assert_eq!([s1.clone()].upper_bound(&s2), 1);
        assert_eq!([s1.clone()].upper_bound(&s1), 1);
        assert_eq!([s1.clone()].upper_bound(&s3), 1);
        assert_eq!([s1.clone()].upper_bound(&s4), 0);
    }
}
