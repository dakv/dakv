use crate::db::key::InternalKey;
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};

/// meta data of the table file.
pub struct FileMetaData {
    /// seeks allowed until compaction
    /// Actually, only this field can be modified once FileMetaData has been created.
    allowed_seeks: AtomicI64,
    // the number of table file
    file_number: u64,
    // File size in bytes
    file_size: u64,
    // Smallest internal key served by table
    smallest: InternalKey,
    // Largest internal key served by table
    largest: InternalKey,
}

impl FileMetaData {
    pub fn new() -> FileMetaData {
        FileMetaData {
            allowed_seeks: AtomicI64::new(1 << 30),
            file_number: 0,
            file_size: 0,
            smallest: Default::default(),
            largest: Default::default(),
        }
    }

    pub fn allow_seeks(&self) -> i64 {
        self.allowed_seeks.load(Ordering::Acquire)
    }

    pub fn set_allow_seeks(&self, a: i64) {
        self.allowed_seeks.store(a, Ordering::Release);
    }

    pub fn decrease_seeks(&self) {
        self.allowed_seeks.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn get_file_size(&self) -> u64 {
        self.file_size
    }

    pub fn set_file_size(&mut self, s: u64) {
        self.file_size = s;
    }

    pub fn get_file_number(&self) -> u64 {
        self.file_number
    }

    pub fn set_file_number(&mut self, n: u64) {
        self.file_number = n;
    }

    pub fn get_smallest(&self) -> &InternalKey {
        &self.smallest
    }

    pub fn set_smallest(&mut self, key: InternalKey) {
        self.smallest = key;
    }

    pub fn set_smallest_from_slice(&mut self, key: &[u8]) {
        self.smallest.rep = key.to_owned();
    }

    pub fn get_largest(&self) -> &InternalKey {
        &self.largest
    }

    pub fn set_largest_from_slice(&mut self, key: &[u8]) {
        self.largest.rep = key.to_owned();
    }

    pub fn set_largest(&mut self, key: InternalKey) {
        self.largest = key;
    }
}

impl fmt::Debug for FileMetaData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}, {}, {:?}, {:?}",
            self.file_size,
            self.file_number,
            self.smallest.rep.as_slice(),
            self.largest.rep.as_slice()
        )
    }
}

impl fmt::Display for FileMetaData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "smallest:[{:?}] num:{}", self.smallest, self.file_number)
    }
}

#[cfg(test)]
mod test {
    use crate::db::file_meta::FileMetaData;

    #[test]
    fn test_new() {
        let f = FileMetaData::new();
        assert_eq!(f.allow_seeks(), 1 << 30);
    }
}
