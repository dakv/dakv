use memmap::{Mmap, MmapMut, MmapOptions};
use std::io::{Read, Write};

use std::fs::File;
use std::io;

#[allow(unused)]
pub struct MmapWritableFile {
    size: u64,
    offset: usize,
    mmap: MmapMut,
}

#[allow(unused)]
impl MmapWritableFile {
    pub fn new(file: &File, size: u64) -> io::Result<Self> {
        assert!(size > 0);
        file.set_len(size)?;
        let mmap = unsafe { MmapMut::map_mut(file).unwrap() };
        Ok(MmapWritableFile {
            offset: 0,
            mmap,
            size,
        })
    }

    pub fn new_ano(size: usize) -> io::Result<Self> {
        let mmap_ret = MmapMut::map_anon(size);
        mmap_ret.map(|mmap| Self {
            offset: 0,
            mmap,
            size: size as u64,
        })
    }

    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn read(&self, offset: usize, buf: &mut [u8]) -> io::Result<usize> {
        (&self.mmap[offset..]).read(buf)
    }

    pub fn flush(&self) -> Result<(), io::Error> {
        self.mmap.flush()
    }

    pub fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        (&mut self.mmap[..]).write_all(buf)
    }
}

pub struct MmapReadableFile {
    mmap: Mmap,
}

impl MmapReadableFile {
    pub fn new(file: &File) -> io::Result<Self> {
        let mmap = unsafe { MmapOptions::new().map(file)? };
        Ok(MmapReadableFile { mmap })
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn read(&self, offset: usize, buf: &mut [u8]) -> io::Result<usize> {
        (&self.mmap[offset..]).read(buf)
    }
}

#[cfg(test)]
mod tests {
    use crate::env::mmap::{MmapReadableFile, MmapWritableFile};
    use std::io::Write;
    use tempfile;

    #[test]
    fn test_read() {
        let mut tmp_file = tempfile::tempfile().unwrap();
        let data = &[1, 2, 3];
        ignore!(tmp_file.write_all(data));
        let f = MmapReadableFile::new(&tmp_file).unwrap();
        assert_eq!(f.len(), data.len());
        for i in 0..data.len() {
            let mut v = vec![0; data.len() - i];
            ignore!(f.read(i, v.as_mut_slice()));
            assert_eq!(&data[i..], v.as_slice());
        }
    }

    #[test]
    fn test_write() {
        let tmp_file = tempfile::tempfile().unwrap();
        let data = &[1, 2, 3];
        let mut f = MmapWritableFile::new(&tmp_file, 3).unwrap();
        assert_eq!(f.len(), 3);
        let mut v = vec![0; 3];
        f.write_all(data).unwrap();
        f.flush().unwrap();
        ignore!(f.read(0, v.as_mut_slice()));
        assert_eq!(v.as_slice(), data);
    }
}
