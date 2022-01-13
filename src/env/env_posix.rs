use crate::db::{DError, DResult};
use crate::env::limiter::{max_open_files, Limiter};
use crate::env::lock_table::PosixLockTable;
use crate::env::mmap::MmapReadableFile;
use crate::env::Env;
use crate::env::{RandomAccessFile, SequentialFile, WritableFile};
use hashbrown::HashMap;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::{fs, io};

const DEFAULT_MMAP_LIMIT: i64 = 1000;

pub struct PosixRandomAccessFile {
    inner: Arc<RwLock<BufReader<fs::File>>>,
    fd_limiter: Limiter,
}

impl Drop for PosixRandomAccessFile {
    fn drop(&mut self) {
        self.fd_limiter.release();
    }
}

impl PosixRandomAccessFile {
    pub fn new(f: io::Result<fs::File>, fd_limiter: Limiter) -> DResult<Self> {
        match f {
            Ok(f) => Ok(Self {
                fd_limiter,
                inner: Arc::new(RwLock::new(BufReader::new(f))),
            }),
            Err(err) => Err(DError::CreateEnvFileFailed(
                "PosixRandomAccessFile".parse().unwrap(),
                err.to_string(),
            )),
        }
    }
}

impl RandomAccessFile for PosixRandomAccessFile {
    fn read(&self, offset: usize, result: &mut [u8]) -> DResult<usize> {
        self.inner
            .write()
            .unwrap()
            .seek(SeekFrom::Start(offset as u64))?;
        let r = self.inner.write().unwrap().read(result);
        match r {
            Ok(n) => Ok(n),
            Err(_) => Err(DError::UnexpectedErrorToGetFileMeta),
        }
    }
}

pub struct PosixMmapReadableFile {
    mmap: MmapReadableFile,
}

impl PosixMmapReadableFile {
    pub fn new<P: AsRef<Path>>(path: P) -> DResult<Self> {
        let file = fs::OpenOptions::new().read(true).open(&path)?;
        match MmapReadableFile::new(&file) {
            Ok(mmap) => Ok(PosixMmapReadableFile { mmap }),
            Err(_) => Err(DError::InvalidReadArgument),
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl RandomAccessFile for PosixMmapReadableFile {
    fn read(&self, offset: usize, result: &mut [u8]) -> DResult<usize> {
        self.mmap
            .read(offset, result)
            .map_err(|_| -> DError { DError::InvalidReadArgument })
    }
}

/// From https://doc.rust-lang.org/std/io/struct.BufReader.html
/// It can be excessively inefficient to work directly with a Read instance. For example,
/// every call to read on TcpStream results in a system call. A BufReader<R> performs large,
/// infrequent reads on the underlying Read and maintains an in-memory buffer of the results.
pub struct PosixSequentialFile {
    inner: Arc<RwLock<BufReader<fs::File>>>,
}

impl PosixSequentialFile {
    pub fn new(f: io::Result<fs::File>) -> DResult<Self> {
        match f {
            Ok(f) => Ok(Self {
                inner: Arc::new(RwLock::new(BufReader::new(f))),
            }),
            Err(err) => Err(DError::CreateEnvFileFailed(
                "PosixSequentialFile".parse().unwrap(),
                err.to_string(),
            )),
        }
    }
}

impl SequentialFile for PosixSequentialFile {
    fn read(&self, n: usize, result: &mut [u8]) -> DResult<usize> {
        let ret = self.inner.write().unwrap().read(&mut result[..n]);
        match ret {
            Ok(n) => Ok(n),
            Err(_) => Err(DError::UnexpectedErrorToReadPosixFile),
        }
    }

    fn skip(&self, n: u64) -> DResult<()> {
        self.inner.write().unwrap().seek(SeekFrom::Start(n))?;
        Ok(())
    }
}

// BufWriter<W> can improve the speed of programs that make small and repeated write calls to the
// same file or network socket. It does not help when writing very large amounts at once,
// or writing just one or a few times.
// We use batch write and we need to ensure new files flushed to disk, so use std::File.
pub struct PosixWritableFile {
    inner: Arc<RwLock<BufWriter<fs::File>>>,
}

impl PosixWritableFile {
    // Support open already exists file or create non exist file.
    pub fn new(file: fs::File) -> Self {
        Self {
            inner: Arc::new(RwLock::new(BufWriter::new(file))),
        }
    }

    fn flush_buffer(&self) -> io::Result<()> {
        self.inner.write().unwrap().flush()
    }
}

impl WritableFile for PosixWritableFile {
    fn append(&self, buf: &[u8]) -> DResult<()> {
        let _write_size = buf.len();
        let ret = self.inner.write().unwrap().write_all(buf);
        match ret {
            Ok(_) => Ok(()),
            Err(_) => Err(DError::UnexpectedErrorToWritePosixFile),
        }
    }

    fn flush(&self) -> DResult<()> {
        Ok(self.flush_buffer()?)
    }

    // Ensure that all in-memory data reaches the filesystem before returning.
    fn sync(&self) -> DResult<()> {
        Ok(self.inner.write().unwrap().flush()?)
    }
}

#[derive(Default)]
pub struct PosixEnv {
    locks: PosixLockTable,
    mmap_limiter: Limiter,
    fd_limiter: Limiter,
}

impl PosixEnv {
    pub fn new() -> PosixEnv {
        Self {
            mmap_limiter: Limiter::new(DEFAULT_MMAP_LIMIT),
            fd_limiter: Limiter::new(max_open_files()),
            ..Default::default()
        }
    }
}

impl Env for PosixEnv {
    fn new_random_access_file(&self, file_name: &str) -> DResult<Arc<dyn RandomAccessFile>> {
        let f = fs::OpenOptions::new().read(true).open(file_name);
        if self.mmap_limiter.acquire() {
            return match PosixRandomAccessFile::new(f, self.fd_limiter.clone()) {
                Ok(p) => Ok(Arc::new(p)),
                Err(err) => Err(err),
            };
        }
        defer! {
            self.mmap_limiter.release();
        }
        match PosixMmapReadableFile::new(file_name) {
            Ok(file) => Ok(Arc::new(file)),
            Err(err) => Err(err),
        }
    }

    fn new_writable_file(&self, file_name: &str) -> DResult<Arc<dyn WritableFile>> {
        let f = fs::OpenOptions::new()
            .read(false)
            .write(true)
            .truncate(true)
            .create(true)
            .mode(0o644)
            .open(file_name)
            .unwrap();
        Ok(Arc::new(PosixWritableFile::new(f)))
    }

    fn new_sequential_file(&self, file_name: &str) -> DResult<Arc<dyn SequentialFile>> {
        let f = fs::OpenOptions::new().read(true).open(file_name);
        match PosixSequentialFile::new(f) {
            Ok(p) => Ok(Arc::new(p)),
            Err(err) => Err(err),
        }
    }

    fn new_appendable_file(&self, file_name: &str) -> DResult<Arc<dyn WritableFile>> {
        let f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_name)
            .unwrap();
        Ok(Arc::new(PosixWritableFile::new(f)))
    }

    fn create_dir(&self, name: &str) -> DResult<()> {
        fs::create_dir(name)?;
        Ok(())
    }

    fn delete_dir(&self, name: &str) -> DResult<()> {
        fs::remove_dir(name)?;
        Ok(())
    }

    fn delete_file(&self, file_name: &str) -> DResult<()> {
        fs::remove_file(file_name)?;
        Ok(())
    }

    fn rename_file(&self, from: &str, to: &str) -> DResult<()> {
        fs::rename(from, to)?;
        Ok(())
    }

    fn file_exists(&self, file_name: &str) -> bool {
        fs::metadata(file_name).is_ok()
    }

    fn lock_file(&self, file_name: &str) -> DResult<()> {
        if !self.locks.insert(file_name.to_string()) {
            return Err(DError::CustomError("already held by process"));
        }

        Ok(())
    }

    fn unlock_file(&self, file_name: &str) -> DResult<()> {
        self.locks.remove(file_name);
        Ok(())
    }
}

pub struct MockStorage {
    /// Actually we don't need RwLock,
    /// just make the trait `RandomAccessFile` `SequentialFile` immutable, reduce some lock().unwrap() code.
    content: Arc<RwLock<Vec<u8>>>,
}

impl Default for MockStorage {
    fn default() -> Self {
        Self {
            content: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl RandomAccessFile for MockStorage {
    fn read(&self, offset: usize, result: &mut [u8]) -> DResult<usize> {
        let tmp = self.content.read().unwrap();
        let left = &tmp.as_slice()[offset..];
        let size = left.len();
        if size < result.len() {
            result.copy_from_slice(left);
            return Ok(size);
        }
        result.copy_from_slice(
            // [offset..offset + result.len()] => [offset..][..result.len()]
            // make it readable
            &self.content.read().unwrap().as_slice()[offset..][..result.len()],
        );
        Ok(result.len())
    }
}

impl SequentialFile for MockStorage {
    fn read(&self, n: usize, result: &mut [u8]) -> DResult<usize> {
        let data_len = self.content.read().unwrap().len();
        if n > data_len {
            result[..data_len].copy_from_slice(self.content.read().unwrap().as_slice());
            self.content.write().unwrap().drain(..data_len);
            return Ok(data_len);
        }
        result[..n].copy_from_slice(&self.content.read().unwrap().as_slice()[..n]);
        self.content.write().unwrap().drain(..n);
        Ok(n)
    }

    fn skip(&self, _n: u64) -> DResult<()> {
        unimplemented!()
    }
}

impl WritableFile for MockStorage {
    fn append(&self, buf: &[u8]) -> DResult<()> {
        self.content.write().unwrap().extend_from_slice(buf);
        Ok(())
    }

    fn flush(&self) -> DResult<()> {
        Ok(())
    }
}

impl io::Write for MockStorage {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.content.write().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct TestEnv {
    mock_fs: Arc<RwLock<HashMap<String, Arc<MockStorage>>>>,
}

unsafe impl Send for TestEnv {}

unsafe impl Sync for TestEnv {}

impl Env for TestEnv {
    fn new_random_access_file(&self, file_name: &str) -> DResult<Arc<dyn RandomAccessFile>> {
        return match self.mock_fs.read().unwrap().get(file_name) {
            Some(f) => Ok(Arc::new(MockStorage {
                content: Arc::new(RwLock::new(f.content.read().unwrap().clone())),
            })),
            None => Err(DError::NoSuchFile),
        };
    }

    fn new_writable_file(&self, file_name: &str) -> DResult<Arc<dyn WritableFile>> {
        let storage = Arc::new(MockStorage::default());
        self.mock_fs
            .write()
            .unwrap()
            .insert(file_name.to_string(), storage.clone());
        Ok(storage)
    }

    fn new_sequential_file(&self, file_name: &str) -> DResult<Arc<dyn SequentialFile>> {
        return match self.mock_fs.read().unwrap().get(file_name) {
            Some(f) => Ok(Arc::new(MockStorage {
                content: Arc::new(RwLock::new(f.content.read().unwrap().clone())),
            })),
            None => Err(DError::NoSuchFile),
        };
    }

    fn new_appendable_file(&self, file_name: &str) -> DResult<Arc<dyn WritableFile>> {
        let storage = Arc::new(MockStorage::default());
        self.mock_fs
            .write()
            .unwrap()
            .insert(file_name.to_string(), storage.clone());
        Ok(storage)
    }

    fn new_log_file(&self, _db_name: &str) -> Option<fs::File> {
        None
    }

    fn delete_file(&self, file_name: &str) -> DResult<()> {
        if self.mock_fs.write().unwrap().remove(file_name).is_some() {
            return Ok(());
        }
        Err(DError::NoSuchFile)
    }

    fn rename_file(&self, from: &str, to: &str) -> DResult<()> {
        if self.mock_fs.read().unwrap().contains_key(from) {
            let mut mock = self.mock_fs.write().unwrap();
            let value = mock.remove(from);
            mock.insert(to.to_string(), value.unwrap());
        }
        Ok(())
    }

    fn file_exists(&self, file_name: &str) -> bool {
        self.mock_fs.read().unwrap().contains_key(file_name)
    }

    fn get_children(&self, file_name: &str) -> DResult<Vec<String>> {
        let mut ret = vec![];
        for (k, _) in self.mock_fs.read().unwrap().iter() {
            if k.starts_with(file_name) {
                ret.push(k.clone());
            }
        }
        Ok(ret)
    }

    fn lock_file(&self, _file_name: &str) -> DResult<()> {
        Ok(())
    }

    fn unlock_file(&self, _file_name: &str) -> DResult<()> {
        Ok(())
    }

    fn get_file_size(&self, file_name: &str, size: &mut u64) -> DResult<()> {
        let value = self.mock_fs.read().unwrap().get(file_name).unwrap().clone();
        *size = value.content.read().unwrap().len() as u64;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::env::env_posix::{PosixEnv, TestEnv};
    use crate::env::Env;
    use crate::utils::random::{Random, RandomGenerator};
    use std::cell::RefCell;
    use std::cmp::min;
    use std::rc::Rc;

    const READ_ONLY_FILE_LIMIT: usize = 4;
    const MMAP_LIMIT: usize = 4;

    #[test]
    fn test_posix_sequential_file() {
        let test_file_name = "test_posix_sequential_file".to_string();

        let env = TestEnv::default();
        {
            let writable_file = env.new_writable_file(&test_file_name).unwrap();
            assert!(writable_file.append(b"123").is_ok());
            assert!(writable_file.close().is_ok());
        }
        {
            let f = env.new_sequential_file(&test_file_name).unwrap();
            let mut data = vec![0; 1];
            let ret = f.read(1, &mut data);
            assert!(ret.is_ok());
            assert_eq!(&data, b"1");

            let mut data = vec![0; 2];
            let ret = f.read(2, &mut data);
            assert!(ret.is_ok());
            assert_eq!(&data, b"23");
        }
        {
            let f = env.new_sequential_file(&test_file_name).unwrap();
            let mut data = vec![0; 2];
            let ret = f.read(2, &mut data);
            assert!(ret.is_ok());
            assert_eq!(&data, b"12");
        }

        assert!(env.delete_file(&test_file_name).is_ok());
    }

    #[test]
    fn test_open_on_read() {
        let env = TestEnv::default();
        let test_file = "test_test_open_on_read".to_string();

        // Write some test data to a single file that will be opened |n| times.
        let data = "abcdefghijklmnopqrstuvwxyz";
        let f = env.new_writable_file(&test_file).unwrap();
        f.append(data.as_bytes()).unwrap();

        let num_files = READ_ONLY_FILE_LIMIT + MMAP_LIMIT + 5;
        let mut files = vec![];
        for _ in 0..num_files {
            let random_access_file = env.new_random_access_file(&test_file);
            assert!(random_access_file.is_ok());
            files.push(random_access_file.unwrap());
        }
        let mut result = vec![0; 1];
        for i in 0..num_files {
            assert!(files[i].read(i, &mut result).is_ok());
            assert_eq!(data.as_bytes()[i], result[0]);
        }

        assert!(env.delete_file(&test_file).is_ok());
    }

    fn random_str(rnd: Rc<RefCell<dyn RandomGenerator>>, len: usize, dst: &mut Vec<u8>) {
        dst.resize(len, 0);
        for i in 0..len {
            dst[i] = (' ' as u32 + rnd.borrow_mut().uniform(95)) as u8;
        }
    }

    #[test]
    fn test_read_write() {
        let rnd = Rc::new(RefCell::new(Random::new(301)));

        // Get file to use for testing.
        let test_file = "test_read_write".to_string();
        let env = TestEnv::default();

        let writable_file = env.new_writable_file(&test_file).unwrap();
        // Fill a file with data generated via a sequence of randomly sized writes.
        // 10 MB
        let data_size: usize = 10 * 1024 * 1024;
        let mut data = vec![];
        while data.len() < data_size {
            let len = rnd.borrow_mut().skewed(18);
            let mut r = vec![];
            random_str(rnd.clone(), len as usize, &mut r);
            assert!(writable_file.append(r.as_slice()).is_ok());
            data.extend_from_slice(r.as_slice());
            if rnd.borrow_mut().one_in(10) {
                assert!(writable_file.flush().is_ok());
            }
        }
        assert!(writable_file.sync().is_ok());
        assert!(writable_file.close().is_ok());

        // Read all data using a sequence of randomly sized reads.
        let sequential_file = env.new_sequential_file(&test_file).unwrap();

        let mut read_result = vec![];
        let mut scratch = vec![0; 10 * 1048576];
        while read_result.len() < data.len() {
            let len = min(
                rnd.borrow_mut().skewed(18) as usize,
                data.len() - read_result.len(),
            );
            let ret = sequential_file.read(len, &mut scratch);
            assert!(ret.is_ok());
            let n = ret.unwrap();
            if len > 0 {
                assert!(n > 0);
            }
            assert!(n <= len);
            read_result.extend_from_slice(&scratch[..n]);
        }
        assert_eq!(read_result.as_slice(), data.as_slice());
    }

    #[test]
    fn test_run_immediately() {
        // todo
    }

    #[test]
    fn test_open_non_exist_file() {
        let non_exist_file = "test_open_non_exist_file".to_string();

        let env = PosixEnv::default();
        assert_eq!(env.file_exists(&non_exist_file), false);
        {
            let random_access_file = env.new_random_access_file(&non_exist_file);
            assert!(random_access_file.is_err());
            let e = random_access_file.err().unwrap().to_string();
            assert!(e.contains("PosixRandomAccessFile"));
            assert!(e.contains("No such file or directory"));
        }
        {
            let sequential_file = env.new_sequential_file(&non_exist_file);
            assert!(sequential_file.is_err());
            let e = sequential_file.err().unwrap().to_string();
            assert!(e.contains("PosixSequentialFile"));
            assert!(e.contains("No such file or directory"));
        }
    }

    #[test]
    fn test_reopen_writable_file() {
        let test_file_name = "test_reopen_writable_file".to_string();

        let env = PosixEnv::default();
        let r = env.delete_file(&test_file_name);
        assert!(r.is_err());
        let e = r.err().unwrap().to_string();
        assert!(e.contains("No such file or directory"));
        {
            let writable_file = env.new_writable_file(&test_file_name).unwrap();
            assert!(writable_file.append(b"hello world!").is_ok());
            assert!(writable_file.close().is_ok());
        }
        {
            let writable_file = env.new_writable_file(&test_file_name).unwrap();
            assert!(writable_file.append(b"123").is_ok());
            assert!(writable_file.close().is_ok());
        }

        let result = env.read_file_to_string(&test_file_name).unwrap();
        assert_eq!(result.as_bytes(), b"123");
        assert!(env.delete_file(&test_file_name).is_ok());
    }

    #[test]
    fn test_reopen_appendable_file() {
        // todo
    }
}
