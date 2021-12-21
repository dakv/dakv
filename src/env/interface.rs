use crate::db::{DError, DResult};
use crate::env::{RandomAccessFile, SequentialFile, WritableFile};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, thread};

pub trait Env {
    // can't use AsRef<Path>, the trait `interface::Env` cannot be made into an object
    fn new_random_access_file(&self, file_name: &str) -> DResult<Arc<dyn RandomAccessFile>>;

    fn new_writable_file(&self, file_name: &str) -> DResult<Arc<dyn WritableFile>>;

    fn new_sequential_file(&self, file_name: &str) -> DResult<Arc<dyn SequentialFile>>;

    fn new_appendable_file(&self, file_name: &str) -> DResult<Arc<dyn WritableFile>>;

    fn new_log_file(&self, db_name: &str) -> Option<fs::File> {
        Some(
            fs::OpenOptions::new()
                .read(false)
                .write(true)
                .truncate(true)
                .create(true)
                .open(db_name)
                .unwrap(),
        )
    }

    fn create_dir(&self, _name: &str) -> DResult<()> {
        Ok(())
    }

    fn delete_dir(&self, _name: &str) -> DResult<()> {
        Ok(())
    }

    fn delete_file(&self, _file_name: &str) -> DResult<()> {
        Ok(())
    }

    fn rename_file(&self, _from: &str, _to: &str) -> DResult<()> {
        Ok(())
    }

    fn file_exists(&self, _file_name: &str) -> bool {
        true
    }

    fn get_children(&self, file_name: &str) -> DResult<Vec<String>> {
        let mut ret = vec![];
        for f in fs::read_dir(file_name)? {
            let e = f?;
            if e.file_type()?.is_file() {
                ret.push(e.file_name().into_string().unwrap());
            }
        }
        Ok(ret)
    }

    fn read_file_to_string(&self, file_name: &str) -> DResult<String> {
        let f = self.new_sequential_file(file_name)?;
        let buffer_size = 8192;
        let mut data = vec![];

        loop {
            let mut fragment = vec![0; buffer_size];
            let ret = f.read(buffer_size, &mut fragment);
            if let Ok(n) = ret {
                data.extend_from_slice(&fragment[..n]);
                if n == 0 {
                    break;
                }
            } else {
                break;
            }
        }
        unsafe { Ok(String::from_utf8_unchecked(data)) }
    }

    /// May create the named file if it does not already exist.
    fn lock_file(&self, file_name: &str) -> DResult<()>;

    fn unlock_file(&self, file_name: &str) -> DResult<()>;

    /// Sleep the thread
    fn sleep_for_microseconds(&self, v: u64) {
        let duration = Duration::from_micros(v);
        thread::sleep(duration);
    }

    fn get_file_size(&self, file_name: &str, size: &mut u64) -> DResult<()> {
        match fs::metadata(file_name) {
            Ok(f) => {
                *size = f.len();
                Ok(())
            }
            Err(_) => Err(DError::UnexpectedErrorToGetFileMeta),
        }
    }
}
