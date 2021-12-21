use crate::db::{table_file_name, DResult, ErrorIterator, Options, ReadOptions, Saver};
use crate::env::Env;
use crate::table::table_reader::Table;
use crate::utils::coding::encode_u64_le;
use crate::utils::iter::Iter;
use crate::utils::lru_cache::{Cache, SharedLRUCache};
use slog::info;
use std::mem;
use std::sync::{Arc, Mutex};

pub struct TableCache {
    db: String,
    options: Options,
    cache: Arc<Mutex<dyn Cache<Vec<u8>, Table> + Send + Sync>>,
    env: Arc<dyn Env + Send + Sync>,
}

impl TableCache {
    pub fn new(db: String, options: Options, entries: u64) -> Self {
        Self {
            env: options.env.clone(),
            db,
            options,
            cache: Arc::new(Mutex::new(SharedLRUCache::new(entries as usize))),
        }
    }

    /// Return an iterator for the specified file number (the corresponding
    /// file length must be exactly "file_size" bytes).
    pub fn new_iter(
        &self,
        read_opt: ReadOptions,
        file_number: u64,
        file_size: u64,
    ) -> (Box<dyn Iter>, Option<Table>) {
        let result = self.find_table(file_number, file_size);
        match result {
            Ok(table) => {
                let iter = table.new_iter(read_opt);
                (Box::new(iter), Some(table))
            }
            Err(_) => (Box::new(ErrorIterator::new()), None),
        }
    }

    /// If the file_number table exist in the cache, return.
    /// else read the file, and put it into cache, then return.
    pub fn find_table(&self, file_number: u64, file_size: u64) -> DResult<Table> {
        let mut key = vec![0u8; mem::size_of::<u64>()];
        encode_u64_le(key.as_mut_slice(), file_number);

        let tc = self.cache.lock().unwrap();
        let handle = tc.lookup(&key);

        return if let Some(h) = handle {
            info!(self.options.info_log.as_ref().unwrap(), "table cache hit");
            Ok(h)
        } else {
            info!(self.options.info_log.as_ref().unwrap(), "table cache miss");
            let file_name = table_file_name(self.db.as_str(), file_number);
            let file = self.env.new_random_access_file(&file_name)?;

            let table = Table::open(self.options.clone(), file.clone(), file_size as usize)?;
            tc.insert(key, table.clone());
            Ok(table)
        };
    }

    pub fn get(
        &self,
        read_opt: ReadOptions,
        file_number: u64,
        file_size: u64,
        k: &[u8],
        arg: &mut Saver,
        func: impl FnMut(&mut Saver, &[u8], &[u8]),
    ) -> DResult<()> {
        let table = self.find_table(file_number, file_size)?;
        table.internal_get(read_opt, k, arg, func)?;
        // todo cache_->Release(handle);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::db::FileType;
    use crate::db::Options;
    use crate::env::TestEnv;
    use std::sync::Arc;

    #[test]
    fn test_new_table_cache() {
        let mut opt = Options::default();
        opt.env = Arc::new(TestEnv::default());
        let f = opt
            .env
            .new_writable_file(&format!("test/000001.{}", FileType::TableFile.to_str()))
            .unwrap();
        f.append(vec![0; 100].as_slice()).unwrap();
        // todo
        // let mut tc = TableCache::new("test".to_string(), Arc::new(opt), 1);
        // assert_eq!(tc.cache.read().unwrap().total_charge(), 0);
        // let _table = tc.find_table(1, 100).unwrap();
        // assert_eq!(tc.cache.read().unwrap().total_charge(), 1);
        // let _table = tc.find_table(1, 100).unwrap();
        // assert_eq!(tc.cache.read().unwrap().total_charge(), 1);
    }
}
