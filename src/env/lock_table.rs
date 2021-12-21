use std::collections::HashSet;
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct PosixLockTable {
    locked_files: Arc<Mutex<HashSet<String>>>,
}

impl PosixLockTable {
    pub fn insert(&self, file_name: String) -> bool {
        self.locked_files.lock().unwrap().insert(file_name)
    }

    pub fn remove(&self, file_name: &str) -> bool {
        self.locked_files.lock().unwrap().remove(file_name)
    }
}

#[cfg(test)]
mod test {
    use crate::env::lock_table::PosixLockTable;

    #[test]
    fn test_lock_table() {
        let table = PosixLockTable::default();
        assert_eq!(table.insert(String::from("1")), true);
        assert_eq!(table.insert(String::from("1")), false);
        assert_eq!(table.remove(&String::from("1")), true);
        assert_eq!(table.remove(&String::from("1")), false);
    }
}
