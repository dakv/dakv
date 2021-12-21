use crate::db::errors::{DError, DResult};
use crate::env::Env;
use std::path::Path;
use std::sync::Arc;
use std::{fmt, u64};

#[allow(clippy::enum_variant_names)]
#[derive(PartialOrd, PartialEq)]
pub enum FileType {
    LogFile,
    TableFile,
    TempFile,
    LockFile,
    DescriptorFile,
    CurrentFile,
    InfoLogFile,
    OldInfoLogFile,
}

impl FileType {
    pub fn to_str(&self) -> &'static str {
        match *self {
            FileType::LogFile => "log",
            FileType::TableFile => "ddb",
            FileType::TempFile => "tmp",
            FileType::LockFile => "LOCK",
            FileType::DescriptorFile => "MANIFEST",
            FileType::CurrentFile => "CURRENT",
            FileType::InfoLogFile => "LOG",
            FileType::OldInfoLogFile => "LOG.old",
        }
    }
}

impl fmt::Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_str())
    }
}

/// Parse file name and get FileType
impl From<u64> for FileType {
    fn from(v: u64) -> Self {
        match v {
            0 => FileType::LogFile,
            _ => panic!("Cannot convert to FileType"),
        }
    }
}

/// generate file name, format {db_name}/{number}.{suffix}
fn make_file_name(db_name: &str, number: u64, suffix: FileType) -> String {
    assert!(number > 0);
    let f_name = format!("{:06}.{}", number, suffix.to_str());
    let target = Path::new(db_name).join(f_name);
    target.to_str().unwrap().to_string()
}

pub(crate) fn log_file_name(db_name: &str, number: u64) -> String {
    make_file_name(db_name, number, FileType::LogFile)
}

pub fn table_file_name(db_name: &str, number: u64) -> String {
    make_file_name(db_name, number, FileType::TableFile)
}

pub(crate) fn temp_file_name(db_name: &str, number: u64) -> String {
    make_file_name(db_name, number, FileType::TempFile)
}

pub(crate) fn manifest_file_name(db_name: &str, number: u64) -> String {
    assert!(number > 0);
    format!("{}/MANIFEST-{:06}", db_name, number)
}

pub(crate) fn info_log_file(db_name: &str) -> String {
    let target = Path::new(db_name).join(FileType::InfoLogFile.to_str());
    target.to_str().unwrap().to_string()
}

pub(crate) fn old_info_log_file(db_name: &str) -> String {
    let target = Path::new(db_name).join(FileType::OldInfoLogFile.to_str());
    target.to_str().unwrap().to_string()
}

pub(crate) fn current_file_name(db_name: &str) -> String {
    format!("{}/CURRENT", db_name)
}

pub(crate) fn lock_file_name(db_name: &str) -> String {
    format!("{}/LOCK", db_name)
}

pub fn do_write_string_to_file(
    env: Arc<dyn Env + Send + Sync>,
    contents: String,
    file: &str,
    should_sync: bool,
) -> DResult<()> {
    let f = env.new_writable_file(file)?;
    f.append(contents.as_bytes())?;
    if should_sync {
        // This is intended for use cases that must synchronize content, but don't need the metadata
        // on disk. The goal of this method is to reduce disk operations.
        f.sync()?;
    }
    Ok(())
}

pub fn do_write_string_to_file_sync(
    env: Arc<dyn Env + Send + Sync>,
    s: String,
    file: &str,
) -> DResult<()> {
    do_write_string_to_file(env, s, file, true)
}

pub fn set_current_file(
    env: Arc<dyn Env + Send + Sync>,
    db_name: &str,
    descriptor_number: u64,
) -> DResult<()> {
    let manifest = manifest_file_name(db_name, descriptor_number);
    let contents = manifest;

    // Remove leading "dbname/" and add newline to manifest file name
    let contents = contents
        .strip_prefix(format!("{}/", db_name).as_str())
        .unwrap();

    let tmp = temp_file_name(db_name, descriptor_number);
    match do_write_string_to_file_sync(env.clone(), format!("{}\n", contents), &tmp) {
        Ok(_) => env.rename_file(&tmp, &current_file_name(db_name)),
        Err(_) => {
            env.delete_file(&tmp)?;
            Err(DError::CustomError("set CURRENT file failed"))
        }
    }
}

pub(crate) fn parse_file_name(filename: &str, num: &mut u64, file_type: &mut FileType) -> bool {
    if filename == FileType::CurrentFile.to_str() {
        *num = 0;
        *file_type = FileType::CurrentFile;
    } else if filename == FileType::LockFile.to_str() {
        *num = 0;
        *file_type = FileType::LockFile;
    } else if filename == "LOG" || filename == "LOG.old" {
        *num = 0;
        *file_type = FileType::InfoLogFile;
    } else if filename.starts_with("MANIFEST-") {
        let mut number = 0;
        let mut f = filename.to_string();
        f.drain(0.."MANIFEST-".len());
        if !consume_decimal_num(&mut f, &mut number) {
            return false;
        }
        if !f.is_empty() {
            return false;
        }
        *num = number;
        *file_type = FileType::DescriptorFile;
    } else {
        // 000002.log
        let mut number = 0;
        let mut f = filename.to_string();
        if !consume_decimal_num(&mut f, &mut number) {
            return false;
        }
        if f.as_str() == format!(".{}", FileType::LogFile).as_str() {
            *file_type = FileType::LogFile;
        } else if f.as_str() == format!(".{}", FileType::TableFile).as_str() {
            *file_type = FileType::TableFile;
        } else if f.as_str() == format!(".{}", FileType::TempFile).as_str() {
            *file_type = FileType::TempFile;
        } else {
            return false;
        }
        *num = number;
    }
    true
}

pub fn consume_decimal_num(rest: &mut String, num: &mut u64) -> bool {
    let max_u64 = u64::MAX;
    let last_digit_of_max_u64 = (max_u64 % 10) as u8 + b'0';

    let mut value = 0;
    let mut read_len = 0;

    for i in rest.as_bytes() {
        if *i < b'0' || *i > b'9' {
            break;
        }
        if value > max_u64 / 10 || (value == max_u64 / 10 && *i > last_digit_of_max_u64) {
            return false;
        }
        value = (value * 10) + (*i - b'0') as u64;
        read_len += 1;
    }
    *num = value;
    rest.drain(..read_len);
    read_len != 0
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_make_log_name() {
        assert_eq!(
            "test/000002.log".to_string(),
            log_file_name(&"test".to_string(), 2)
        );
    }

    #[test]
    fn test_make_table_name() {
        assert_eq!(
            format!("test/000002.{}", FileType::TableFile.to_str()),
            table_file_name(&"test".to_string(), 2)
        );
    }

    #[test]
    fn test_make_temp_name() {
        assert_eq!(
            "test/000002.tmp".to_string(),
            temp_file_name(&"test".to_string(), 2)
        );
    }

    #[test]
    fn test_descriptor_file_name() {
        assert_eq!(
            "test/MANIFEST-000002".to_string(),
            manifest_file_name(&"test".to_string(), 2)
        );
    }

    #[test]
    fn test_() {
        let mut num = 0;
        let mut s = String::from("00002.log");
        assert!(consume_decimal_num(&mut s, &mut num));
        assert_eq!(num, 2);
        assert_eq!(s, String::from(".log"));
    }
}
