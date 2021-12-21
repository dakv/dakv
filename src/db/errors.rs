use std::error;
use std::result;
/// A shortcut to box an error.
#[macro_export]
macro_rules! box_err {
    ($e:expr) => ({
        let e: Box<dyn error::Error + Sync + Send> = format!("[{}:{}]: {}", file!(), line!(),  $e).into();
        e.into()
    });
    ($f:tt, $($arg:expr),+) => ({
        box_err!(format!($f, $($arg),+))
    });
}

quick_error! {
    #[derive(Debug)]
    pub enum DError {
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("Unknown error: {:?}", err)
        }
        // Following is for From other errors.
        Io(err: std::io::Error) {
            from()
            cause(err)
            description(err.description())
            display("Io {}", err)
        }
        PathInvalid {
            description("Path should be a directory")
        }
        NotFound{
            description("Record not found")
        }
        KeyIsDeleted{
            description("Key is deleted")
        }
        // File error
        UnexpectedErrorToGetFileMeta{
            description("Unexpected error to get file meta")
        }
        UnexpectedErrorToReadPosixFile{
            description("Unexpected error to read posix file")
        }
        UnexpectedErrorToWritePosixFile{
            description("Unexpected error to write posix file")
        }
        CreateEnvFileFailed(file: String, err: String) {
            description(err)
            display("Create {} failed {}", file, err)
        }
        ChecksumMismatch{
            description("block checksum mismatch")
        }
        BadBlockType{
            description("Bad block type")
        }
        InvalidReadArgument{
            description("Invalid argument to read file")
        }
        NoSuchFile{
            description("No such file")
        }
        CorruptedKey{
            description(err)
            display("corrupted key")
        }
        RecordNotFound{
            description("record not found")
        }
        CustomError(key: &'static str) {
            description(err)
            display("{}", key)
        }
        CustomErrorStr(key: String) {
            description(err)
            display("{}", key)
        }
    }
}

// fixme clone
impl Clone for DError {
    fn clone(&self) -> Self {
        DError::CustomErrorStr(self.to_string())
    }
}

pub type DResult<T> = result::Result<T, DError>;
