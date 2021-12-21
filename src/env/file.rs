use crate::db::DResult;

/// RandomAccessFile does not store the offset of last read.
pub trait RandomAccessFile {
    /// Read up to "n" bytes from the file starting at "offset".
    /// "result[0..n-1]" may be written by this routine. Sets `result`
    /// to the data that was read (including if fewer than "n" bytes were
    /// successfully read).
    /// If an error was encountered, returns a non-OK status.
    /// Safe for concurrent use by multiple threads.
    fn read(&self, offset: usize, result: &mut [u8]) -> DResult<usize>;
}

/// SequentialFile stores the offset position of last read.
pub trait SequentialFile {
    /// Read up to "n" bytes from the file.  "result[0..n-1]" may be
    /// written by this routine. Sets `result` to the data that was
    /// read (including if fewer than "n" bytes were successfully read).
    /// If an error was encountered, returns a non-OK status.
    /// todo maybe remove n
    fn read(&self, n: usize, result: &mut [u8]) -> DResult<usize>;
    /// Skip "n" bytes from the file. This is guaranteed to be no
    /// slower that reading the same data, but may be faster.
    /// If end of file is reached, skipping will stop at the end of the
    /// file, and Skip will return OK.
    fn skip(&self, n: u64) -> DResult<()>;
}

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
pub trait WritableFile {
    fn append(&self, buf: &[u8]) -> DResult<()>;
    fn flush(&self) -> DResult<()>;
    fn sync(&self) -> DResult<()> {
        Ok(())
    }
    fn close(&self) -> DResult<()> {
        self.flush()?;
        Ok(())
    }
}
