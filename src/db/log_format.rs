/// There are four record types: full, first, middle or last. A multi-chunk
/// journal with fragments has one first chunk, zero or more middle chunks,
/// and one last chunk.
#[derive(Copy, Clone)]
#[non_exhaustive]
pub enum RecordType {
    FullType = 0,
    FirstType = 1,
    MiddleType = 2,
    LastType = 3,
    /// Returned when reader successfully reads the log
    Eof = 4,
    /// Returned whenever we find an invalid physical record.
    /// Currently there are three situations in which this happens:
    /// * The record has an invalid CRC (`read_record_from_file` reports a drop)
    /// * The record is a 0-length record (No drop is reported)
    /// * The record is below constructor's initial_offset (No drop is reported)
    BadRecord = 5,
}

impl From<&u8> for RecordType {
    fn from(v: &u8) -> Self {
        match v {
            0 => RecordType::FullType,
            1 => RecordType::FirstType,
            2 => RecordType::MiddleType,
            3 => RecordType::LastType,
            4 => RecordType::Eof,
            5 => RecordType::BadRecord,
            _ => {
                panic!("Wrong record type");
            }
        }
    }
}

impl Default for RecordType {
    fn default() -> Self {
        RecordType::FullType
    }
}
