use crate::db::file_meta::FileMetaData;
use crate::db::format::SequenceNumber;
use crate::db::key::InternalKey;
use crate::db::{DError, DResult, LEVEL_NUMBER};
use crate::utils::coding::{
    get_length_prefixed_slice, get_varint32, get_varint64, put_length_prefixed_slice, put_varint,
};

/// Tag numbers for serialized VersionEdit. These numbers are written to
/// disk and should not be changed.
enum Tag {
    Comparator = 1,
    LogNumber = 2,
    NextFileNumber = 3,
    LastSequence = 4,
    CompactPointer = 5,
    DeletedFile = 6,
    NewFile = 7,
    // 8 was used for large value refs
    // 9 was used for prev log number
    Unknown = 10,
}

impl From<u32> for Tag {
    fn from(i: u32) -> Self {
        match i {
            1 => Tag::Comparator,
            2 => Tag::LogNumber,
            3 => Tag::NextFileNumber,
            4 => Tag::LastSequence,
            5 => Tag::CompactPointer,
            6 => Tag::DeletedFile,
            7 => Tag::NewFile,
            _ => Tag::Unknown,
        }
    }
}

type FileNumber = u64;

#[derive(Default)]
pub struct VersionEdit {
    comparator_name: String,
    has_comparator: bool,
    new_files: Vec<(i64, FileMetaData)>,
    deleted_files: Vec<(i64, FileNumber)>,
    compact_pointers: Vec<(i64, InternalKey)>,
    has_log_number: bool,
    has_next_file_number: bool,
    has_last_sequence: bool,
    pub(crate) log_number: u64,
    pub(crate) next_file_number: u64,
    pub(crate) last_sequence: SequenceNumber,
}

impl VersionEdit {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.comparator_name.clear();
        self.log_number = 0;
        self.last_sequence = 0;
        self.next_file_number = 0;
        self.has_comparator = false;
        self.has_log_number = false;
        self.has_next_file_number = false;
        self.has_last_sequence = false;
        self.deleted_files.clear();
        self.new_files.clear();
    }

    pub(crate) fn set_comparator_name(&mut self, name: &str) {
        self.has_comparator = true;
        self.comparator_name = String::from(name)
    }

    pub(crate) fn comparator_name(&self) -> &str {
        self.comparator_name.as_str()
    }

    pub(crate) fn has_cmp(&self) -> bool {
        self.has_comparator
    }

    pub(crate) fn set_log_number(&mut self, num: u64) {
        self.has_log_number = true;
        self.log_number = num;
    }

    pub(crate) fn has_log_number(&self) -> bool {
        self.has_log_number
    }

    pub(crate) fn set_next_file(&mut self, num: u64) {
        self.has_next_file_number = true;
        self.next_file_number = num;
    }

    pub(crate) fn has_next_file_number(&self) -> bool {
        self.has_next_file_number
    }

    pub(crate) fn set_last_sequence(&mut self, num: u64) {
        self.has_last_sequence = true;
        self.last_sequence = num;
    }

    pub(crate) fn has_last_sequence(&self) -> bool {
        self.has_last_sequence
    }

    pub(crate) fn decode_from(&mut self, src: &[u8]) -> DResult<()> {
        self.clear();
        let mut input = src;
        let mut msg = "";
        let mut tag = 0;
        let mut s: &[u8] = Default::default();

        let mut level = 0;
        let mut number = 0;
        let mut key = InternalKey::default();

        while msg.is_empty() && get_varint32(&mut input, &mut tag) {
            match tag.into() {
                Tag::Comparator => {
                    if get_length_prefixed_slice(&mut input, &mut s) {
                        self.comparator_name =
                            unsafe { std::str::from_utf8_unchecked(s).to_string() };
                        self.has_comparator = true;
                    } else {
                        msg = "comparator name";
                    }
                }
                Tag::LogNumber => {
                    if get_varint64(&mut input, &mut self.log_number) {
                        self.has_log_number = true;
                    } else {
                        msg = "log number";
                    }
                }
                Tag::NextFileNumber => {
                    if get_varint64(&mut input, &mut self.next_file_number) {
                        self.has_next_file_number = true;
                    } else {
                        msg = "next file number";
                    }
                }
                Tag::LastSequence => {
                    if get_varint64(&mut input, &mut self.last_sequence) {
                        self.has_last_sequence = true;
                    } else {
                        msg = "last sequence";
                    }
                }
                Tag::CompactPointer => {
                    if get_level(&mut input, &mut level) && get_internal_key(&mut input, &mut key)
                    {
                        let tmp = (level, key.clone());
                        self.compact_pointers.push(tmp);
                    } else {
                        msg = "compaction pointer";
                    }
                }
                Tag::DeletedFile => {
                    if get_level(&mut input, &mut level) && get_varint64(&mut input, &mut number) {
                        let tmp = (level, number);
                        self.deleted_files.push(tmp);
                    } else {
                        msg = "deleted file";
                    }
                }
                Tag::NewFile => {
                    let mut file_number = 0;
                    let mut file_size = 0;
                    let mut smallest = InternalKey::default();
                    let mut largest = InternalKey::default();
                    if get_level(&mut input, &mut level)
                        && get_varint64(&mut input, &mut file_number)
                        && get_varint64(&mut input, &mut file_size)
                        && get_internal_key(&mut input, &mut smallest)
                        && get_internal_key(&mut input, &mut largest)
                    {
                        let mut f = FileMetaData::new();
                        f.set_file_number(file_number);
                        f.set_file_size(file_size);
                        f.set_smallest(smallest);
                        f.set_largest(largest);
                        let ss = (level, f);
                        self.new_files.push(ss);
                    } else {
                        msg = "new file entry";
                    }
                }
                _ => {
                    msg = "unknown tag";
                }
            }
        }

        if msg.is_empty() && !input.is_empty() {
            msg = "invalid tag";
        }
        if !msg.is_empty() {
            return Err(DError::CustomError("Invalid version edit"));
        }
        Ok(())
    }

    pub(crate) fn encode_to(&self, s: &mut Vec<u8>) {
        if self.has_comparator {
            put_varint(s, Tag::Comparator as u64);
            put_length_prefixed_slice(s, self.comparator_name().as_bytes());
        }
        if self.has_log_number {
            put_varint(s, Tag::LogNumber as u64);
            put_varint(s, self.log_number);
        }
        if self.has_next_file_number {
            put_varint(s, Tag::NextFileNumber as u64);
            put_varint(s, self.next_file_number);
        }
        if self.has_last_sequence {
            put_varint(s, Tag::LastSequence as u64);
            put_varint(s, self.last_sequence);
        }
        for i in &self.compact_pointers {
            put_varint(s, Tag::CompactPointer as u64);
            put_varint(s, i.0);
            put_length_prefixed_slice(s, i.1.encode());
        }
        for i in &self.deleted_files {
            put_varint(s, Tag::DeletedFile as u64);
            // level
            put_varint(s, i.0);
            // file number
            put_varint(s, i.1);
        }

        for i in &self.new_files {
            let f = &i.1;
            put_varint(s, Tag::NewFile as u64);
            put_varint(s, i.0);
            put_varint(s, f.get_file_number());
            put_varint(s, f.get_file_size());
            put_length_prefixed_slice(s, f.get_smallest().encode());
            put_length_prefixed_slice(s, f.get_largest().encode());
        }
    }

    pub(crate) fn set_compact_pointer(&mut self, level: i64, key: InternalKey) {
        self.compact_pointers.push((level, key));
    }

    pub(crate) fn get_compact_pointers(&self) -> &Vec<(i64, InternalKey)> {
        &self.compact_pointers
    }

    pub(crate) fn add_files(
        &mut self,
        level: i64,
        file: u64,
        file_size: u64,
        smallest: &InternalKey,
        largest: &InternalKey,
    ) {
        let mut f = FileMetaData::new();
        f.set_file_number(file);
        f.set_file_size(file_size);
        f.set_largest(largest.clone());
        f.set_smallest(smallest.clone());
        self.new_files.push((level, f));
    }

    pub(crate) fn get_new_files(&self) -> &Vec<(i64, FileMetaData)> {
        &self.new_files
    }

    pub(crate) fn delete_files(&mut self, level: i64, file: u64) {
        self.deleted_files.push((level, file));
    }

    pub(crate) fn get_delete_files(&self) -> &Vec<(i64, FileNumber)> {
        &self.deleted_files
    }
}

fn get_level(input: &mut &[u8], level: &mut i64) -> bool {
    let mut v = 0;
    if get_varint32(input, &mut v) && v < LEVEL_NUMBER as u32 {
        *level = v as i64;
        true
    } else {
        false
    }
}

fn get_internal_key(input: &mut &[u8], dst: &mut InternalKey) -> bool {
    let mut s: &[u8] = &[];
    if get_length_prefixed_slice(input, &mut s) {
        dst.decode(s);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod test {
    use crate::db::format::ValueType;
    use crate::db::key::InternalKey;
    use crate::db::version_edit::VersionEdit;

    fn test_encode_decode(edit: &VersionEdit) {
        let encoded = &mut vec![];
        let encoded2 = &mut vec![];

        edit.encode_to(encoded);

        let mut parsed = VersionEdit::new();
        parsed.decode_from(encoded.as_slice()).unwrap();
        parsed.encode_to(encoded2);
        assert_eq!(encoded.as_slice(), encoded2.as_slice());
    }

    #[test]
    fn test_version_edit() {
        let big = 1u64.wrapping_shl(50);
        assert_eq!(big, 1125899906842624);

        let mut edit = VersionEdit::new();
        for i in 0..4 {
            edit.add_files(
                3,
                big + 300 + i,
                big + 400 + i,
                &InternalKey::new(b"foo", big + 500 + i, ValueType::TypeValue),
                &InternalKey::new(b"zoo", big + 600 + i, ValueType::TypeDeletion),
            );
            edit.delete_files(4, big + 700 + i);
            edit.set_compact_pointer(
                i as i64,
                InternalKey::new(b"x", big + 900 + i, ValueType::TypeValue),
            )
        }

        edit.set_comparator_name("foo");
        edit.set_log_number(big + 100);
        edit.set_next_file(big + 200);
        edit.set_last_sequence(big + 1000);
        test_encode_decode(&edit);
    }
}
