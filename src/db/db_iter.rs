use crate::db::database::Inner;
use crate::db::format::{SequenceNumber, ValueType, VALUE_TYPE_FOR_SEEK};
use crate::db::key::{
    append_internal_key, extract_user_key, parse_internal_key, ParsedInternalKey,
};
use crate::db::{DError, DResult, READ_BYTES_PERIOD};
use crate::utils::cmp::Comparator;
use crate::utils::iter::Iter;
use crate::utils::random::{Random, RandomGenerator};
use std::cell::RefCell;
use std::sync::Arc;

/// Which direction is the iterator currently moving?
/// (1) When moving forward, the internal iterator is positioned at
///     the exact entry that yields self.key(), self.value()
/// (2) When moving backwards, the internal iterator is positioned
///     just before all entries whose user key == self.key().
#[derive(PartialOrd, PartialEq, Debug)]
enum Direction {
    Forward,
    Reverse,
}

/// ```text
///                        ┌──────────┐
///                        │  DBIter  │
///                        └──────────┘
///                             ▼
///                       ┌─────────────┐
///        ┌──────────────┤ MergingIter ├───────────┐
///        │              └─────────────┘           │
///        ▼                    ▼                   ▼
///   ┌─────────┐       ┌───────────────┐  ┌──────────────┐
///   │ MemIter │       │ TwoLevelIter  │  │ TwoLevelIter │
///   │         │  ┌────┤   (level 0)   │  │ (level 1-n)  ├──────────┐
///   └─────────┘  │    └───────────────┘  └──────────────┘          │
///                ▼             ▼                 ▼                 ▼
/// ┌────────────────┐ ┌───────────────┐ ┌───────────────────┐  ┌───────────────┐
/// │ index:BlockIter│ │ data:BlockIter│ │  LevelFileNumIter │  │ TwoLevelIter  │
/// └────────────────┘ └───────────────┘ │      index        │  │    data       │
///                                      └───────────────────┘  └───────────────┘
///                                                                 ▼         ▼
///                                                   ┌─────────────────┐ ┌──────────────┐
///                                                   │ index:BlockIter │ │data:BlockIter│
///                                                   └─────────────────┘ └──────────────┘
///```
#[allow(clippy::upper_case_acronyms)]
pub struct DBIter {
    user_cmp: Arc<dyn Comparator>,
    iter: Box<dyn Iter>,
    sequence: SequenceNumber,
    rnd: Box<dyn RandomGenerator>,
    valid: bool,
    direction: Direction,
    /// current key when direction == Reverse
    saved_key: Vec<u8>,
    /// current raw value when direction == Reverse
    saved_value: Vec<u8>,
    bytes_until_read_sampling: RefCell<usize>,
    status: RefCell<DResult<()>>,
    inner: Arc<Inner>,
}

impl DBIter {
    pub fn new(
        inner: Arc<Inner>,
        user_comparator: Arc<dyn Comparator>,
        iter: Box<dyn Iter>,
        sequence: SequenceNumber,
        seed: u32,
    ) -> Self {
        let s = Self {
            valid: false,
            saved_key: Default::default(),
            saved_value: Default::default(),
            direction: Direction::Forward,
            user_cmp: user_comparator,
            iter,
            sequence,
            inner,
            rnd: Box::new(Random::new(seed)),
            bytes_until_read_sampling: RefCell::new(0),
            status: RefCell::new(Ok(())),
        };
        *s.bytes_until_read_sampling.borrow_mut() = s.random_compaction_period();
        s
    }

    // Picks the number of bytes that can be read until a compaction is scheduled.
    fn random_compaction_period(&self) -> usize {
        self.rnd.uniform(2 * READ_BYTES_PERIOD) as usize
    }

    fn parse_key<'a>(&'a self, ikey: &mut ParsedInternalKey<'a>) -> bool {
        let bytes_read = self.iter.key().len() + self.iter.value().len();

        while *self.bytes_until_read_sampling.borrow() < bytes_read {
            *self.bytes_until_read_sampling.borrow_mut() += self.random_compaction_period();
            self.inner.record_read_sample(self.iter.key());
        }

        assert!(*self.bytes_until_read_sampling.borrow() >= bytes_read);
        *self.bytes_until_read_sampling.borrow_mut() -= bytes_read;
        if !parse_internal_key(self.iter.key(), ikey) {
            *self.status.borrow_mut() =
                Err(DError::CustomError("corrupted internal key in DBIter"));
            false
        } else {
            true
        }
    }

    fn clear_saved_value(&mut self) {
        self.saved_value.clear();
    }

    fn clear_saved_key(&mut self) {
        self.saved_key.clear();
    }

    #[allow(unused_labels)]
    fn find_next_user_entry(&mut self, s: bool) {
        let mut skip = s;
        assert!(self.iter.valid());
        assert_eq!(self.direction, Direction::Forward);

        'do_while: while {
            let mut ikey = ParsedInternalKey::default();
            if self.parse_key(&mut ikey) && ikey.sequence() <= self.sequence {
                match ikey.value_type() {
                    ValueType::TypeDeletion => {
                        // Arrange to skip all upcoming entries for this key since
                        // they are hidden by this deletion.
                        self.saved_key = Vec::from(ikey.user_key());
                        skip = true;
                    }
                    ValueType::TypeValue => {
                        if skip && self.user_cmp.le(ikey.user_key(), self.saved_key.as_slice()) {
                        } else {
                            self.valid = true;
                            self.clear_saved_key();
                            return;
                        }
                    }
                }
            }
            self.iter.next();
            self.iter.valid()
        } {}
        self.clear_saved_key();
        self.valid = false;
    }

    fn find_prev_user_entry(&mut self) {
        assert_eq!(self.direction, Direction::Reverse);

        let mut value_type = ValueType::TypeDeletion;
        if self.iter.valid() {
            'do_while: while {
                let mut ikey = ParsedInternalKey::default();
                if self.parse_key(&mut ikey) && ikey.sequence() <= self.sequence {
                    if value_type != ValueType::TypeDeletion
                        && self.user_cmp.lt(ikey.user_key(), self.saved_key.as_slice())
                    {
                        break 'do_while;
                    }
                    value_type = ikey.value_type();
                    if value_type == ValueType::TypeDeletion {
                        self.clear_saved_key();
                        self.clear_saved_value();
                    } else {
                        if self.saved_value.len() > self.iter.value().len() + 1048576 {
                            self.clear_saved_value();
                        }
                        self.save_key();
                        self.saved_value = self.iter.value().to_vec();
                    }
                }
                self.iter.prev();
                self.iter.valid()
            } {}
        }

        if value_type == ValueType::TypeDeletion {
            self.valid = false;
            self.clear_saved_key();
            self.clear_saved_value();
            self.direction = Direction::Forward;
        } else {
            self.valid = true;
        }
    }

    fn save_key(&mut self) {
        self.clear_saved_key();
        self.saved_key
            .extend_from_slice(extract_user_key(self.iter.key()));
    }

    fn find_next(&mut self) {
        if self.iter.valid() {
            self.find_next_user_entry(false);
        } else {
            self.valid = false;
        }
    }
}

impl Iter for DBIter {
    fn valid(&self) -> bool {
        self.valid
    }

    fn seek_to_first(&mut self) {
        self.direction = Direction::Forward;
        self.clear_saved_value();
        self.iter.seek_to_first();
        self.find_next();
    }

    fn seek_to_last(&mut self) {
        self.direction = Direction::Reverse;
        self.clear_saved_value();
        self.iter.seek_to_last();
        self.find_prev_user_entry();
    }

    fn seek(&mut self, target: &[u8]) {
        self.direction = Direction::Forward;
        self.clear_saved_value();
        self.clear_saved_key();

        append_internal_key(
            &mut self.saved_key,
            &ParsedInternalKey::new(target, self.sequence, VALUE_TYPE_FOR_SEEK),
        );
        self.iter.seek(self.saved_key.as_slice());
        self.find_next();
    }

    fn next(&mut self) {
        assert!(self.valid);
        if self.direction == Direction::Reverse {
            self.direction = Direction::Forward;
            // iter is pointing just before the entries for self.key(),
            // so advance into the range of entries for self.key() and then
            // use the normal skipping code below.
            if !self.iter.valid() {
                self.iter.seek_to_first();
            } else {
                self.iter.next();
            }
            if !self.iter.valid() {
                self.valid = false;
                self.clear_saved_key();
                return;
            }
        } else {
            // Store in saved_key the current key so we skip it below.
            self.save_key();
        }

        self.find_next_user_entry(true);
    }

    fn prev(&mut self) {
        assert!(self.valid);

        if self.direction == Direction::Forward {
            // iter is pointing at the current entry.  Scan backwards until
            // the key changes so we can use the normal reverse scanning code.
            assert!(self.iter.valid());
            self.save_key();
            loop {
                self.iter.prev();
                if !self.iter.valid() {
                    self.valid = false;
                    self.clear_saved_key();
                    self.clear_saved_value();
                    return;
                }
                let t = extract_user_key(self.iter.key());
                if self.user_cmp.lt(t, self.saved_key.as_slice()) {
                    break;
                }
            }
            self.direction = Direction::Reverse;
        }
        self.find_prev_user_entry();
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid);
        if self.direction == Direction::Forward {
            extract_user_key(self.iter.key())
        } else {
            &self.saved_key
        }
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid);
        if self.direction == Direction::Forward {
            self.iter.value()
        } else {
            &self.saved_value
        }
    }

    fn status(&self) -> DResult<()> {
        if self.status.borrow().is_ok() {
            self.iter.status()
        } else {
            let tmp = self.status.borrow();
            tmp.clone()
        }
    }
}
