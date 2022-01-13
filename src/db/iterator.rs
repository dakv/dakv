use crate::db::file_meta::FileMetaData;
use crate::db::version_manager::find_file;
use crate::db::{DResult, ReadOptions};
use crate::utils::cmp::{Comparator, InternalKeyComparator};
use crate::utils::coding::encode_u64_le;
use crate::utils::iter::Iter;
use std::cmp::Ordering;
use std::sync::Arc;

pub struct EmptyIterator;

impl EmptyIterator {
    pub fn new() -> Self {
        Self
    }
}

impl Iter for EmptyIterator {
    fn valid(&self) -> bool {
        false
    }

    fn seek_to_first(&mut self) {}

    fn seek_to_last(&mut self) {}

    fn seek(&mut self, _target: &[u8]) {}

    fn next(&mut self) {
        unimplemented!()
    }

    fn prev(&mut self) {
        unimplemented!()
    }

    fn key(&self) -> &[u8] {
        unimplemented!()
    }

    fn value(&self) -> &[u8] {
        unimplemented!()
    }
}

// A internal wrapper class with an interface similar to Iterator that
// caches the valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// cache locality.
#[derive(Default)]
struct IteratorWrapper {
    valid: bool,
    // BlockIter or LevelFileNumIterator or SkipListIter
    iter: Option<Box<dyn Iter>>,
    key: Vec<u8>,
}

impl IteratorWrapper {
    pub fn new(iter: Option<Box<dyn Iter>>) -> Self {
        let mut s = Self {
            valid: false,
            iter: None,
            key: Default::default(),
        };
        s.set(iter);
        s
    }

    pub fn update(&mut self) {
        self.valid = self.iter.as_ref().unwrap().valid();
        if self.valid {
            self.key = self.iter.as_ref().unwrap().key().to_vec();
        }
    }

    pub fn set(&mut self, iter: Option<Box<dyn Iter>>) {
        self.iter = iter;
        if self.iter.is_none() {
            self.valid = false
        } else {
            self.update();
        }
    }
}

impl Iter for IteratorWrapper {
    fn valid(&self) -> bool {
        self.valid
    }

    fn seek_to_first(&mut self) {
        self.iter.as_mut().unwrap().seek_to_first();
        self.update();
    }

    fn seek_to_last(&mut self) {
        self.iter.as_mut().unwrap().seek_to_last();
        self.update();
    }

    fn seek(&mut self, target: &[u8]) {
        self.iter.as_mut().unwrap().seek(target);
        self.update();
    }

    fn next(&mut self) {
        self.iter.as_mut().unwrap().next();
        self.update();
    }

    fn prev(&mut self) {
        self.iter.as_mut().unwrap().prev();
        self.update();
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        &self.key
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid());
        self.iter.as_ref().unwrap().value()
    }

    fn status(&self) -> DResult<()> {
        self.iter.as_ref().unwrap().status()
    }
}

// fixme [trait aliases are experimental]
// trait two_level_fn = FnMut(
//     ReadOptions,
//     &[u8],
// ) -> Box<dyn Iter>;

pub struct TwoLevelIter<F>
where
    F: FnMut(ReadOptions, &[u8]) -> Box<dyn Iter>,
{
    index_iter: IteratorWrapper,
    data_iter: IteratorWrapper,
    data_block_handle: Vec<u8>,
    read_opt: ReadOptions,
    block_fn: F,
    // use Options and RandomAccessFile to replace arg_
    status: DResult<()>,
}

impl<F> TwoLevelIter<F>
where
    F: FnMut(ReadOptions, &[u8]) -> Box<dyn Iter>,
{
    /// Return a new two level iterator.  A two-level iterator contains an
    /// index iterator whose values point to a sequence of blocks where
    /// each block is itself a sequence of key,value pairs.  The returned
    /// two-level iterator yields the concatenation of all key/value pairs
    /// in the sequence of blocks.  Takes ownership of "index_iter" and
    /// will delete it when no longer needed.
    ///
    /// Uses a supplied function to convert an index_iter value into
    /// an iterator over the contents of the corresponding block.
    pub fn new(iter: Box<dyn Iter>, read_opt: ReadOptions, f: F) -> Self {
        Self {
            index_iter: IteratorWrapper::new(Some(iter)),
            data_iter: IteratorWrapper::new(None),
            data_block_handle: Default::default(),
            read_opt,
            block_fn: f,
            status: Ok(()),
        }
    }

    pub fn init_data_block(&mut self) {
        if !self.index_iter.valid() {
            self.set_data_iterator(None);
        } else {
            let handle = self.index_iter.value();
            if self.data_iter.iter.is_some() && handle == self.data_block_handle {
                // data_iter_ is already constructed with this iterator, so
                // no need to change anything
            } else {
                let iter = (self.block_fn)(self.read_opt.clone(), handle);
                self.data_block_handle = handle.to_vec();
                self.set_data_iterator(Some(iter));
            }
        }
    }

    fn save_error(&mut self, s: DResult<()>) {
        if self.status.is_ok() && s.is_err() {
            self.status = s;
        }
    }

    fn set_data_iterator(&mut self, data_iter: Option<Box<dyn Iter>>) {
        if self.data_iter.iter.is_some() {
            self.save_error(self.data_iter.status())
        }
        self.data_iter.set(data_iter)
    }

    fn skip_empty_data_blocks_forward(&mut self) {
        while self.data_iter.iter.is_none() || !self.data_iter.valid() {
            // Move to next block
            if !self.index_iter.valid() {
                self.set_data_iterator(None);
                return;
            }
            self.index_iter.next();
            self.init_data_block();
            if self.data_iter.iter.is_some() {
                self.data_iter.seek_to_first();
            }
        }
    }

    fn skip_empty_data_blocks_backward(&mut self) {
        while self.data_iter.iter.is_none() || !self.data_iter.valid() {
            // Move to next block
            if !self.index_iter.valid() {
                self.set_data_iterator(None);
                return;
            }
            self.index_iter.prev();
            self.init_data_block();
            if self.data_iter.iter.is_some() {
                self.data_iter.seek_to_last();
            }
        }
    }
}

impl<F> Iter for TwoLevelIter<F>
where
    F: FnMut(ReadOptions, &[u8]) -> Box<dyn Iter>,
{
    fn valid(&self) -> bool {
        self.data_iter.valid()
    }

    fn seek_to_first(&mut self) {
        self.index_iter.seek_to_first();
        self.init_data_block();
        if self.data_iter.iter.is_some() {
            self.data_iter.seek_to_first();
        }
        self.skip_empty_data_blocks_forward();
    }

    fn seek_to_last(&mut self) {
        self.index_iter.seek_to_last();
        self.init_data_block();
        if self.data_iter.iter.is_some() {
            self.data_iter.seek_to_last();
        }
        self.skip_empty_data_blocks_backward();
    }

    fn seek(&mut self, target: &[u8]) {
        self.index_iter.seek(target);

        self.init_data_block();
        if self.data_iter.iter.is_some() {
            self.data_iter.seek(target);
        }
        self.skip_empty_data_blocks_forward();
    }

    fn next(&mut self) {
        assert!(self.valid());
        self.data_iter.next();
        self.skip_empty_data_blocks_forward();
    }

    fn prev(&mut self) {
        assert!(self.valid());
        self.data_iter.prev();
        self.skip_empty_data_blocks_backward();
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        self.data_iter.key()
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid());
        self.data_iter.value()
    }

    fn status(&self) -> DResult<()> {
        if self.index_iter.status().is_err() {
            self.index_iter.status()
        } else if self.data_iter.iter.is_some() && self.data_iter.status().is_err() {
            self.data_iter.status()
        } else {
            self.status.clone()
        }
    }
}

#[derive(Eq, PartialEq)]
enum Direction {
    Forward,
    Reverse,
}

/// Combine multiple iterators into one.
/// Maybe with mem_table & immutable_mem_table & table file.
/// ```text
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
/// │     index      │ │     data      │ │      index        │  │    data       │
/// │    BlockIter   │ │    BlockIter  | │  LevelFileNumIter │  │ TwoLevelIter  │
/// └────────────────┘ └───────────────┘ └───────────────────┘  └───────────────┘
///                                                                 ▼         ▼
///                                                   ┌─────────────────┐ ┌──────────────┐
///                                                   │ index:BlockIter │ │data:BlockIter│
///                                                   └─────────────────┘ └──────────────┘
///```
pub struct MergingIterator {
    cmp: Arc<dyn Comparator>,
    n: usize,
    direction: Direction,
    current: Option<usize>,
    children: Vec<IteratorWrapper>,
}

#[allow(clippy::if_same_then_else)]
fn check_child_and_swap(
    children: &[IteratorWrapper],
    ret: &mut Option<usize>,
    c: usize,
    order: Ordering,
    cmp: Arc<dyn Comparator>,
) {
    let child = &children[c];
    if child.valid() {
        if let Some(i) = ret {
            if cmp.compare(child.key(), children[*i].key()) == order {
                *ret = Some(c);
            }
        } else {
            *ret = Some(c);
        }
    }
}

impl MergingIterator {
    fn new(cmp: Arc<dyn Comparator>, children: Vec<Box<dyn Iter>>, n: usize) -> Self {
        let mut c = vec![];
        for child in children.into_iter().take(n) {
            let mut it = IteratorWrapper::default();
            it.set(Some(child));
            c.push(it);
        }
        MergingIterator {
            cmp,
            n,
            direction: Direction::Forward,
            current: None,
            children: c,
        }
    }

    // Find the boundary iterator and set current
    /// Use the newest Internal Key.
    fn find_smallest(&mut self) {
        let mut smallest = None;
        for child in 0..self.n {
            check_child_and_swap(
                &self.children,
                &mut smallest,
                child,
                Ordering::Less,
                self.cmp.clone(),
            );
        }
        self.current = smallest;
    }

    // Find the boundary iterator and set current
    fn find_largest(&mut self) {
        let mut largest = None;
        for child in (0..self.n).rev() {
            check_child_and_swap(
                &self.children,
                &mut largest,
                child,
                Ordering::Greater,
                self.cmp.clone(),
            );
        }
        self.current = largest;
    }
}

fn seek_and_go_to_next(
    child: &mut IteratorWrapper,
    curr: &IteratorWrapper,
    cmp: Arc<dyn Comparator>,
) {
    child.seek(curr.key());
    if child.valid() && cmp.eq(curr.key(), child.key()) {
        child.next();
    }
}

fn seek_and_go_to_prev(
    child: &mut IteratorWrapper,
    curr: &IteratorWrapper,
    _cmp: Arc<dyn Comparator>,
) {
    child.seek(curr.key());
    if child.valid() {
        child.prev();
    } else {
        child.seek_to_last()
    }
}

impl Iter for MergingIterator {
    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn seek_to_first(&mut self) {
        for i in 0..self.n {
            self.children[i].seek_to_first();
        }
        self.find_smallest();
        self.direction = Direction::Forward;
    }

    fn seek_to_last(&mut self) {
        for i in 0..self.n {
            self.children[i].seek_to_last();
        }
        self.find_largest();
        self.direction = Direction::Reverse;
    }

    fn seek(&mut self, target: &[u8]) {
        for i in 0..self.n {
            self.children[i].seek(target);
        }
        self.find_smallest();
        self.direction = Direction::Forward;
    }

    #[allow(clippy::collapsible_else_if)]
    fn next(&mut self) {
        assert!(self.valid());
        if self.direction != Direction::Forward {
            for child in 0..self.n {
                let curr = self.current.unwrap();
                if child != curr {
                    // cannot borrow `*self` as immutable because it is also borrowed as mutable
                    if child > curr {
                        if let [curr, .., child] = &mut self.children[curr..child + 1] {
                            seek_and_go_to_next(child, curr, self.cmp.clone());
                        }
                    } else {
                        if let [child, .., curr] = &mut self.children[child..curr + 1] {
                            seek_and_go_to_next(child, curr, self.cmp.clone());
                        }
                    }
                }
            }
            self.direction = Direction::Forward;
        }
        let curr = &mut self.children[self.current.unwrap()];
        curr.next();
        self.find_smallest();
    }

    #[allow(clippy::collapsible_else_if)]
    fn prev(&mut self) {
        assert!(self.valid());
        if self.direction != Direction::Reverse {
            for child in 0..self.n {
                let curr = self.current.unwrap();
                if child != curr {
                    if child > curr {
                        if let [curr, .., child] = &mut self.children[curr..child + 1] {
                            seek_and_go_to_prev(child, curr, self.cmp.clone());
                        }
                    } else {
                        if let [child, .., curr] = &mut self.children[child..curr + 1] {
                            seek_and_go_to_prev(child, curr, self.cmp.clone());
                        }
                    }
                }
            }
            self.direction = Direction::Reverse;
        }
        let curr = &mut self.children[self.current.unwrap()];
        curr.prev();
        self.find_largest();
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        let curr = &self.children[self.current.unwrap()];
        curr.key()
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid());
        let curr = &self.children[self.current.unwrap()];
        curr.value()
    }

    fn status(&self) -> DResult<()> {
        let mut ret = Ok(());
        for i in self.children.iter() {
            ret = i.status();
            if ret.is_err() {
                break;
            }
        }
        ret
    }
}

pub fn new_merging_iterator(
    cmp: Arc<dyn Comparator>,
    mut list: Vec<Box<dyn Iter>>,
    n: usize,
) -> Box<dyn Iter> {
    if n == 0 {
        Box::new(EmptyIterator::new())
    } else if n == 1 {
        list.remove(0)
    } else {
        Box::new(MergingIterator::new(cmp, list, n))
    }
}

pub struct LevelFileNumIter {
    icmp: Arc<dyn InternalKeyComparator>,
    list: Vec<Arc<FileMetaData>>,
    index: usize,
    value: [u8; 16],
}

impl LevelFileNumIter {
    pub fn new(icmp: Arc<dyn InternalKeyComparator>, list: Vec<Arc<FileMetaData>>) -> Self {
        Self {
            icmp,
            index: list.len(),
            list,
            value: Default::default(),
        }
    }

    fn update_value(&mut self) {
        if self.valid() {
            encode_u64_le(
                &mut self.value[..8],
                self.list[self.index].get_file_number(),
            );
            encode_u64_le(
                &mut self.value[8..16],
                self.list[self.index].get_file_size(),
            );
        }
    }
}

impl Iter for LevelFileNumIter {
    fn valid(&self) -> bool {
        self.index < self.list.len()
    }

    fn seek_to_first(&mut self) {
        self.index = 0;
        self.update_value();
    }

    fn seek_to_last(&mut self) {
        self.index = if self.list.is_empty() {
            0
        } else {
            self.list.len() - 1
        };
        self.update_value();
    }

    // Binary search to find specific file and update value with [file_number, file_size]
    fn seek(&mut self, target: &[u8]) {
        self.index = find_file(self.icmp.clone(), &self.list, target);
        self.update_value();
    }

    fn next(&mut self) {
        assert!(self.valid());
        self.index += 1;
        self.update_value();
    }

    fn prev(&mut self) {
        assert!(self.valid());
        if self.index == 0 {
            self.index = self.list.len();
        } else {
            self.index -= 1;
            self.update_value();
        }
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        self.list[self.index].get_largest().encode()
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid());
        &self.value
    }
}

pub struct ErrorIterator;

impl ErrorIterator {
    pub fn new() -> Self {
        Self
    }
}

impl Iter for ErrorIterator {
    fn valid(&self) -> bool {
        false
    }

    fn seek_to_first(&mut self) {}

    fn seek_to_last(&mut self) {}

    fn seek(&mut self, _target: &[u8]) {}

    fn next(&mut self) {
        unreachable!()
    }

    fn prev(&mut self) {
        unreachable!()
    }

    fn key(&self) -> &[u8] {
        unimplemented!()
    }

    fn value(&self) -> &[u8] {
        unimplemented!()
    }
}
