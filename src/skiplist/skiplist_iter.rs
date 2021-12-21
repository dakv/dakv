use crate::skiplist::skipnode::Node;
use crate::skiplist::{SkipList, K_MAX_HEIGHT};
use crate::utils::arena::Arena;
use crate::utils::cmp::BaseComparator;
use crate::utils::random::RandomGenerator;
use std::iter;
use std::ptr::{null, null_mut};

pub struct SkipListIter<R, C, A>
where
    R: RandomGenerator,
    C: BaseComparator,
    A: Arena,
{
    list: SkipList<R, C, A>,
    node: *const Node,
}

impl<R, C, A> SkipListIter<R, C, A>
where
    R: RandomGenerator,
    C: BaseComparator,
    A: Arena,
{
    pub fn new(list: SkipList<R, C, A>) -> Self {
        Self { list, node: null() }
    }

    pub fn valid(&self) -> bool {
        !self.node.is_null()
    }

    pub fn seek_to_first(&mut self) {
        let n = self.list.get_head();
        self.node = n.get_next(0);
    }

    pub fn seek_to_last(&mut self) {
        self.node = self.list.find_last();
        if self.node == self.list.get_head() {
            self.node = null();
        }
    }

    /// For mem table to seek entry.
    pub fn seek(&mut self, target: &[u8]) {
        let mut prev = iter::repeat(null_mut()).take(K_MAX_HEIGHT).collect();
        self.node = self.list.find(target, &mut prev);
    }

    pub fn next(&mut self) {
        assert!(self.valid());
        self.node = unsafe { (*self.node).get_next(0) };
    }

    pub fn prev(&mut self) {
        assert!(self.valid());
        let key = unsafe { (*self.node).data.as_slice() };
        self.node = self.list.find_less_than(key);

        if self.node == self.list.get_head() {
            self.node = null();
        }
    }

    pub fn key(&self) -> &[u8] {
        assert!(self.valid());
        unsafe { (*self.node).data.as_slice() as _ }
    }
}

#[cfg(test)]
mod tests {
    use crate::skiplist::{SkipList, SkipListIter};
    use crate::utils::arena::ArenaImpl;
    use crate::utils::cmp::DefaultComparator;
    use crate::utils::random::{Random, RandomGenerator};
    use crate::utils::slice::Slice;

    #[test]
    fn test_basic() {
        let mut sl = SkipList::new(
            Random::new(0xdead_beef),
            DefaultComparator::default(),
            ArenaImpl::new(),
        );
        let mut s = vec![];
        for i in 0..100u8 {
            s.push(vec![i]);
            sl.insert(Slice::from(s.last().unwrap()));
        }

        let mut iter = SkipListIter::new(sl);
        assert!(!iter.valid());
        iter.seek_to_first();
        assert!(iter.valid());
        assert_eq!(iter.key(), &[0]);
        iter.seek_to_last();
        assert_eq!(iter.key(), &[99]);

        iter.seek(&[88]);
        assert_eq!(iter.key(), &[88]);

        iter.next();
        assert_eq!(iter.key(), &[89]);

        iter.seek(&[99]);
        assert_eq!(iter.key(), &[99]);
        iter.prev();
        assert_eq!(iter.key(), &[98]);
    }

    #[test]
    #[ignore]
    fn test_random() {
        let rnd = Random::new(3);

        for _ in 0..100 {
            let mut sl = SkipList::new(
                Random::new(0xdead_beef),
                DefaultComparator::default(),
                ArenaImpl::new(),
            );
            let mut s = vec![];
            for _ in 0..10000 {
                s.push(vec![rnd.next() as u8]);
                sl.insert(Slice::from(s.last().unwrap()));
            }
            let mut iter = SkipListIter::new(sl.clone());

            for i in s {
                iter.seek(i.as_slice());
                assert!(iter.valid());
            }
        }
    }
}
