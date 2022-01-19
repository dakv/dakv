use crate::skiplist::skipnode::Node;
use crate::skiplist::K_MAX_HEIGHT;
use crate::utils::arena::Arena;
use crate::utils::cmp::BaseComparator;
use crate::utils::random::RandomGenerator;
use crate::utils::slice::Slice;
use std::cmp;
use std::fmt;
use std::iter;
use std::marker::PhantomData;
use std::mem;
use std::ptr::{null_mut, NonNull};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Skip list is a data structure that allows O(log n) search complexity as well as
/// O(log n) insertion complexity within an ordered sequence of n elements.
/// Thus it can get the best of array while maintaining a linked list-like structure
/// that allows insertion- which is not possible in an array. Fast search is made
/// possible by maintaining a linked hierarchy of subsequences, with each successive
/// subsequence skipping over fewer elements than the previous one. Searching starts
/// in the sparsest subsequence until two consecutive elements have been found,
/// one smaller and one larger than or equal to the element searched for.
pub struct SkipListInner<R, C, A>
where
    R: RandomGenerator,
    C: BaseComparator,
    A: Arena,
{
    head: NonNull<Node>,
    max_height: AtomicUsize,
    len: AtomicUsize,
    rnd: R,
    cmp: C,
    arena: A,
}

unsafe impl<R, C, A> Send for SkipListInner<R, C, A>
where
    R: RandomGenerator,
    C: BaseComparator,
    A: Arena,
{
}

unsafe impl<R, C, A> Sync for SkipListInner<R, C, A>
where
    R: RandomGenerator,
    C: BaseComparator,
    A: Arena,
{
}

#[derive(Clone)]
pub struct SkipList<R, C, A>
where
    R: RandomGenerator,
    C: BaseComparator,
    A: Arena,
{
    inner: Arc<SkipListInner<R, C, A>>,
}

impl<R, C, A> SkipList<R, C, A>
where
    R: RandomGenerator,
    C: BaseComparator,
    A: Arena,
{
    pub fn new(rnd: R, cmp: C, arena: A) -> Self {
        SkipList {
            inner: Arc::new(SkipListInner {
                head: NonNull::from(Node::head(&arena)),
                max_height: AtomicUsize::new(1), // max height in all of the nodes except head node
                len: AtomicUsize::new(0),
                rnd,
                cmp,
                arena,
            }),
        }
    }

    /// Returns the number of elements in the skiplist.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len.load(Ordering::SeqCst)
    }

    /// Returns `true` if the skiplist is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn memory_size(&self) -> usize {
        self.inner.arena.memory_usage()
    }

    pub fn remain_bytes(&self) -> usize {
        self.inner.arena.remain_bytes()
    }

    #[inline]
    pub fn get_max_height(&self) -> usize {
        self.inner.max_height.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn set_max_height(&mut self, h: usize) {
        self.inner.max_height.store(h, Ordering::SeqCst);
    }

    /// Clear every single node and reset the head node.
    #[inline]
    pub fn clear(&mut self) {
        // let new_head = Node::head(&self.inner.herd);
        self.inner.len.store(0, Ordering::SeqCst);
        // unsafe { mem::replace(&mut self.inner.head.as_ptr(), new_head) }
    }

    /// 1/4 probability
    fn random_height(&mut self) -> usize {
        let k_branching = 4;
        let mut height = 1;
        while height < K_MAX_HEIGHT && (self.inner.rnd.next() % k_branching == 0) {
            height += 1;
        }
        assert!(height > 0);
        assert!(height <= K_MAX_HEIGHT);
        height
    }

    /// Look for the node greater than or equal to key
    pub fn find(&self, key: &[u8], prev: &mut Vec<*mut Node>) -> *mut Node {
        // const pointer
        let mut const_ptr: *const Node = unsafe { self.inner.head.as_ref() };
        let mut height = self.get_max_height() - 1;
        loop {
            let next_ptr = unsafe { (*const_ptr).get_next(height) };
            // if key > next_ptr => now = next
            if self.key_is_after_node(key, next_ptr) {
                const_ptr = next_ptr as *const Node;
            } else {
                if !prev.is_empty() {
                    prev[height] = const_ptr as *mut Node;
                }
                if height == 0 {
                    return next_ptr;
                } else {
                    height -= 1;
                }
            }
        }
    }

    fn key_is_after_node(&self, key: &[u8], node: *mut Node) -> bool {
        if node.is_null() {
            false
        } else {
            self.lt(unsafe { (*node).data.as_slice() }, key)
        }
    }

    /// 1. Find the node greater than or equal to the key and return the mutable reference
    /// 2. Randomly generate level
    /// 3. Create new node
    /// 4. Insert and set forwards
    pub fn insert(&mut self, key: Slice) {
        let mut prev = iter::repeat(null_mut()).take(K_MAX_HEIGHT).collect();
        self.find(key.as_slice(), &mut prev);
        // random height
        let height = self.random_height();
        // record all previous node that are higher than the current
        if height > self.get_max_height() {
            for node in prev.iter_mut().take(height).skip(self.get_max_height()) {
                *node = self.inner.head.as_ptr();
            }
            self.set_max_height(height);
        }
        // Accelerate memory allocation
        let n = Node::new(key, height, &self.inner.arena);
        for (i, &mut node) in prev.iter_mut().enumerate().take(height) {
            unsafe {
                let tmp = (*node).get_next(i);
                n.set_next(i, tmp);
                (*node).set_next(i, n);
            }
        }
        self.inner.len.fetch_add(1, Ordering::SeqCst);
    }

    pub fn contains(&mut self, key: &[u8]) -> bool {
        let mut prev = iter::repeat(null_mut()).take(K_MAX_HEIGHT).collect();
        let x = self.find(key, &mut prev);
        !x.is_null() && self.eq(key, unsafe { (*x).data.as_slice() })
    }

    fn eq(&self, a: &[u8], b: &[u8]) -> bool {
        self.inner.cmp.compare(a, b) == cmp::Ordering::Equal
    }

    fn lt(&self, a: &[u8], b: &[u8]) -> bool {
        self.inner.cmp.compare(a, b) == cmp::Ordering::Less
    }

    fn gte(&self, a: &[u8], b: &[u8]) -> bool {
        let r = self.inner.cmp.compare(a, b);
        r == cmp::Ordering::Greater || r == cmp::Ordering::Equal
    }

    pub fn get_head(&self) -> &Node {
        unsafe { self.inner.head.as_ref() }
    }

    #[allow(clippy::unnecessary_unwrap)]
    pub fn find_less_than(&self, key: &[u8]) -> *const Node {
        let mut x: *const Node = unsafe { mem::transmute_copy(&self.inner.head) };
        let mut level = self.get_max_height() - 1;
        unsafe {
            loop {
                let next = (*x).get_next(level);
                if next.is_null() || self.gte((*next).data.as_slice(), key) {
                    if level == 0 {
                        return x;
                    } else {
                        level -= 1;
                    }
                } else {
                    x = next;
                }
            }
        }
    }

    pub fn find_last(&self) -> *const Node {
        let mut x = self.inner.head.as_ptr() as *const Node;
        let mut level = self.get_max_height() - 1;

        loop {
            let next = unsafe { (*x).get_next(level) };
            if !next.is_null() {
                x = next;
            } else if level == 0 {
                return x;
            } else {
                level -= 1;
            }
        }
    }
}

impl<R, C, A> fmt::Display for SkipList<R, C, A>
where
    R: RandomGenerator,
    C: BaseComparator,
    A: Arena,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[")?;
        unsafe {
            let mut head: *const Node = mem::transmute_copy(&self.inner.head);
            loop {
                let next = (*head).get_next(0);
                if next.is_null() {
                    break;
                } else {
                    write!(f, "{:?} ", (*next).data.as_slice())?;
                    head = next as *const Node;
                }
            }
        }
        write!(f, "]")
    }
}

pub struct Iter<'a> {
    head: *const Node,
    size: usize,
    _lifetime: PhantomData<&'a Node>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a Node;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            // If the lowest forward node is None, return None.
            let next = (*self.head).get_next(0);
            if !next.is_null() {
                self.head = next;
                if self.size > 0 {
                    self.size -= 1;
                }
                return Some(&*self.head);
            }
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}

impl<'a, R, C, A> iter::IntoIterator for &'a SkipList<R, C, A>
where
    R: RandomGenerator,
    C: BaseComparator,
    A: Arena,
{
    type Item = &'a Node;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        Iter {
            head: unsafe { mem::transmute_copy(&self.inner.head) },
            size: self.len(),
            _lifetime: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::skiplist::SkipList;
    use crate::utils::arena::{ArenaImpl, K_BLOCK_SIZE};
    use crate::utils::cmp::DefaultComparator;
    use crate::utils::random::Random;
    use crate::utils::slice::Slice;
    use std::mem;

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
        assert_eq!(sl.len(), 100);
        for i in 0..100 {
            assert!(sl.contains(&[i]));
        }
        for i in 100..120 {
            assert_eq!(sl.contains(&[i]), false);
        }
    }

    #[test]
    fn test_clear() {
        let mut sl = SkipList::new(
            Random::new(0xdead_beef),
            DefaultComparator::default(),
            ArenaImpl::new(),
        );
        let mut s = vec![];
        for i in 0..12 {
            s.push(vec![i]);
            sl.insert(Slice::from(s.last().unwrap()));
        }
        sl.clear();
        assert!(sl.is_empty());
    }

    #[test]
    fn test_into_iter() {
        let mut sl = SkipList::new(
            Random::new(0xdead_beef),
            DefaultComparator::default(),
            ArenaImpl::new(),
        );
        let mut s = vec![];
        for i in 0..10 {
            s.push(vec![i]);
            sl.insert(Slice::from(s.last().unwrap()));
        }
        for (count, i) in (&sl).into_iter().enumerate() {
            assert_eq!(i.data.as_slice()[0], count as u8);
        }
    }

    #[test]
    fn test_basic_desc() {
        let mut sl = SkipList::new(
            Random::new(0xdead_beef),
            DefaultComparator::default(),
            ArenaImpl::new(),
        );
        let mut s = vec![];
        for i in (0..12).rev() {
            s.push(vec![i]);
            sl.insert(Slice::from(s.last().unwrap()));
        }
        assert_eq!(
            "[[0] [1] [2] [3] [4] [5] [6] [7] [8] [9] [10] [11] ]",
            format!("{}", sl)
        );
    }

    #[test]
    fn test_memory_usage() {
        let mut sl = SkipList::new(
            Random::new(0xdead_beef),
            DefaultComparator::default(),
            ArenaImpl::new(),
        );
        assert_eq!(sl.memory_size(), K_BLOCK_SIZE + mem::size_of::<usize>());
        assert_eq!(sl.remain_bytes(), 3984); // 3992 - 3968 = 24 = (32 - 16)
        sl.insert(Slice::from(&vec![0; 1000]));
        assert_eq!(sl.memory_size(), K_BLOCK_SIZE + mem::size_of::<usize>());
        assert_eq!(sl.remain_bytes(), 3952); // 48 = 32 + 8 * height(2)
    }

    #[test]
    #[ignore]
    fn test_concurrency() {
        // TODO: concurrent test
        // let sl: SkipList<Random, DefaultComparator, ArenaImpl> = SkipList::default();
        // for i in 0..12 {
        //     let mut csl = sl.clone();
        //     thread::Builder::new()
        //         .name(format!("thread:{}", i))
        //         .spawn(move || {
        //             csl.insert(vec![i]);
        //         })
        //         .unwrap();
        // }
        // assert_eq!(
        //     "[[0] [1] [2] [3] [4] [5] [6] [7] [8] [9] [10] [11] ]",
        //     format!("{}", sl)
        // );
    }
}
