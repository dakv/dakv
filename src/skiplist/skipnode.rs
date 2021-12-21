use crate::skiplist::K_MAX_HEIGHT;
use crate::utils::arena::Arena;
use crate::utils::slice::Slice;
use std::fmt::{Error, Formatter};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::{fmt, mem, ptr};

pub struct Node {
    pub data: Slice,
    pub forward: [AtomicPtr<Self>; K_MAX_HEIGHT],
}

impl Node {
    #[allow(clippy::mut_from_ref)]
    pub fn new<A: Arena>(data: Slice, height: usize, arena: &A) -> &mut Self {
        let size = mem::size_of::<Self>() /* 32 */
                - (K_MAX_HEIGHT - height) * mem::size_of::<AtomicPtr<Self>>(); /* 8 * height*/

        let ptr = arena.alloc(size) as *mut Node;

        unsafe {
            let node = &mut *ptr;
            ptr::write(&mut node.data, data);
            ptr::write_bytes(node.forward.as_mut_ptr(), 0, height);
            node
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn head<A: Arena>(arena: &A) -> &mut Self {
        Self::new(Slice::default(), K_MAX_HEIGHT, arena)
    }

    #[inline]
    pub fn set_next(&self, n: usize, node: *mut Node) {
        self.forward[n].store(node, Ordering::SeqCst);
    }

    #[inline]
    pub fn get_next(&self, n: usize) -> *mut Node {
        self.forward[n].load(Ordering::SeqCst)
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{:?}", self.data.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::Node;
    use crate::utils::arena::ArenaImpl;
    use crate::utils::slice::Slice;

    #[test]
    fn test_new_node() {
        let arena = ArenaImpl::new();

        let node = Node::head(&arena);
        assert_eq!(format!("{}", node), "[]");

        let node = Node::new("da".into(), 0, &arena);
        assert_eq!(format!("{}", node), "[100, 97]");
    }

    #[test]
    fn test_next() {
        let arena = ArenaImpl::new();
        let t1 = vec![1];
        let t2 = vec![2];
        let t3 = vec![3];
        let node = Node::new(Slice::from(&t1), 3, &arena);
        let next = Node::new(Slice::from(&t2), 4, &arena);
        let tail = Node::new(Slice::from(&t3), 1, &arena);
        node.set_next(2, next);
        let ret = node.get_next(1);
        assert!(ret.is_null());
        let ret = node.get_next(2);
        assert!(!ret.is_null());
        unsafe {
            assert_eq!((*ret).data.as_slice(), &[2]);
        }

        next.set_next(3, tail);
        let v = next.get_next(3);
        unsafe {
            assert_eq!((*v).data.as_slice(), &[3]);
        }
    }
}
