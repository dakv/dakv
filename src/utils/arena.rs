use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::{mem, slice};

pub const K_BLOCK_SIZE: usize = 4096;

#[derive(Default)]
pub struct ArenaInner {
    alloc_ptr: AtomicPtr<u8>,
    remaining_bytes: AtomicUsize,
    memory_usage: AtomicUsize,
    blocks: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl ArenaInner {
    fn new() -> Self {
        Self::default()
    }

    fn remaining_bytes(&self) -> usize {
        self.remaining_bytes.load(Ordering::Acquire)
    }

    fn sub_remaining_bytes(&self, bytes: usize) {
        self.remaining_bytes.fetch_sub(bytes, Ordering::Release);
    }

    fn alloc_ptr(&self) -> *mut u8 {
        self.alloc_ptr.load(Ordering::Acquire)
    }

    fn add_alloc_ptr(&self, bytes: usize) {
        let p = self.alloc_ptr();
        self.alloc_ptr
            .store(unsafe { p.add(bytes) }, Ordering::Release);
    }

    fn alloc_fallback(&self, bytes: usize) -> *mut u8 {
        if bytes > K_BLOCK_SIZE / 4 {
            // Object is more than a quarter of our block size.  Allocate it separately
            // to avoid wasting too much space in leftover bytes.
            return self.allocate_new_block(bytes);
        }

        // We waste the remaining space in the current block.
        self.alloc_ptr
            .store(self.allocate_new_block(K_BLOCK_SIZE), Ordering::Release);
        self.remaining_bytes.store(K_BLOCK_SIZE, Ordering::Release);

        let result = self.alloc_ptr();
        self.add_alloc_ptr(bytes);
        self.sub_remaining_bytes(bytes);
        result
    }

    fn allocate_new_block(&self, bytes: usize) -> *mut u8 {
        let mut v = vec![0; bytes];

        let result = v.as_mut_ptr();
        self.blocks.lock().unwrap().push(v);
        self.memory_usage.store(
            self.memory_usage() + bytes + mem::size_of::<usize>(),
            Ordering::Release,
        );
        unsafe { mem::transmute(result) }
    }

    fn memory_usage(&self) -> usize {
        self.memory_usage.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
pub struct ArenaImpl {
    inner: Arc<ArenaInner>,
}

#[allow(clippy::mut_from_ref)]
pub trait Arena {
    /// Return a pointer to a newly allocated memory block of "bytes" bytes.
    fn alloc(&self, bytes: usize) -> *mut u8;

    /// Allocate slice with specific length.
    fn allocate(&self, bytes: usize) -> &mut [u8];

    /// Allocate memory with the normal alignment guarantees provided by malloc
    fn allocate_aligned(&self, bytes: usize) -> &mut [u8];

    /// Returns an estimate of the total memory usage of data allocated
    /// by the arena.
    fn memory_usage(&self) -> usize;

    fn remain_bytes(&self) -> usize;
}

impl Default for ArenaImpl {
    fn default() -> Self {
        Self {
            inner: Arc::new(ArenaInner::new()),
        }
    }
}

impl ArenaImpl {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Arena for ArenaImpl {
    fn alloc(&self, bytes: usize) -> *mut u8 {
        assert!(bytes > 0);

        if bytes <= self.inner.remaining_bytes() {
            assert!(!self.inner.alloc_ptr().is_null());
            let result = self.inner.alloc_ptr();
            self.inner.add_alloc_ptr(bytes);
            self.inner.sub_remaining_bytes(bytes);
            return result;
        }
        self.inner.alloc_fallback(bytes)
    }

    fn allocate(&self, bytes: usize) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.alloc(bytes), bytes) }
    }

    fn allocate_aligned(&self, bytes: usize) -> &mut [u8] {
        let ptr_size = mem::size_of::<usize>();
        let align = if ptr_size > 8 { ptr_size } else { 8 };

        let current_mod = self.inner.alloc_ptr() as usize & (align - 1);
        let slop = if current_mod == 0 {
            0
        } else {
            align - current_mod
        };

        let needed = bytes + slop;
        let result = if needed <= self.inner.remaining_bytes() {
            unsafe {
                let p = self.inner.alloc_ptr().add(slop);
                self.inner.add_alloc_ptr(needed);
                self.inner.sub_remaining_bytes(needed);
                p
            }
        } else {
            self.inner.alloc_fallback(bytes)
        };
        assert_eq!(result as usize & (align - 1), 0);
        unsafe { slice::from_raw_parts_mut(result, bytes) }
    }

    fn memory_usage(&self) -> usize {
        self.inner.memory_usage()
    }

    fn remain_bytes(&self) -> usize {
        self.inner.remaining_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::{Arena, ArenaImpl};
    use crate::utils::random::{Random, RandomGenerator};

    #[test]
    fn test_alloc() {
        let arena = ArenaImpl::new();

        let _ = arena.allocate_aligned(104);
        assert_eq!(arena.memory_usage(), 4104);
    }

    #[test]
    fn test_simple() {
        let mut allocated = vec![];
        let arena = ArenaImpl::new();

        let n = 100000;
        let mut bytes = 0;
        let rnd = Random::new(301);
        for i in 0..n {
            let mut s;
            if i % (n / 10) == 0 {
                s = i;
            } else {
                s = if rnd.one_in(4000) {
                    rnd.uniform(6000) as usize
                } else {
                    if rnd.one_in(10) {
                        rnd.uniform(100) as usize
                    } else {
                        rnd.uniform(20) as usize
                    }
                }
            }
            if s == 0 {
                s = 1;
            }
            let r = if rnd.one_in(10) {
                arena.allocate_aligned(s)
            } else {
                arena.allocate(s)
            };
            for b in 0..s {
                r[b] = (i % 256) as u8;
            }
            bytes += s;
            allocated.push((s, r));
            assert!(arena.memory_usage() >= bytes);
            if i > n / 10 {
                assert!((arena.memory_usage() as f64) <= (bytes as f64) * 1.10);
            }
        }

        for i in 0..allocated.len() {
            let num_bytes = allocated[i].0;
            let p = &allocated[i].1;
            for b in 0..num_bytes {
                assert_eq!(p[b] & 0xff, (i % 256) as u8);
            }
        }
    }
}
