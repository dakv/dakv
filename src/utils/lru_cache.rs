use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use std::collections::HashMap;
use std::mem;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

struct Key<K> {
    k: *const K,
}

impl<K: Hash> Hash for Key<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { (*self.k).hash(state) }
    }
}

impl<K: PartialEq> PartialEq for Key<K> {
    fn eq(&self, other: &Key<K>) -> bool {
        unsafe { (*self.k).eq(&*other.k) }
    }
}

impl<K: Eq> Eq for Key<K> {}

impl<K> Default for Key<K> {
    fn default() -> Self {
        Key { k: ptr::null() }
    }
}

struct LRUEntry<K, V> {
    key: MaybeUninit<K>,
    value: MaybeUninit<V>,
    prev: *mut LRUEntry<K, V>,
    next: *mut LRUEntry<K, V>,
}

impl<K, V> LRUEntry<K, V> {
    fn new(key: K, value: V) -> Self {
        LRUEntry {
            key: MaybeUninit::new(key),
            value: MaybeUninit::new(value),
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }
    fn new_empty() -> Self {
        LRUEntry {
            key: MaybeUninit::uninit(),
            value: MaybeUninit::uninit(),
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }
}

/// LRU cache implementation
pub struct LRUCache<K, V> {
    // The capacity of LRU
    capacity: usize,
    inner: Arc<Mutex<LRUInner<K, V>>>,
    // The size of space which have been allocated
    usage: Arc<AtomicUsize>,
}

struct LRUInner<K, V> {
    table: HashMap<Key<K>, Box<LRUEntry<K, V>>>,
    // head.next is the newest entry
    head: *mut LRUEntry<K, V>,
    tail: *mut LRUEntry<K, V>,
}

impl<K, V> LRUInner<K, V> {
    fn detach(&mut self, n: *mut LRUEntry<K, V>) {
        unsafe {
            (*(*n).next).prev = (*n).prev;
            (*(*n).prev).next = (*n).next;
        }
    }

    fn attach(&mut self, n: *mut LRUEntry<K, V>) {
        unsafe {
            (*n).next = (*self.head).next;
            (*n).prev = self.head;
            (*self.head).next = n;
            (*(*n).next).prev = n;
        }
    }
}

impl<K: Hash + Eq, V: Clone> LRUCache<K, V> {
    pub fn new(cap: usize) -> Self {
        let l = LRUInner {
            table: HashMap::default(),
            head: Box::into_raw(Box::new(LRUEntry::new_empty())),
            tail: Box::into_raw(Box::new(LRUEntry::new_empty())),
        };

        unsafe {
            (*l.head).next = l.tail;
            (*l.tail).prev = l.head;
        }

        LRUCache {
            usage: Arc::new(AtomicUsize::new(0)),
            capacity: cap,
            inner: Arc::new(Mutex::new(l)),
        }
    }
}

impl<K, V> Cache<K, V> for LRUCache<K, V>
where
    K: Send + Sync + Hash + Eq,
    V: Send + Sync + Clone,
{
    fn insert(&self, key: K, mut value: V) -> Option<V> {
        let mut l = self.inner.lock().unwrap();
        if self.capacity > 0 {
            match l.table.get_mut(&Key {
                k: &key as *const K,
            }) {
                Some(h) => {
                    let old_p = h as *mut Box<LRUEntry<K, V>>;
                    unsafe { mem::swap(&mut value, &mut (*(*old_p).value.as_mut_ptr())) };
                    let p: *mut LRUEntry<K, V> = h.as_mut();
                    l.detach(p);
                    l.attach(p);
                    Some(value)
                }
                None => {
                    let mut node = {
                        if self.usage.load(Ordering::Acquire) >= self.capacity {
                            let prev_key = Key {
                                k: unsafe { (*(*l.tail).prev).key.as_ptr() },
                            };
                            let mut n = l.table.remove(&prev_key).unwrap();

                            unsafe {
                                ptr::drop_in_place(n.key.as_mut_ptr());
                                ptr::drop_in_place(n.value.as_mut_ptr());
                            }
                            n.key = MaybeUninit::new(key);
                            n.value = MaybeUninit::new(value);
                            l.detach(n.as_mut());
                            n
                        } else {
                            Box::new(LRUEntry::new(key, value))
                        }
                    };
                    self.usage.fetch_add(1, Ordering::Relaxed);
                    l.attach(node.as_mut());
                    l.table.insert(
                        Key {
                            k: node.key.as_ptr(),
                        },
                        node,
                    );
                    None
                }
            }
        } else {
            None
        }
    }

    fn lookup(&self, key: &K) -> Option<V> {
        let k = Key { k: key as *const K };
        let mut l = self.inner.lock().unwrap();
        if let Some(node) = l.table.get_mut(&k) {
            let p = node.as_mut() as *mut LRUEntry<K, V>;
            l.detach(p);
            l.attach(p);
            Some(unsafe { (*(*p).value.as_ptr()).clone() })
        } else {
            None
        }
    }

    fn erase(&self, key: &K) {
        let k = Key { k: key as *const K };
        let mut l = self.inner.lock().unwrap();
        if let Some(mut n) = l.table.remove(&k) {
            l.detach(n.as_mut() as *mut LRUEntry<K, V>);
        }
    }

    #[inline]
    fn total_charge(&self) -> usize {
        self.usage.load(Ordering::Acquire)
    }

    fn new_id(&self) -> u64 {
        unimplemented!()
    }
}

impl<K, V> Drop for LRUCache<K, V> {
    fn drop(&mut self) {
        let mut l = self.inner.lock().unwrap();
        (*l).table.values_mut().for_each(|e| unsafe {
            ptr::drop_in_place(e.key.as_mut_ptr());
            ptr::drop_in_place(e.value.as_mut_ptr());
        });
        unsafe {
            let _head = *Box::from_raw(l.head);
            let _tail = *Box::from_raw(l.tail);
        }
    }
}

unsafe impl<K: Send, V: Send> Send for LRUCache<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for LRUCache<K, V> {}

pub trait Cache<K, V>: Sync + Send
where
    K: Sync + Send,
    V: Sync + Send,
{
    /// Insert a mapping from key->value into the cache and assign it
    /// the specified charge against the total cache capacity.
    fn insert(&self, key: K, value: V) -> Option<V>;

    /// If the cache has no mapping for `key`, returns `None`.
    fn lookup(&self, key: &K) -> Option<V>;

    /// If the cache contains entry for key, erase it.
    fn erase(&self, key: &K);

    /// Return an estimate of the combined charges of all elements stored in the
    /// cache.
    fn total_charge(&self) -> usize;

    fn new_id(&self) -> u64;
}

/// A sharded cache container by key hash
pub struct SharedLRUCache<K, V>
where
    K: Sync + Send,
    V: Sync + Send,
{
    shards: Arc<Vec<LRUCache<K, V>>>,
    last_id: Arc<Mutex<u64>>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}
const DEFAULT_CACHE_SHARDS: usize = 8;

impl<K, V> SharedLRUCache<K, V>
where
    K: Sync + Send + Hash + Eq,
    V: Sync + Send + Clone,
{
    /// Create a new `ShardedCache` with given shards
    pub fn new(cap: usize) -> Self {
        let mut shards = vec![];
        for _ in 0..DEFAULT_CACHE_SHARDS {
            shards.push(LRUCache::new(cap));
        }
        Self {
            shards: Arc::new(shards),
            last_id: Arc::new(Mutex::new(0)),
            _k: PhantomData,
            _v: PhantomData,
        }
    }

    fn find_shard(&self, k: &K) -> usize {
        let mut s = DefaultHasher::new();
        let len = self.shards.len();
        k.hash(&mut s);
        s.finish() as usize % len
    }
}

impl<K, V> Cache<K, V> for SharedLRUCache<K, V>
where
    K: Sync + Send + Hash + Eq,
    V: Sync + Send + Clone,
{
    fn insert(&self, key: K, value: V) -> Option<V> {
        let idx = self.find_shard(&key);
        self.shards[idx].insert(key, value)
    }

    fn lookup(&self, key: &K) -> Option<V> {
        let idx = self.find_shard(key);
        self.shards[idx].lookup(key)
    }

    fn erase(&self, key: &K) {
        let idx = self.find_shard(key);
        self.shards[idx].erase(key)
    }

    fn total_charge(&self) -> usize {
        self.shards.iter().fold(0, |acc, s| acc + s.total_charge())
    }

    fn new_id(&self) -> u64 {
        let mut last = self.last_id.lock().unwrap();
        *last += 1;
        *last
    }
}
