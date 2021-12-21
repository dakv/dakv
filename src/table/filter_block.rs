use crate::db::extract_user_key;
use crate::utils::coding::{decode_u32_le, put_fixed_32};
use crate::utils::hash::hash;
use std::ptr::null;
use std::slice;
use std::sync::Arc;

pub trait FilterPolicy {
    /// Return the name of this policy. Note that if the filter encoding
    /// changes in an incompatible way, the name returned by this method
    /// must be changed. Otherwise, old incompatible filters may be
    /// passed to methods of this type.
    fn name(&self) -> &'static str;

    /// "filter" contains the data appended by a preceding call to
    /// `create_filter` on this trait. This method must return true if
    /// the key was in the list of keys passed to `create_filter`.
    /// This method may return true or false if the key was not on the
    /// list, but it should aim to return false with a high probability.
    fn key_may_exist(&self, key: &[u8], filter: &[u8]) -> bool;

    /// keys[0, n-1] contains a list of keys (potentially with duplicates)
    /// that are ordered according to the user supplied comparator.
    /// Append a filter that summarizes keys[0, n-1] to `dst`.
    /// Do not change the initial contents of `dst`.
    /// Instead, append the newly constructed filter to `dst`.
    fn create_filter(&self, keys: Vec<&[u8]>, dst: &mut Vec<u8>, n: usize);
}

pub struct BloomFilterPolicy {
    bits_per_key: u64,
    k: usize,
}

fn bloom_hash(key: &[u8]) -> u32 {
    hash(key, key.len(), 0xbc9f1d34)
}

impl FilterPolicy for BloomFilterPolicy {
    fn name(&self) -> &'static str {
        "BuiltinBloomFilter"
    }

    fn key_may_exist(&self, key: &[u8], filter: &[u8]) -> bool {
        let len = filter.len();
        if len < 2 {
            return false;
        }
        let bits = (len - 1) * 8;
        let array = filter;

        let k = array[len - 1];
        if k > 30 {
            return true;
        }
        let mut h = bloom_hash(key);
        let delta = (h >> 17) | (h.wrapping_shl(15));
        for _ in 0..k {
            let bit_pos = h % bits as u32;
            if array[bit_pos as usize / 8] & (1 << (bit_pos % 8) as u8) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }

    fn create_filter(&self, keys: Vec<&[u8]>, dst: &mut Vec<u8>, n: usize) {
        let mut bits = n as u64 * self.bits_per_key;
        if bits < 64 {
            bits = 64;
        }
        let bytes = (bits + 7) / 8;
        bits = bytes * 8;

        let init_size = dst.len();
        // modify dst slice and fill with 0 and append k (number of hash functions)
        dst.extend(vec![0; bytes as usize]);
        dst.push(self.k as u8);
        let array = &mut dst.as_mut_slice()[init_size..];
        for k in keys.iter().take(n) {
            let mut h = bloom_hash(k);
            let delta = (h >> 17) | (h.wrapping_shl(15));
            for _ in 0..self.k {
                let bit_pos = h % bits as u32;
                array[bit_pos as usize / 8] |= (1u32.wrapping_shl(bit_pos % 8)) as u8;
                h = h.wrapping_add(delta);
            }
        }
    }
}

impl BloomFilterPolicy {
    /// Return a new filter policy that uses a bloom filter with approximately
    /// the specified number of bits per key.  A good value for bits_per_key
    /// is 10, which yields a filter with ~ 1% false positive rate.
    ///
    /// Callers must delete the result after any database that is using the
    /// result has been closed.
    /// Note: if you are using a custom comparator that ignores some parts
    /// of the keys being compared, you must not use BloomFilterPolicy::new()
    /// and must provide your own `FilterPolicy` that also ignores the
    /// corresponding parts of the keys.  For example, if the comparator
    /// ignores trailing spaces, it would be incorrect to use a
    /// FilterPolicy (like `NewBloomFilterPolicy`) that does not ignore
    /// trailing spaces in keys.
    pub fn new(bits_per_key: u64) -> Self {
        // 0.69 =~ ln(2)
        let mut k = (bits_per_key as f64 * 0.69) as usize;
        if k < 1 {
            k = 1;
        }
        if k > 30 {
            k = 30;
        }
        Self { bits_per_key, k }
    }
}

/// For options.filter_policy and `Table`
pub struct InternalFilterPolicy {
    user_policy: Arc<dyn FilterPolicy + Send + Sync>,
}

impl InternalFilterPolicy {
    pub fn new(user_policy: Arc<dyn FilterPolicy + Send + Sync>) -> Self {
        Self { user_policy }
    }
}

impl FilterPolicy for InternalFilterPolicy {
    fn name(&self) -> &'static str {
        self.user_policy.name()
    }

    fn key_may_exist(&self, key: &[u8], filter: &[u8]) -> bool {
        self.user_policy
            .key_may_exist(extract_user_key(key), filter)
    }

    fn create_filter(&self, keys: Vec<&[u8]>, dst: &mut Vec<u8>, n: usize) {
        let mut ret = vec![];
        for k in keys.iter().take(n) {
            ret.push(extract_user_key(k));
        }
        self.user_policy.create_filter(ret, dst, n);
    }
}

// Generate new filter every 2KB of data
const FILTER_BASE_LG: u64 = 11;
const FILTER_BASE: u64 = 1 << FILTER_BASE_LG;

/// A `FilterBlockBuilder` is used to construct all of the filters for a
/// particular Table.  It generates a single string which is stored as
/// a special block in the Table.
/// For TableBuilder
pub struct FilterBlockWriter {
    key: Vec<u8>,
    start: Vec<usize>,
    result: Vec<u8>,
    filter_offsets: Vec<u32>,
    policy: Arc<dyn FilterPolicy + Send + Sync>,
}

unsafe impl Send for FilterBlockWriter {}

unsafe impl Sync for FilterBlockWriter {}

impl FilterBlockWriter {
    pub fn new(policy: Arc<dyn FilterPolicy + Send + Sync>) -> Self {
        Self {
            policy,
            ..Default::default()
        }
    }

    // Generate new filter every 2KB of data
    pub fn start_block(&mut self, offset: u64) {
        let filter_index = (offset / FILTER_BASE) as usize;
        while filter_index > self.filter_offsets.len() {
            self.generate_filter();
        }
    }

    pub fn add_key(&mut self, key: &[u8]) {
        self.start.push(self.key.len());
        self.key.extend_from_slice(key);
    }

    fn generate_filter(&mut self) {
        let num_keys = self.start.len();
        if num_keys == 0 {
            self.filter_offsets.push(self.result.len() as u32);
            return;
        }
        self.start.push(self.key.len());

        let mut tmp_keys = vec![];
        for i in 0..num_keys {
            let base = &self.key.as_slice()[self.start[i]..];
            let len = self.start[i + 1] - self.start[i];
            tmp_keys.push(&base[..len]);
        }

        self.filter_offsets.push(self.result.len() as u32);
        self.policy
            .create_filter(tmp_keys, &mut self.result, num_keys);

        self.key.clear();
        self.start.clear();
    }

    pub fn finish(&mut self) -> &Vec<u8> {
        if !self.start.is_empty() {
            self.generate_filter();
        }

        let array_offset = self.result.len();
        for i in 0..self.filter_offsets.len() {
            put_fixed_32(&mut self.result, self.filter_offsets[i]);
        }
        put_fixed_32(&mut self.result, array_offset as u32);
        self.result.push(FILTER_BASE_LG as u8);
        &self.result
    }
}

impl Default for FilterBlockWriter {
    fn default() -> Self {
        let f = Arc::new(BloomFilterPolicy::new(10));
        Self {
            key: Default::default(),
            start: vec![],
            result: Default::default(),
            filter_offsets: vec![],
            policy: f,
        }
    }
}

pub struct FilterBlockReader {
    // num of entries
    num: usize,
    base_lg: usize,
    // pointer to filter data, use raw pointer to make the code easy.
    data: *const u8,
    // pointer to beginning of offset array
    offset: *const u8,
    policy: Arc<dyn FilterPolicy + Send + Sync>,
}

impl FilterBlockReader {
    pub fn new(policy: Arc<dyn FilterPolicy + Send + Sync>, data: &[u8]) -> Self {
        let mut c = Self {
            num: 0,
            base_lg: 0,
            data: null(),
            offset: null(),
            policy,
        };
        let n = data.len();
        if n < 5 {
            return c;
        }
        c.base_lg = data[n - 1] as usize;
        let last_word = decode_u32_le(&data[n - 5..]);
        if last_word > n as u32 - 5 {
            return c;
        }
        c.data = data.as_ptr();
        c.offset = c.data.wrapping_add(last_word as usize);
        c.num = (n - 5 - last_word as usize) / 4;
        c
    }

    pub fn key_may_exist(&self, offset: u64, key: &[u8]) -> bool {
        let index = (offset >> self.base_lg) as usize;
        unsafe {
            if index < self.num {
                let start = decode_u32_le(slice::from_raw_parts(
                    self.offset.wrapping_add(index * 4),
                    4,
                )) as usize;
                let limit = decode_u32_le(slice::from_raw_parts(
                    self.offset.wrapping_add(index * 4 + 4),
                    4,
                )) as usize;

                // offset - data
                if start <= limit && limit <= self.offset as usize - self.data as usize {
                    let filter =
                        slice::from_raw_parts(self.data.wrapping_add(start), limit - start);

                    return self.policy.key_may_exist(key, filter);
                } else if start == limit {
                    return false;
                }
            }
        }
        true
    }
}
