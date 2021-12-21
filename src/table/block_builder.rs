// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.

use crate::db::Options;
use crate::utils::coding::{put_fixed_32, put_varint};
use std::cmp::min;
use std::mem;

#[derive(Default)]
pub struct BlockBuilder {
    // Destination buffer
    buffer: Vec<u8>,
    restart_points: Vec<u32>,
    counter: i64,
    // Check finish() method has been called
    finished: bool,
    // Store last key to compare with next key.
    last_key: Vec<u8>,
    opt: Options,
}

impl BlockBuilder {
    pub fn new(options: Options) -> Self {
        Self {
            restart_points: vec![0; 1],
            counter: 0,
            finished: false,
            opt: options,
            ..Default::default()
        }
    }

    /// Data Block format:
    /// ```text
    ///                                             ┌─────────────┐ ┌───────────┐
    ///                                internal_key=│  key_shared │+│ key_delta │
    /// ┌────────────────┐                          └─────────────┘ └───────────┘
    /// │  Key  -> Value │                                                ▲
    /// ├────────────────┤    ┌───────────┬──────────────┬───────────┬────────────────────┬───────┐
    /// │  Key  -> Value │    │shared_len │ unshared_len │ value_len │ internal_key_delta │ value │
    /// ├────────────────┤    ├───────────┴─────┬────────┴┬──────────┴───────┬────────────┴───────┘
    /// │    restarts    ├───►│ restart[0](4B)] │ ... ... │ [restart[n](4B)] │ [num_restarts(4B)] │
    /// ├────────────────┤    ├─────────────────┴─────┬───┴────────────┬─────┴────────────────────┘
    /// │    trailer     ├───►│ compression_type(1B)  │   CRC32(4B)    │
    /// └────────────────┘    └───────────────────────┴────────────────┘
    /// ```
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let last_key_piece = &self.last_key;
        assert!(!self.finished);

        let mut shared = 0;
        if self.counter < self.opt.block_restart_interval {
            let min_len = min(last_key_piece.len(), key.len());
            while shared < min_len && last_key_piece.as_slice()[shared] == key[shared] {
                shared += 1;
            }
        } else {
            self.restart_points.push(self.buffer.len() as u32);
            self.counter = 0;
        }

        let non_shared = key.len() - shared;

        put_varint(&mut self.buffer, shared);
        put_varint(&mut self.buffer, non_shared);
        put_varint(&mut self.buffer, value.len());

        // Add string delta to buffer followed by value
        self.buffer
            .extend_from_slice(&key[shared..shared + non_shared]);
        self.buffer.extend_from_slice(value);

        // Update state
        self.last_key.resize(shared, 0);
        self.last_key
            .extend_from_slice(key[shared..shared + non_shared].as_ref());
        self.counter += 1;
    }

    pub fn empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restart_points.clear();
        self.restart_points.push(0);
        self.counter = 0;
        self.finished = false;
        self.last_key.clear();
    }

    // Append restart array
    // Generate the buffer and write into file.
    pub fn finish(&mut self) -> &Vec<u8> {
        for i in &self.restart_points {
            put_fixed_32(&mut self.buffer, *i);
        }
        put_fixed_32(&mut self.buffer, self.restart_points.len() as u32);
        self.finished = true;
        &self.buffer
    }

    pub fn current_size_estimate(&self) -> usize {
        self.buffer.len() + /* buffer data */
            self.restart_points.len() * mem::size_of::<u32>() + /* restart length*/
            mem::size_of::<u32>() /* restart array length*/
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        let mut b = BlockBuilder::new(Options::default());
        b.add(b"revolve", b"value");
        assert_eq!(
            b.buffer.as_slice(),
            &[
                0, /* shared len*/
                7, /* non shared len*/
                5, /* value len*/
                114, 101, 118, 111, 108, 118, 101, /* "revolve" */
                118, 97, 108, 117, 101 /* "value" */
            ]
        );
        b.add(b"reverse", b"value");
        assert_eq!(
            &b.buffer.as_slice()[15..],
            &[
                3, /* shared len*/
                4, /* non shared len*/
                5, /* value len*/
                101, 114, 115, 101, /* "erse" */
                118, 97, 108, 117, 101 /* value */
            ]
        );
        b.add(b"read", b"value");
        assert_eq!(
            &b.buffer.as_slice()[27..],
            &[
                2, /* shared len*/
                2, /* non shared len*/
                5, /* value len*/
                97, 100, /* "ad" */
                118, 97, 108, 117, 101 /* value */
            ]
        );
        b.add(b"reel", b"value");
        assert_eq!(
            &b.buffer.as_slice()[37..],
            &[
                2, /* shared len*/
                2, /* non shared len*/
                5, /* value len*/
                101, 108, /* "el" */
                118, 97, 108, 117, 101 /* value */
            ]
        );
        b.add(b"recv", b"value");
        assert_eq!(
            &b.buffer.as_slice()[47..],
            &[
                2, /* shared len*/
                2, /* non shared len*/
                5, /* value len*/
                99, 118, /* "cv" */
                118, 97, 108, 117, 101 /* value */
            ]
        );
        assert_eq!(b.counter, 5);
        for i in 0..12u8 {
            b.add(&[i], &[i]);
        }
        assert_eq!(b.counter, 1);
        assert_eq!(b.buffer.len(), 57 + 12 * 5);
        let c = &b.finish().as_slice()[57 + 12 * 5..];
        // because block_restart_interval default value is 16, so the restart point is
        // 57 + 11 * 5 = 112 (16 count), so we got 0 and 112 as restart points.
        assert_eq!(
            c,
            &[
                0, 0, 0, 0, /* 0 restart_points */
                112, 0, 0, 0, /* 112 restart point */
                2, 0, 0, 0 /* restarts length*/
            ]
        );
    }
}
