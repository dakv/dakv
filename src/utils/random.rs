use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

pub trait RandomGenerator {
    fn next(&self) -> u32;
    // Returns a uniformly distributed value in the range [0..n-1]
    // REQUIRES: n > 0
    fn uniform(&self, n: u32) -> u32 {
        self.next() % n
    }
    // Randomly returns true ~"1/n" of the time, and false otherwise.
    // REQUIRES: n > 0
    fn one_in(&self, n: u32) -> bool {
        (self.next() % n) == 0
    }
    // Skewed: pick "base" uniformly from range [0,max_log] and then
    // return "base" random bits.  The effect is to pick a number in the
    // range [0,2^max_log-1] with exponential bias towards smaller numbers.
    fn skewed(&self, max_log: u32) -> u32 {
        let tmp = 1u32.wrapping_shl(self.uniform(max_log + 1));
        self.uniform(tmp)
    }
}

#[derive(Clone)]
pub struct Random {
    seed: Arc<AtomicU32>,
}

impl Random {
    pub fn new(s: u32) -> Random {
        let mut seed = s & 0x7fff_ffff_u32;
        if seed == 0 || seed == 2_147_483_647 {
            seed = 1;
        }
        Random {
            seed: Arc::new(AtomicU32::new(seed)),
        }
    }
}

impl RandomGenerator for Random {
    fn next(&self) -> u32 {
        static M: u32 = 2_147_483_647; // 2^31-1
        static A: u64 = 16807; // bits 14, 8, 7, 5, 2, 1, 0
        let product = self.seed.load(Ordering::SeqCst) as u64 * A;
        self.seed.store(
            ((product >> 31) + (product & M as u64)) as u32,
            Ordering::SeqCst,
        );

        if self.seed.load(Ordering::SeqCst) > M {
            self.seed.fetch_sub(M, Ordering::SeqCst);
        }
        self.seed.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::{Random, RandomGenerator};

    #[test]
    fn test_cmp() {
        let s = Random::new(0xdead_beef);
        assert_eq!(s.next(), 1624403320);
        assert!(s.one_in(386994929));
        assert_eq!(s.uniform(1643288587 + 1), 1643288587);
        assert_eq!(s.next(), 2111581289);
        assert_eq!(s.skewed(1993), 17525);
    }
}
