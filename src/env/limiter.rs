use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct Limiter {
    acquires_allowed: Arc<AtomicI64>,
}

impl Limiter {
    pub fn new(max_acquires: i64) -> Self {
        Limiter {
            acquires_allowed: Arc::new(AtomicI64::new(max_acquires)),
        }
    }
    pub fn acquire(&self) -> bool {
        let old_acquires_allowed = self.acquires_allowed.fetch_sub(1, Ordering::Relaxed);
        if old_acquires_allowed > 0 {
            return true;
        }
        self.acquires_allowed.fetch_add(1, Ordering::Relaxed);
        false
    }
    pub fn release(&self) {
        self.acquires_allowed.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for Limiter {
    fn default() -> Self {
        Self::new(max_open_files())
    }
}

pub fn max_open_files() -> i64 {
    100
}

#[cfg(test)]
mod test {
    use crate::env::limiter::Limiter;

    #[test]
    fn test_limiter_acquire() {
        let l = Limiter::new(1);
        assert_eq!(l.acquire(), true);
        assert_eq!(l.acquire(), false);
    }

    #[test]
    fn test_limiter_release() {
        let l = Limiter::new(1);
        assert_eq!(l.acquire(), true);
        assert_eq!(l.acquire(), false);
        l.release();
        assert_eq!(l.acquire(), true);
    }
}
