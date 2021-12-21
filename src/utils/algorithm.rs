use std::cmp::Ordering;
use std::cmp::Ordering::{Greater, Less};

/// Returns the index pointing to the first element in the range [first, last)
/// that is not less than (i.e. greater or equal to) value, or last if no such element is found.
/// Use binary search.
pub trait LowerBound {
    type Item;

    fn lower_bound(&self, key: &Self::Item) -> usize
    where
        Self::Item: Ord,
    {
        self.lower_bound_by(|a, b| a.cmp(b), key)
    }

    /// Support custom comparator so that we can compare the item with their private member easily.
    /// To use this method, just need to implement custom comparator.
    /// It's more flexible than `ordslice`.
    fn lower_bound_by<F>(&self, f: F, key: &Self::Item) -> usize
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering;
}

/// Returns the index pointing to the first element in the range [first, last)
/// that is greater than value, or last if no such element is found.
/// Use binary search.
pub trait UpperBound {
    type Item;

    fn upper_bound(&self, key: &Self::Item) -> usize
    where
        Self::Item: Ord,
    {
        self.upper_bound_by(|a, b| a.cmp(b), key)
    }

    fn upper_bound_by<F>(&self, f: F, key: &Self::Item) -> usize
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering;
}

impl<T> LowerBound for [T] {
    type Item = T;

    fn lower_bound_by<F>(&self, f: F, key: &T) -> usize
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering,
    {
        let mut start = 0usize;
        let mut last = self.len();
        if last == 0 {
            return 0;
        }
        while start < last {
            let mid = (start + last) / 2;
            if f(self.get(mid).unwrap(), key) != Less {
                last = mid;
            } else {
                start = mid + 1;
            }
        }
        start
    }
}

impl<T> UpperBound for [T] {
    type Item = T;

    fn upper_bound_by<F>(&self, f: F, key: &T) -> usize
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering,
    {
        let mut start = 0usize;
        let mut last = self.len();
        if last == 0 {
            return 0;
        }
        while start < last {
            let mid = (start + last) / 2;
            if f(self.get(mid).unwrap(), key) != Greater {
                start = mid + 1
            } else {
                last = mid;
            }
        }
        start
    }
}

#[cfg(test)]
mod test {
    use crate::utils::algorithm::{LowerBound, UpperBound};

    #[test]
    fn test_lower_bound() {
        let a = [10, 11, 13, 13, 15];
        assert_eq!(a.lower_bound(&9), 0);
        assert_eq!(a.lower_bound(&10), 0);
        assert_eq!(a.lower_bound(&11), 1);
        assert_eq!(a.lower_bound(&12), 2);
        assert_eq!(a.lower_bound(&13), 2);
        assert_eq!(a.lower_bound(&14), 4);
        assert_eq!(a.lower_bound(&15), 4);
        assert_eq!(a.lower_bound(&16), 5);
    }

    #[test]
    fn test_upper_bound() {
        let a = [10, 11, 13, 13, 15];
        assert_eq!(a.upper_bound(&9), 0);
        assert_eq!(a.upper_bound(&10), 1);
        assert_eq!(a.upper_bound(&11), 2);
        assert_eq!(a.upper_bound(&12), 2);
        assert_eq!(a.upper_bound(&13), 4);
        assert_eq!(a.upper_bound(&14), 4);
        assert_eq!(a.upper_bound(&15), 5);
        assert_eq!(a.upper_bound(&16), 5);
    }
}
