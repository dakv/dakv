pub use skiplist::SkipList;
pub use skiplist_iter::SkipListIter;

mod skiplist;
mod skiplist_iter;
mod skipnode;

pub const K_MAX_HEIGHT: usize = 12;
