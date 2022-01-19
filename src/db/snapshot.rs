use crate::db::format::SequenceNumber;
use std::sync::{Arc, RwLock};

// Snapshots are kept in a doubly-linked list in the DB.
// TODO: refactor to SnapNode{seq:SequenceNumber}
#[derive(Clone)]
pub struct SnapNode {
    seq: SequenceNumber,
    prev: Option<Arc<RwLock<SnapNode>>>,
    next: Option<Arc<RwLock<SnapNode>>>,
}

impl SnapNode {
    pub fn new(seq: SequenceNumber) -> Self {
        Self {
            seq,
            prev: None,
            next: None,
        }
    }

    pub fn seq(&self) -> SequenceNumber {
        self.seq
    }
}

pub struct SnapshotList {
    // Dummy head of doubly-linked list of snapshots
    // Circular doubly-linked list
    head: Arc<RwLock<SnapNode>>,
}

impl Default for SnapshotList {
    fn default() -> Self {
        let head = Arc::new(RwLock::new(SnapNode::new(0)));
        head.write().unwrap().prev = Some(head.clone());
        head.write().unwrap().next = Some(head.clone());
        Self { head }
    }
}

impl SnapshotList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        let a = self.head.read().unwrap().next.clone().unwrap();
        Arc::ptr_eq(&a, &self.head)
    }

    fn newest(&self) -> Arc<RwLock<SnapNode>> {
        assert!(!self.is_empty());
        self.head.read().unwrap().prev.clone().unwrap()
    }

    pub fn oldest(&self) -> Arc<RwLock<SnapNode>> {
        assert!(!self.is_empty());
        self.head.read().unwrap().next.clone().unwrap()
    }

    pub fn delete(&self, snap: &mut SnapNode) {
        snap.prev.as_mut().unwrap().write().unwrap().next = snap.next.clone();
        snap.next.as_mut().unwrap().write().unwrap().prev = snap.prev.clone();
    }

    pub fn create_and_append(&self, seq: SequenceNumber) -> Arc<RwLock<SnapNode>> {
        assert!(self.is_empty() || self.newest().read().unwrap().seq <= seq);

        let snap = SnapNode::new(seq);
        let s = Arc::new(RwLock::new(snap));
        s.write().unwrap().next = Some(self.head.clone());
        let mut head_prev = self.head.read().unwrap().prev.clone();
        s.write().unwrap().prev = head_prev.clone();
        head_prev.as_mut().unwrap().write().unwrap().next = Some(s.clone());
        s.write()
            .unwrap()
            .next
            .as_mut()
            .unwrap()
            .write()
            .unwrap()
            .prev = Some(s.clone());
        s
    }
}

#[cfg(test)]
mod test {
    use crate::db::snapshot::SnapshotList;

    #[test]
    fn test_snapshot() {
        let s = SnapshotList::new();
        assert!(s.is_empty());

        ignore!(s.create_and_append(1));
        let old = s.oldest();
        assert_eq!(old.read().unwrap().seq, 1);
        assert_eq!(
            old.read()
                .unwrap()
                .next
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .seq,
            0
        );
        assert_eq!(
            old.read()
                .unwrap()
                .prev
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .seq,
            0
        );

        ignore!(s.create_and_append(2));
        let old = s.oldest();
        assert_eq!(old.read().unwrap().seq, 1);
        assert_eq!(
            old.read()
                .unwrap()
                .next
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .seq,
            2
        );
        assert_eq!(
            old.read()
                .unwrap()
                .prev
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .seq,
            0
        );
        let new = s.newest();
        assert_eq!(new.read().unwrap().seq, 2);
        assert_eq!(
            new.read()
                .unwrap()
                .next
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .seq,
            0
        );
        assert_eq!(
            new.read()
                .unwrap()
                .prev
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .seq,
            1
        );

        s.delete(&mut *new.write().unwrap());
        let new = s.newest();
        assert_eq!(new.read().unwrap().seq, 1);
        assert_eq!(
            new.read()
                .unwrap()
                .next
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .seq,
            0
        );
        assert_eq!(
            new.read()
                .unwrap()
                .prev
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .seq,
            0
        );
    }
}
