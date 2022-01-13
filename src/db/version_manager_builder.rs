use crate::db::version::Version;
use crate::db::version_edit::VersionEdit;
use crate::db::{FileMetaData, LEVEL_NUMBER};
use crate::utils::algorithm::UpperBound;
use crate::utils::cmp::InternalKeyComparator;
use hashbrown::HashSet;
use rb_tree::{Comparator, RBSet};
use std::cmp::Ordering;
use std::mem::MaybeUninit;
use std::sync::Arc;

#[derive(Clone)]
struct LevelState {
    pub deleted_files: HashSet<u64>,
    pub new_files: RBSet<Arc<FileMetaData>, BySmallestKey>,
}

/// Use `Builder` to create a new version, NewVersion = CurrentVersion + VersionEdit.
/// thus it needs to read the current version of VersionManager as `base`,
/// also `Builder` update the compact_pointer value of VersionManager.
pub(crate) struct Builder<'a> {
    levels: [LevelState; LEVEL_NUMBER],
    icmp: Arc<dyn InternalKeyComparator>,
    base: &'a Version,
    // read files of current version.
    compact_pointer: &'a mut Vec<Vec<u8>>,
}

#[derive(Clone)]
struct BySmallestKey {
    internal_comparator: Arc<dyn InternalKeyComparator>,
}

impl BySmallestKey {
    pub fn new(internal_comparator: Arc<dyn InternalKeyComparator>) -> Self {
        Self {
            internal_comparator,
        }
    }
}

impl Comparator<Arc<FileMetaData>> for BySmallestKey {
    fn cmp(&self) -> Box<dyn Fn(&Arc<FileMetaData>, &Arc<FileMetaData>) -> Ordering> {
        // clone the internal_comparator and move it into closure
        let icmp = self.internal_comparator.clone();
        Box::new(move |a: &Arc<FileMetaData>, b: &Arc<FileMetaData>| {
            let ret = icmp.compare_internal_key(a.get_smallest(), b.get_smallest());
            if ret != Ordering::Equal {
                ret
            } else {
                a.get_file_number().cmp(&b.get_file_number())
            }
        })
    }
}

// A helper so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
impl<'a> Builder<'a> {
    pub fn new(
        base: &'a Version,
        icmp: Arc<dyn InternalKeyComparator>,
        compact_pointer: &'a mut Vec<Vec<u8>>,
    ) -> Self {
        let cmp = BySmallestKey::new(icmp.clone());

        let levels = {
            // Create an array of uninitialized values.
            let mut array: [MaybeUninit<LevelState>; LEVEL_NUMBER] =
                unsafe { MaybeUninit::uninit().assume_init() };
            for (_, element) in array.iter_mut().enumerate() {
                let item = LevelState {
                    deleted_files: HashSet::new(),
                    new_files: RBSet::new(cmp.clone()),
                };
                *element = MaybeUninit::new(item);
            }
            unsafe { std::mem::transmute::<_, [LevelState; LEVEL_NUMBER]>(array) }
        };
        Self {
            levels,
            icmp,
            base,
            compact_pointer,
        }
    }

    /// Apply changed files to edit, update compact pointers and deleted files, new files.
    pub fn apply_changes_from_version_edit(&mut self, edit: &VersionEdit) {
        // Update compaction pointer
        for (level, key) in edit.get_compact_pointers().iter() {
            self.compact_pointer[*level as usize] = key.encode().to_owned();
        }
        // Delete files
        for (level, number) in edit.get_delete_files().iter() {
            self.levels[*level as usize].deleted_files.insert(*number);
        }

        // New files
        for (level, new_file) in edit.get_new_files().iter() {
            let mut f = FileMetaData::new();
            f.set_file_number(new_file.get_file_number());
            f.set_file_size(new_file.get_file_size());
            f.set_smallest(new_file.get_smallest().clone());
            f.set_largest(new_file.get_largest().clone());
            // We arrange to automatically compact this file after
            // a certain number of seeks.  Let's assume:
            //   (1) One seek costs 10ms
            //   (2) Writing or reading 1MB costs 10ms (100MB/s)
            //   (3) A compaction of 1MB does 25MB of IO:
            //         1MB read from this level
            //         10-12MB read from next level (boundaries may be misaligned)
            //         10-12MB written to next level
            // This implies that 25 seeks cost the same as the compaction
            // of 1MB of data. I.e., one seek costs approximately the
            // same as the compaction of 40KB of data. We are a little
            // conservative and allow approximately one seek for every 16KB
            // of data before triggering a compaction.
            f.set_allow_seeks(f.get_file_size() as i64 / (1 << 14));
            if f.allow_seeks() < 100 {
                f.set_allow_seeks(100);
            }

            self.levels[*level as usize]
                .deleted_files
                .remove(&f.get_file_number());
            self.levels[*level as usize].new_files.insert(Arc::new(f));
        }
    }

    /// Steps to create a new version.
    /// 1. Create empty version.
    /// 2. Apply the edits.
    /// 3. Save the current state.
    pub(crate) fn save_to(&self) -> [Vec<Arc<FileMetaData>>; LEVEL_NUMBER] {
        let mut files = Default::default();
        let cmp = BySmallestKey::new(self.icmp.clone());

        for level in 0..LEVEL_NUMBER {
            // Merge the set of added files with the set of pre-existing files.
            // Drop any deleted files.  Store the result in *v.
            let base_files: &Vec<Arc<FileMetaData>> = self.base.num_files(level);
            let mut base_iter = 0usize;
            let base_end = base_files.len();

            for new_file in self.levels[level].new_files.iter() {
                // Add all smaller files listed in base_
                let mut bpos =
                    base_files.as_slice()[base_iter..base_end].upper_bound_by(cmp.cmp(), new_file);
                // bpos need add base_iter.
                bpos += base_iter;

                while base_iter != bpos {
                    self.maybe_add_file(level, base_files[base_iter].clone(), &mut files);
                    base_iter += 1;
                }
                self.maybe_add_file(level, new_file.clone(), &mut files);
            }
            // Add remaining base files
            while base_iter != base_end {
                self.maybe_add_file(level, base_files[base_iter].clone(), &mut files);
                base_iter += 1;
            }
        }
        files
    }

    /// Add file to version.files
    fn maybe_add_file(
        &self,
        level: usize,
        file_meta: Arc<FileMetaData>,
        v: &mut [Vec<Arc<FileMetaData>>; LEVEL_NUMBER],
    ) {
        if self.levels[level]
            .deleted_files
            .contains(&file_meta.get_file_number())
        {
            // File is deleted: do nothing
        } else {
            let files = &mut v[level];
            if level > 0 && !files.is_empty() {
                // Must not overlap
                assert_eq!(
                    self.icmp.clone().compare_internal_key(
                        files[files.len() - 1].get_largest(),
                        file_meta.get_smallest(),
                    ),
                    Ordering::Less
                );
            }
            files.push(file_meta);
        }
    }
}

#[cfg(test)]
mod test {
    use crate::db::format::ValueType;
    use crate::db::key::InternalKey;
    use crate::db::version_manager_builder::BySmallestKey;
    use crate::db::FileMetaData;
    use crate::utils::cmp::InternalKeyComparatorImpl;
    use rb_tree::RBSet;
    use std::sync::Arc;

    #[test]
    fn test_by_smallest_key() {
        let cmp = InternalKeyComparatorImpl::default();
        let mut set = RBSet::new(BySmallestKey::new(Arc::new(cmp)));

        let internal_key1 = InternalKey::new(b"a", 1, ValueType::TypeValue);
        let internal_key2 = InternalKey::new(b"b", 2, ValueType::TypeValue);
        let internal_key3 = InternalKey::new(b"a", 3, ValueType::TypeValue);

        // Test InternalKeyComparator: different user_key
        let mut f = FileMetaData::new();
        f.set_smallest(internal_key1.clone());
        let file_meta_data1 = Arc::new(f);
        set.insert(file_meta_data1);

        let mut f = FileMetaData::new();
        f.set_smallest(internal_key2.clone());
        let file_meta_data2 = Arc::new(f);
        set.insert(file_meta_data2);
        // smallest:'a',1,1 file_number:0
        // smallest:'b',2,1 file_number:0
        assert_eq!(
            set.iter()
                .map(|x| format!("{}", x.to_string()))
                .collect::<Vec<_>>()
                .join(" "),
            r###"smallest:["a",1,TypeValue] num:0 smallest:["b",2,TypeValue] num:0"###
        );
        // Test InternalKeyComparator: same user_key, different sequence
        let mut f = FileMetaData::new();
        f.set_smallest(internal_key3.clone());
        let file_meta_data3 = Arc::new(f);
        set.insert(file_meta_data3);
        // smallest:'a',3,1 file_number:0
        // smallest:'a',1,1 file_number:0
        // smallest:'b',2,1 file_number:0
        assert_eq!(
            set.iter()
                .map(|x| format!("{}", x.to_string()))
                .collect::<Vec<_>>()
                .join(" "),
            r###"smallest:["a",3,TypeValue] num:0 smallest:["a",1,TypeValue] num:0 smallest:["b",2,TypeValue] num:0"###
        );

        // Test BySmallestKey: same smallest InternalKey, different file_number
        let mut f = FileMetaData::new();
        f.set_file_number(1);
        f.set_smallest(internal_key3.clone());
        let file_meta_data4 = Arc::new(f);
        set.insert(file_meta_data4);
        // smallest:'a',3,1 file_number:0
        // smallest:'a',3,1 file_number:1
        // smallest:'a',1,1 file_number:0
        // smallest:'b',2,1 file_number:0
        assert_eq!(
            set.iter()
                .map(|x| format!("{}", x.to_string()))
                .collect::<Vec<_>>()
                .join(" "),
            r###"smallest:["a",3,TypeValue] num:0 smallest:["a",3,TypeValue] num:1 smallest:["a",1,TypeValue] num:0 smallest:["b",2,TypeValue] num:0"###
        );
    }
}
