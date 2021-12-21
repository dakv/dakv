use crate::db::{table_file_name, DResult, FileMetaData, Options, ReadOptions};
use crate::env::Env;
use crate::table::TableBuilder;
use crate::table::TableCache;
use crate::utils::iter::Iter;
use std::sync::Arc;

pub fn build_table(
    db: &str,
    opt: Options,
    meta: &mut FileMetaData,
    table_cache: Arc<TableCache>,
    mut mem_iter: Box<dyn Iter>,
    env: Arc<dyn Env + Send + Sync>,
) -> DResult<()> {
    meta.set_file_size(0);
    mem_iter.seek_to_first();

    let file_name = table_file_name(db, meta.get_file_number());
    if mem_iter.valid() {
        let write_file = env.new_writable_file(&file_name).unwrap();

        let mut builder = TableBuilder::new(opt, write_file);
        // First key is the smallest.
        meta.set_smallest_from_slice(mem_iter.key());
        while mem_iter.valid() {
            let key = mem_iter.key();
            // Set the current key to the largest until the memory iter becomes invalid.
            meta.set_largest_from_slice(key);
            builder.add(key, mem_iter.value());
            mem_iter.next();
        }

        builder.finish()?;

        meta.set_file_size(builder.file_size());
        assert!(meta.get_file_size() > 0);

        // Verify the table is usable
        // Also, make the table the LRU Cache's first item.
        table_cache.new_iter(
            ReadOptions::new(),
            meta.get_file_number(),
            meta.get_file_size(),
        );
        // remove invalid file
        if meta.get_file_size() == 0 {
            env.delete_file(&file_name)?;
        }
    }
    Ok(())
}
