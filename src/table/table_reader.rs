use crate::db::{
    CompressionType, DError, DResult, ErrorIterator, Options, ReadOptions, Saver, TwoLevelIter,
    BLOCK_TRAILER_SIZE,
};
use crate::env::RandomAccessFile;
use crate::table::block::{Block, BlockContents};
use crate::table::block_handle::BlockHandle;
use crate::table::filter_block::FilterBlockReader;
use crate::table::footer::{Footer, ENCODED_LENGTH};
use crate::utils::cmp::BytewiseComparatorImpl;
use crate::utils::coding::{decode_u32_le, encode_u64_le};
use crate::utils::compressor::generate_compressor;
use crate::utils::crc32c::{unmask, value};
use crate::utils::iter::Iter;
use slog::info;
use std::fmt;
use std::sync::Arc;

/// file could be PosixRandomAccessFile or PosixMmapReadableFile
/// * PosixMmapReadableFile cacheable = false, heap_allocated = false
/// * PosixRandomAccessFile cacheable = true, heap_allocated = true
/// Read block from file, the offset and length depends on `BlockHandle`.
pub fn read_block(
    file: Arc<dyn RandomAccessFile>,
    opt: ReadOptions,
    handle: &BlockHandle,
    result: &mut BlockContents,
) -> DResult<()> {
    result.data = vec![];
    result.cacheable = false;
    result.heap_allocated = false;
    let n = handle.get_size() as usize;
    let mut buf = vec![0; n + BLOCK_TRAILER_SIZE];
    let ret = file.read(handle.get_offset() as usize, buf.as_mut_slice())?;

    if ret != n + BLOCK_TRAILER_SIZE {
        return Err(DError::CustomError("truncated block read"));
    }
    // Check the crc of the type and the block contents
    let data = buf.as_slice();
    if opt.verify_checksums {
        let expect_crc = unmask(decode_u32_le(&data[n + 1..]));
        let actual = value(&data[0..n + 1]);
        if actual != expect_crc {
            return Err(DError::ChecksumMismatch);
        }
    }
    let compressor_type: CompressionType = data[n].into();
    let compressor = generate_compressor(compressor_type);
    match compressor_type {
        CompressionType::NoCompress => {
            result.data = Vec::from(&data[0..n]);
            result.cacheable = true;
            result.heap_allocated = true;
            // TODO: heap_allocated is unused
            // result->heap_allocated = true;
            // result->cachable = true;
        }
        CompressionType::Snappy => match compressor.decompress(data) {
            Ok(buf) => {
                result.data = buf;
                result.cacheable = true;
                result.heap_allocated = true;
            }
            Err(_) => {
                // decompress failed
                return Err(DError::CustomError(
                    "Failed to decompress, corrupted compressed block contents",
                ));
            }
        },
        CompressionType::Zstd => {
            unimplemented!()
        }
    }

    Ok(())
}

/// Read from cache or file, and return the iterator.
/// use `opt` and `file` to replace Arc<Mutex<Table>>
fn block_reader(inner: Table, read_opt: ReadOptions, index_value: &[u8]) -> Box<dyn Iter> {
    let opt = inner.options.clone();
    let file = inner.file.clone();
    let cache_id = inner.cache_id;

    let mut handle = BlockHandle::new();
    let mut s = index_value;
    let result = handle.decode(&mut s);

    let mut block = None;
    if result.is_ok() {
        let mut contents = BlockContents::new();
        // if set up block cache, try to fetch block from cache.
        //     if cache miss, read from file, and store into cache.
        // if not set up block cache, read file.
        if let Some(c) = opt.block_cache.clone() {
            let mut cache_key_buffer = vec![0; 16];

            encode_u64_le(&mut cache_key_buffer, cache_id);
            encode_u64_le(
                &mut cache_key_buffer.as_mut_slice()[8..],
                handle.get_offset(),
            );
            let key = cache_key_buffer;
            let c = c.lock().unwrap();
            let cache_handle = c.lookup(&key);
            if let Some(bb) = cache_handle {
                info!(opt.info_log.as_ref().unwrap(), "block cache hit");
                block = Some(bb);
            } else {
                info!(opt.info_log.as_ref().unwrap(), "block cache miss");
                let s = read_block(file, read_opt.clone(), &handle, &mut contents);
                if s.is_ok() {
                    let contents = Arc::new(contents);
                    let b = Block::new(contents.clone());
                    if contents.cacheable && read_opt.fill_cache {
                        c.insert(key, b.clone());
                    }
                    block = Some(b);
                }
            }
        } else {
            let s = read_block(file, read_opt, &handle, &mut contents);
            if s.is_ok() {
                block = Some(Block::new(Arc::new(contents)));
            }
        }
    }

    // data iterator
    if block.is_some() {
        block.as_ref().unwrap().new_iter(opt.comparator.clone())
    } else {
        Box::new(ErrorIterator::new())
    }
}

/// Use Table to read data.
#[derive(Clone)]
pub struct Table {
    cache_id: u64,
    options: Options,
    file: Arc<dyn RandomAccessFile>,
    index_block: Block,
    meta_index_handle: BlockHandle,
    filter: Option<Arc<FilterBlockReader>>,
    // TODO: raw pointer?
    // pub filter_data: *const u8,
    block: Option<Arc<BlockContents>>, // Ensure that BlockContents are not released..
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        ignore!(write!(f, "{} ", self.cache_id));
        ignore!(write!(f, "{:?} ", self.index_block));
        ignore!(write!(f, "{} ", self.filter.is_some()));
        ignore!(write!(f, "{} ", self.block.is_some()));
        write!(f, "{}", self.meta_index_handle)
    }
}

unsafe impl Send for Table {}

unsafe impl Sync for Table {}

impl Table {
    fn new(
        file: Arc<dyn RandomAccessFile>,
        index_block: Block,
        options: Options,
        meta_index_handle: BlockHandle,
        cache_id: u64,
        block: Option<Arc<BlockContents>>,
        filter: Option<Arc<FilterBlockReader>>,
    ) -> Self {
        Self {
            cache_id,
            options,
            file,
            index_block,
            meta_index_handle,
            filter,
            block,
        }
    }

    /// Use Table::open to return a table object.
    /// 1. read footer from the end of file.
    /// 2. init index_block contents
    /// 3. return Table
    pub fn open(options: Options, file: Arc<dyn RandomAccessFile>, size: usize) -> DResult<Table> {
        if size < ENCODED_LENGTH {
            return Err(DError::CustomError("File is too short to be an table file"));
        }
        // Read the 48 bytes from the end of the file and get footer.
        let mut footer_input = vec![0; ENCODED_LENGTH];
        file.read(size - ENCODED_LENGTH, footer_input.as_mut_slice())?;
        let mut footer = Footer::new();
        footer.decode(footer_input.as_slice())?;

        // Read the index block and store it into `index_block_contents`
        let mut index_block_contents = BlockContents::new();
        let mut opt = ReadOptions::new();
        if options.paranoid_checks {
            opt.verify_checksums = true;
        }
        read_block(
            file.clone(),
            opt,
            footer.get_index_handle(),
            &mut index_block_contents,
        )?;

        let index_block = Block::new(Arc::new(index_block_contents));
        let cache_id = if let Some(bc) = &options.block_cache {
            bc.lock().unwrap().new_id()
        } else {
            0
        };
        let (block, filter) = Self::read_meta(options.clone(), file.clone(), &footer);
        let t = Table::new(
            file,
            index_block,
            options,
            footer.get_meta_index_handle(),
            cache_id,
            block,
            filter,
        );
        Ok(t)
    }

    pub fn new_iter(&self, option: ReadOptions) -> impl Iter {
        // init_data_block method may be called multiple times, so just give the Rc pointers.
        // Otherwise, it cannot be put in FnMut.
        let inner = self.clone();
        TwoLevelIter::new(
            self.index_block.new_iter(self.options.comparator.clone()),
            option,
            move |read_opt, s| block_reader(inner.clone(), read_opt, s),
        )
    }

    pub fn approximate_offset_of(&self, key: &[u8]) -> u64 {
        let mut index_iter = self.index_block.new_iter(self.options.comparator.clone());
        index_iter.seek(key);
        return if index_iter.valid() {
            let mut handle = BlockHandle::default();
            let input = index_iter.value();
            let mut tmp = input;
            let s = handle.decode(&mut tmp);

            if s.is_ok() {
                handle.get_offset()
            } else {
                self.meta_index_handle.get_offset()
            }
        } else {
            self.meta_index_handle.get_offset()
        };
    }

    fn read_meta(
        options: Options,
        file: Arc<dyn RandomAccessFile>,
        footer: &Footer,
    ) -> (Option<Arc<BlockContents>>, Option<Arc<FilterBlockReader>>) {
        // Do not need any metadata
        if options.filter_policy.is_none() {
            return (None, None);
        }
        let mut opt = ReadOptions::new();
        if options.paranoid_checks {
            opt.verify_checksums = true;
        }

        let mut contents = BlockContents::new();
        if read_block(
            file.clone(),
            opt,
            &footer.get_meta_index_handle(),
            &mut contents,
        )
        .is_err()
        {
            // Do not propagate errors since meta info is not needed for operation
            return (None, None);
        }

        let meta = Block::new(Arc::new(contents));
        let mut iter = meta.new_iter(Arc::new(BytewiseComparatorImpl::new()));
        let key = format!("filter.{}", options.filter_policy.as_ref().unwrap().name());
        iter.seek(key.as_bytes());
        if iter.valid() && iter.key() == key.as_bytes() {
            return Self::read_filter(options, file, iter.value());
        }
        (None, None)
    }

    /// Read the filter.
    fn read_filter(
        options: Options,
        file: Arc<dyn RandomAccessFile>,
        filter_handle_value: &[u8],
    ) -> (Option<Arc<BlockContents>>, Option<Arc<FilterBlockReader>>) {
        let mut v = filter_handle_value;
        let mut filter_handle = BlockHandle::default();
        if filter_handle.decode(&mut v).is_err() {
            return (None, None);
        }
        let mut opt = ReadOptions::new();
        if options.paranoid_checks {
            opt.verify_checksums = true;
        }
        let mut block = BlockContents::new();
        if read_block(file.clone(), opt, &filter_handle, &mut block).is_err() {
            return (Some(Arc::new(block)), None);
        }

        if block.heap_allocated {
            // self.filter_data = block.data.as_ptr();
        }
        let filter = FilterBlockReader::new(options.filter_policy.clone().unwrap(), &block.data);
        (Some(Arc::new(block)), Some(Arc::new(filter)))
    }

    /// Try to check the key in the filter, if not exist return,
    /// else read the file seek the key, store it into the `Saver`
    pub fn internal_get(
        &self,
        read_opt: ReadOptions,
        k: &[u8],
        arg: &mut Saver,
        mut func: impl FnMut(&mut Saver, &[u8], &[u8]),
    ) -> DResult<()> {
        let mut iter = self.index_block.new_iter(self.options.comparator.clone());
        iter.seek(k);
        if iter.valid() {
            let handle_value = iter.value();
            let mut hv = handle_value;
            let filter = self.filter.clone();

            let mut handle = BlockHandle::default();
            if filter.is_some()
                && handle.decode(&mut hv).is_ok()
                && !filter
                    .as_ref()
                    .unwrap()
                    .key_may_exist(handle.get_offset(), k)
            {

                // Bloom filter check not exist, so return not found.
                // Not found
            } else {
                let mut block_iter = block_reader(self.clone(), read_opt, iter.value());
                block_iter.seek(k);
                if block_iter.valid() {
                    func(arg, block_iter.key(), block_iter.value())
                }
                // TODO: s = block_iter->status();
            }
        }
        Ok(())
    }
}
