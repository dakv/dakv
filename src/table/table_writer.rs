use crate::db::DResult;
use crate::db::{CompressionType, Options, BLOCK_TRAILER_SIZE};
use crate::env::WritableFile;
use crate::table::block_builder::BlockBuilder;
use crate::table::block_handle::BlockHandle;
use crate::table::filter_block::FilterBlockWriter;
use crate::table::footer::Footer;
use crate::utils::coding::encode_u32_le;
use crate::utils::compressor::generate_compressor;
use crate::utils::crc32c::{extend, mask, value};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

/// [ compression type 1 Byte] [ crc32 4 Bytes]
pub fn gen_trailer(data: &[u8], t: CompressionType) -> Vec<u8> {
    let mut trailer = vec![0; BLOCK_TRAILER_SIZE];
    trailer[0] = t.into();
    let mut crc = value(data);
    crc = extend(crc, &trailer[0..1]);
    encode_u32_le(&mut trailer[1..], mask(crc));
    trailer
}

pub struct TableBuilder {
    offset: u64,
    closed: bool,
    // If the filter_policy is enabled, filter_block must not be None
    // HACK: use Rc<RefCell> to make compiler happy:
    filter_block: Option<Rc<RefCell<FilterBlockWriter>>>,
    status: DResult<()>,
    opt: Options,
    last_key: Vec<u8>,
    // pending_index_entry is true only if data_block is empty.
    pending_index_entry: bool,
    written_entries_num: u64,
    file: Arc<dyn WritableFile>,
    // HACK: use Rc<RefCell> to make compiler happy:
    data_block: Rc<RefCell<BlockBuilder>>,
    /// store offset and size before last block was written.
    pending_handle: Rc<RefCell<BlockHandle>>,
    index_block: Rc<RefCell<BlockBuilder>>,
}

impl TableBuilder {
    pub fn new(opt: Options, file: Arc<dyn WritableFile>) -> Self {
        let mut filter_block = None;
        if let Some(filter_policy) = opt.filter_policy.clone() {
            filter_block = Some(Rc::new(RefCell::new(FilterBlockWriter::new(filter_policy))));
        }
        let mut index_block_options = opt.clone();
        index_block_options.block_restart_interval = 1;
        let mut s = Self {
            offset: 0,
            data_block: Rc::new(RefCell::new(BlockBuilder::new(opt.clone()))),
            index_block: Rc::new(RefCell::new(BlockBuilder::new(index_block_options))),
            pending_index_entry: false,
            written_entries_num: 0,
            closed: false,
            last_key: Default::default(),
            pending_handle: Rc::new(RefCell::new(BlockHandle::default())),
            status: Ok(()),
            opt,
            filter_block,
            file,
        };
        if let Some(f) = &mut s.filter_block {
            f.borrow_mut().start_block(0);
        }
        s
    }

    /// Write KV data into data block, if the filter policy is enabled, update the filter block.
    /// Add key,value to the table being constructed.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(!self.closed);
        if !self.is_ok() {
            return;
        }

        if self.written_entries_num > 0 {
            // make sure key is greater than last key
            assert!(self.opt.comparator.gt(key, self.last_key.as_slice()));
        }

        // After we write a data block into file, and reset the data block,
        // pending_index_entry will set true
        if self.pending_index_entry {
            assert!(self.data_block.borrow().empty());
            self.last_key = self
                .opt
                .comparator
                .find_shortest_separator(self.last_key.as_slice(), key);

            let mut handle_encoding = vec![];
            self.pending_handle.borrow().encode(&mut handle_encoding);
            self.index_block
                .borrow_mut()
                .add(self.last_key.as_slice(), handle_encoding.as_slice());
            self.pending_index_entry = false;
        }
        // update filter block
        if let Some(f) = &mut self.filter_block {
            f.borrow_mut().add_key(key);
        }

        // write into data block and update last key with current.
        self.last_key = key.to_owned();
        self.written_entries_num += 1;
        self.data_block.borrow_mut().add(key, value);

        let estimated_block_size = self.data_block.borrow().current_size_estimate();
        // Flush the data block if the length exceeds opt.block_size
        if estimated_block_size >= self.opt.block_size {
            self.flush();
        }
    }

    /// Flush the data to ensure that all intermediately buffered
    /// contents reach their destination.
    /// Advanced operation: flush any buffered key/value pairs to file.
    /// Can be used to ensure that two adjacent entries never live in
    /// the same data block.
    fn flush(&mut self) {
        assert!(!self.closed);
        if !self.is_ok() {
            return;
        }
        // Flush only if the data block is not empty.
        if self.data_block.borrow().empty() {
            return;
        }
        assert!(!self.pending_index_entry);

        self.write_block(
            &mut *self.data_block.clone().borrow_mut(),
            &mut *self.pending_handle.clone().borrow_mut(),
        );

        if self.is_ok() {
            self.pending_index_entry = true;
            self.status = self.file.flush();
        }
        // generate filter
        if let Some(f) = &mut self.filter_block {
            f.borrow_mut().start_block(self.offset);
        }
    }

    fn write_block(&mut self, block: &mut BlockBuilder, handle: &mut BlockHandle) {
        assert!(self.is_ok());
        let raw = block.finish();

        let block_contents;
        let mut compression_type = self.opt.compression;
        let compressed_output;

        let compressor = generate_compressor(compression_type);
        match compression_type {
            CompressionType::NoCompress => {
                block_contents = raw.as_slice();
            }
            CompressionType::Snappy => {
                compressed_output = compressor.compress(raw).unwrap();
                // todo error!("")
                if compressed_output.len() < raw.len() - (raw.len() / 8) {
                    compression_type = CompressionType::Snappy;
                    block_contents = compressed_output.as_slice();
                } else {
                    // Snappy not supported, or compressed less than 12.5%, so just
                    // store uncompressed form
                    block_contents = raw;
                    compression_type = CompressionType::NoCompress;
                }
            }
            CompressionType::Zstd => {
                unimplemented!()
            }
        }
        self.write_raw_block(block_contents, compression_type, handle);
        block.reset();
    }

    // Write block contents and the trailer with compression type and CRC value.
    fn write_raw_block(
        &mut self,
        block_contents: &[u8],
        type_: CompressionType,
        handle: &mut BlockHandle,
    ) {
        handle.set_offset(self.offset);
        handle.set_size(block_contents.len() as u64);

        self.status = self.file.append(block_contents);
        if self.is_ok() {
            let trailer = gen_trailer(block_contents, type_);
            // write trailer []
            self.status = self.file.append(trailer.as_slice());
            if self.is_ok() {
                self.offset += (block_contents.len() + BLOCK_TRAILER_SIZE) as u64;
            }
        }
    }

    // Finish building the table.  Stops using the file passed to the
    // constructor after this function returns.
    // REQUIRES: finish(), abandon() have not been called
    pub fn finish(&mut self) -> DResult<()> {
        self.flush();
        assert!(!self.closed);
        self.close();

        // Write filter block without compression
        let mut filter_block_handle = BlockHandle::default();
        if self.is_ok() {
            if let Some(f) = &mut self.filter_block.clone() {
                let mut f = f.borrow_mut();
                let block = f.finish();

                self.write_raw_block(
                    block.as_slice(),
                    CompressionType::NoCompress,
                    &mut filter_block_handle,
                );
            }
        }

        // Write meta index block (index of the filter block).
        let mut meta_index_block_handle = BlockHandle::default();
        if self.is_ok() {
            let mut meta_index_block = BlockBuilder::new(self.opt.clone());
            if self.filter_block.is_some() {
                let mut key = String::from("filter.");
                if let Some(f) = &self.opt.filter_policy {
                    let name = f.name();
                    key.push_str(name);
                }

                let encoding = &mut vec![];
                filter_block_handle.encode(encoding);

                meta_index_block.add(key.as_bytes(), encoding.as_slice());
            }
            self.write_block(&mut meta_index_block, &mut meta_index_block_handle);
        }

        // Write index block
        let mut index_block_handle = BlockHandle::default();
        if self.is_ok() {
            if self.pending_index_entry {
                self.last_key = self
                    .opt
                    .comparator
                    .find_short_successor(self.last_key.as_slice());

                let encoding = &mut vec![];

                self.pending_handle.borrow_mut().encode(encoding);
                self.index_block
                    .borrow_mut()
                    .add(self.last_key.as_slice(), encoding.as_slice());
                self.pending_index_entry = false;
            }
            self.write_block(
                &mut *self.index_block.clone().borrow_mut(),
                &mut index_block_handle,
            );
        }

        // Write footer
        if self.is_ok() {
            let mut footer = Footer::new();
            footer.set_meta_index_handle(meta_index_block_handle);
            footer.set_index_handle(index_block_handle);

            let encoding = &mut vec![];
            footer.encode(encoding);
            self.status = self.file.append(encoding.as_slice());
            if self.status.is_ok() {
                self.status = self.file.flush();
            }
            if self.is_ok() {
                self.offset += encoding.len() as u64;
            }
        }
        self.status.clone()
    }

    fn close(&mut self) {
        self.closed = true;
    }

    fn is_ok(&self) -> bool {
        self.status.is_ok()
    }

    // Indicate that the contents of this builder should be abandoned.  Stops
    // using the file passed to the constructor after this function returns.
    // If the caller is not going to call finish(), it must call abandon()
    // before destroying this builder.
    pub fn abandon(&mut self) {
        assert!(!self.closed);
        self.closed = true;
    }

    // Number of calls to add() so far.
    pub fn num_entries(&self) -> u64 {
        self.written_entries_num
    }

    // Size of the file generated so far.  If invoked after a successful
    // finish() call, returns the size of the final generated file.
    pub fn file_size(&self) -> u64 {
        self.offset
    }
}
