use crate::common::numerical::ByteMagic;
use crate::index::hash_index::HashIndex;
use crate::index::{Index, IndexInfo};
use crate::schema::schema::Layout;
use crate::storage::blockid::BlockId;
use crate::storage::free_space::FreeMap;
use crate::storage::heap::{HeapPage, PageIter};
use crate::storage::storagemgr::StorageManager;
use crate::storage::tuple::Tuple;
use crate::table::hash_iter::HashIter;
use crate::table::heap_iter::TableIter;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;

/// TableManager is an entity owned by a database that is responsible for executing operations
/// against the heap pages that represents a database table on disk
///
/// It internally uses the Heap Page interface for its responsibilities
pub struct TableManager {
    pub free_map: FreeMap,
    table_blocks: Vec<BlockId>,
    storage_mgr: Rc<RefCell<StorageManager>>,
    layout: Rc<Layout>,
    indexes: HashMap<String, Index>,
}

impl TableManager {
    /// Creates a TableManager instance from the Blocks that represent the table,
    /// a refrence to the Database Storage Manager , a Free Space Map (FSM) read from disk and a reference
    /// to the tuple layout for this table.
    /// If no (FSM) is passed , new() creates it and saves it to disk
    pub fn new(
        blocks: Vec<BlockId>,
        storage_mgr: Rc<RefCell<StorageManager>>,
        free_map: Option<FreeMap>,
        layout: Rc<Layout>,
        indexes: Vec<IndexInfo>,
    ) -> Self {
        let free_map = if free_map.is_none() {
            let mut strg_mgr = storage_mgr.borrow_mut();
            let mut map = FreeMap::new();
            for block in &blocks {
                let blk_header = strg_mgr.read_raw(block, 4);
                let space_start = blk_header.as_slice().extract_u16(0);
                let space_end = blk_header.as_slice().extract_u16(2);
                let space = (space_end - space_start);
                if space != 0 {
                    map.add_blockspace(space, block);
                };
            }
            map
        } else {
            free_map.unwrap()
        };

        let indexes = indexes
            .into_iter()
            .map(|idx| {
                let column = idx.column.clone();
                let blks = storage_mgr
                    .borrow_mut()
                    .file_blks(idx.index_file_path.clone());
                (column, Index::load_index(idx, blks))
            })
            .collect();

        Self {
            free_map,
            table_blocks: blocks,
            storage_mgr,
            layout,
            indexes,
        }
    }

    pub fn from_file(
        storage_mgr: Rc<RefCell<StorageManager>>,
        filepath: PathBuf,
        layout: Rc<Layout>,
        indexes: Vec<IndexInfo>,
    ) -> Self {
        let blks = storage_mgr.borrow().file_blks(filepath);
        Self::new(blks, storage_mgr, None, layout, indexes)
    }

    /// Marks a tuple for deletion using it's BlockId and Page slot number
    ///
    /// When a tuple is marked for deletion , it is not acutally deleted but only marked and is actually
    /// removed during compaction in vacuuming
    pub fn delete_tuple(&self, blk: &BlockId, slot_num: usize) {
        let mut heap_page = self.get_heap_page(blk);
        heap_page.mark_delete(slot_num)
    }

    /// Get multiple fields of a tuple as bytes , reinterpreting them is the responsibility of the caller
    pub fn get_fields(
        &self,
        blk: &BlockId,
        slot_num: usize,
        field_names: Vec<String>,
    ) -> HashMap<String, Option<Vec<u8>>> {
        let heap_page = self.get_heap_page(blk);
        heap_page.get_multiple_fields(field_names, slot_num as u16)
    }
    /// Get a single field of a tuple as bytes , reinterpreting it is the responsibility of the caller
    pub fn get_field(&self, blk: &BlockId, slot_num: usize, field_name: &str) -> Option<Vec<u8>> {
        let heap_page = self.get_heap_page(blk);
        heap_page.get_field(field_name, slot_num as u16)
    }

    pub fn get_tuple(&self, blk: &BlockId, slot_num: usize) -> HashMap<String, Option<Vec<u8>>> {
        self.get_fields(
            blk,
            slot_num,
            self.layout
                .index_map()
                .keys()
                .map(|k| k.to_string())
                .collect::<Vec<String>>(),
        )
    }
    pub fn get_heapfile_name(&self) -> &str {
        self.table_blocks[0].filename.as_str()
    }

    /// Insert a tuple into a table
    /// Searches the FSM first for a block that has the least free space required for a tuple to insert
    /// it in , if None exists , the Heap File representing the table is extended by 1 block and the
    /// tuple is inserted into this page and the remaining space in it is added to the FSM
    pub fn try_insert_tuple(&mut self, tuple_bytes: Vec<(String, Option<Vec<u8>>)>) {
        let tuple = Tuple::new(tuple_bytes.clone(), self.layout.clone());
        let target_block = self.free_map.get_smallest_fit(tuple.tuple_size());
        let mut storage_mgr = self.storage_mgr.borrow_mut();
        if let Some((free_size, block)) = target_block {
            let mut frame = storage_mgr.pin(block.clone()).unwrap();
            let mut target_page = HeapPage::new(frame, &block, self.layout.clone());
            target_page.insert_tuple(tuple);
            self.free_map
                .add_blockspace(target_page.free_space(), &block)
        } else {
            let blkid = storage_mgr.extend_file(self.table_blocks[0].filename.as_str());
            self.table_blocks.push(blkid.clone());
            let mut frame = storage_mgr.pin(blkid.clone()).unwrap();
            let mut target_page = HeapPage::new_from_empty(frame, &blkid, self.layout.clone());
            target_page.insert_tuple(tuple);
            self.free_map
                .add_blockspace(target_page.free_space(), &blkid);
        };
    }
    /// Flush the frame holding a BlockId to disk , resetting the necessary stats
    pub fn flush(&self, blk: &BlockId) {
        let mut storage_mgr = self.storage_mgr.borrow_mut();
        let mut frame = storage_mgr.pin(blk.clone()).unwrap();
        storage_mgr.flush_frame(frame);
    }

    /// Flush all the table blocks to disk , resetting the necessary stats for each
    pub fn flush_all(&self) {
        let mut storage_mgr = self.storage_mgr.borrow_mut();
        for blk in &self.table_blocks {
            let mut frame = storage_mgr.pin(blk.clone()).unwrap();
            storage_mgr.flush_frame(frame.clone());
            storage_mgr.unpin(frame);
        }
    }

    /// Compacts each block in the heap file representing a table by rewriting them in place ,
    /// discarding deleted tuples
    pub fn vacuum(&mut self) {
        let mut storage_mgr = self.storage_mgr.borrow_mut();
        for block in &self.table_blocks {
            let frame = storage_mgr.pin(block.clone()).unwrap();
            let mut heap_page = HeapPage::new(frame.clone(), block, self.layout.clone());
            heap_page.vacuum();
            storage_mgr.unpin(frame);
        }
    }
    /// Helper function to pin a block to a frame and construct a heap page out of it
    fn get_heap_page(&self, blk: &BlockId) -> HeapPage {
        let mut storage_mgr = self.storage_mgr.borrow_mut();
        let frame = storage_mgr.pin(blk.clone()).unwrap();
        HeapPage::new(frame, blk, self.layout.clone())
    }

    pub fn get_layout(&self) -> Rc<Layout> {
        self.layout.clone()
    }

    /// Creates a TableIter instance that is an sequential iterator over ALL the tuples in a table
    pub fn heapscan_iter(&self) -> TableIter {
        TableIter::new(
            &self.table_blocks,
            self.storage_mgr.clone(),
            self.layout.clone(),
        )
    }

    pub fn hashscan_iter(&self, index_field: &str, key: &[u8]) -> Option<HashIter> {
        if let Some(idx) = self.indexes.get(index_field) {
            if let Index::Hash(idx) = idx {
                return Some(HashIter::new(
                    &self,
                    idx,
                    key,
                    self.storage_mgr.borrow_mut(),
                ));
            }
        }
        None
    }

    pub fn add_index_block(&mut self, blk: &BlockId) {
        self.table_blocks.push(blk.clone());
    }

    pub fn remove_index_block(&mut self, blk_num: u64) {
        let pos = self
            .table_blocks
            .iter()
            .position(|block| block.block_num == blk_num)
            .unwrap();
        self.table_blocks.remove(pos);
    }
}
