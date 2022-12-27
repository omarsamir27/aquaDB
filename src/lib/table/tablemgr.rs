use crate::common::numerical::ByteMagic;
use crate::schema::schema::Layout;
use crate::storage::blockid::BlockId;
use crate::storage::free_space::FreeMap;
use crate::storage::heap::{HeapPage, PageIter};
use crate::storage::storagemgr::StorageManager;
use crate::storage::tuple::Tuple;
use std::cell::RefCell;
use std::collections::HashMap;
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

        Self {
            free_map,
            table_blocks: blocks,
            storage_mgr,
            layout,
        }
    }

    /// Marks a tuple for deletion using it's BlockId and Page slot number
    ///
    /// When a tuple is marked for deletion , it is not acutally deleted but only marked and is actually
    /// removed during compaction in vacuuming
    pub fn delete_tuple(&mut self, blk: &BlockId, slot_num: usize) {
        let mut heap_page = self.get_heap_page(blk);
        heap_page.mark_delete(slot_num)
    }

    /// Get multiple fields of a tuple as bytes , reinterpreting them is the responsibility of the caller
    pub fn get_fields(
        &mut self,
        blk: &BlockId,
        slot_num: usize,
        field_names: Vec<String>,
    ) -> HashMap<String, Option<Vec<u8>>> {
        let heap_page = self.get_heap_page(blk);
        heap_page.get_multiple_fields(field_names, slot_num as u16)
    }
    /// Get a single field of a tuple as bytes , reinterpreting it is the responsibility of the caller
    pub fn get_field(
        &mut self,
        blk: &BlockId,
        slot_num: usize,
        field_name: &str,
    ) -> Option<Vec<u8>> {
        let heap_page = self.get_heap_page(blk);
        heap_page.get_field(field_name, slot_num as u16)
    }

    /// Insert a tuple into a table
    /// Searches the FSM first for a block that has the least free space required for a tuple to insert
    /// it in , if None exists , the Heap File representing the table is extended by 1 block and the
    /// tuple is inserted into this page and the remaining space in it is added to the FSM
    pub fn try_insert_tuple(&mut self, tuple_bytes: Vec<(String, Option<Vec<u8>>)>) {
        let tuple = Tuple::new(tuple_bytes, self.layout.clone());
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
    pub fn flush(&mut self, blk: &BlockId) {
        let mut storage_mgr = self.storage_mgr.borrow_mut();
        let mut frame = storage_mgr.pin(blk.clone()).unwrap();
        storage_mgr.flush_frame(frame);
    }

    /// Flush all the table blocks to disk , resetting the necessary stats for each
    pub fn flush_all(&mut self) {
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
    fn get_heap_page(&mut self, blk: &BlockId) -> HeapPage {
        let mut storage_mgr = self.storage_mgr.borrow_mut();
        let frame = storage_mgr.pin(blk.clone()).unwrap();
        HeapPage::new(frame.clone(), blk, self.layout.clone())
    }

    /// Creates a TableIter instance that is an sequential iterator over ALL the tuples in a table
    pub fn heapscan_iter(&self) -> TableIter {
        TableIter::new(
            &self.table_blocks,
            self.storage_mgr.clone(),
            self.layout.clone(),
        )
    }
}

/// A Sequential Iterator over ALL tuples in a database table
pub struct TableIter<'tblmgr> {
    table_blocks: &'tblmgr Vec<BlockId>,
    storage_mgr: Rc<RefCell<StorageManager>>,
    layout: Rc<Layout>,
    current_block_index: usize,
    current_page: HeapPage,
    current_tuple_index: usize,
    current_page_pointer_count: usize,
}
impl<'tblmgr> TableIter<'tblmgr> {
    /// Constructs a TableIter instance and initializes it to use the first block of the heap file
    /// representing the Table.
    pub fn new(
        table_blocks: &'tblmgr Vec<BlockId>,
        storage_mgr: Rc<RefCell<StorageManager>>,
        layout: Rc<Layout>,
    ) -> Self {
        let frame = storage_mgr
            .borrow_mut()
            .pin(table_blocks[0].clone())
            .unwrap();
        let heap_page = HeapPage::new(frame, &table_blocks[0], layout.clone());
        Self {
            table_blocks,
            storage_mgr,
            layout,
            current_block_index: 0,
            current_tuple_index: 0,
            current_page_pointer_count: heap_page.pointer_count(),
            current_page: heap_page,
        }
    }

    /// Retrieves a tuple from the table , skips zeroed tuple pointers until a valid one is met.
    /// Each call to `next` retrieves exactly 1 tuple.
    ///
    ///If a None is returned , the caller is guaranteed that this table has no more records
    pub fn next(&mut self) -> Option<HashMap<String, Option<Vec<u8>>>> {
        while self.current_block_index != (self.table_blocks.len()) {
            while self.current_tuple_index < self.current_page_pointer_count {
                let (pointer_exist, tuple_exist) = self
                    .current_page
                    .pointer_and_tuple_exist(self.current_tuple_index);
                if tuple_exist {
                    let tuple = self.current_page.get_tuple_fields(self.current_tuple_index);
                    self.current_tuple_index += 1;
                    return Some(tuple);
                }
                self.current_tuple_index += 1;
            }
            self.current_tuple_index = 0;
            self.current_block_index += 1;
            let mut storage_mgr = self.storage_mgr.borrow_mut();
            storage_mgr.unpin(self.current_page.frame.clone());
            if self.current_block_index == self.table_blocks.len() {
                break;
            }
            let block = &self.table_blocks[self.current_block_index];
            let frame = storage_mgr.pin(block.clone()).unwrap();
            self.current_page = HeapPage::new(frame, block, self.layout.clone());
        }
        None
    }
    fn close(self) {}
    // pub fn has_next(&self) -> bool{
    //     self.current_block_index < self.table_blocks.len() - 1
    //     || ((self.current_block_index == self.table_blocks.len() - 1) && self.current_tuple_index < self.current_page_pointer_count )
    // }
}
