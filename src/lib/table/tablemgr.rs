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

pub struct TableManager {
    pub free_map: FreeMap,
    // heap_pages: HashMap<BlockId, HeapPage>,
    table_blocks: Vec<BlockId>,
    storage_mgr: Rc<RefCell<StorageManager>>,
    layout: Rc<Layout>,
}

impl TableManager {
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
                let space = (space / 10) * 10;
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

    pub fn try_insert_tuple(&mut self, tuple_bytes: Vec<(String, Vec<u8>)>) {
        let tuple = Tuple::new(tuple_bytes, self.layout.clone());
        let target_block = self.free_map.get_smallest_fit(tuple.tuple_size());
        let mut storage_mgr = self.storage_mgr.borrow_mut();
        let mut target_page = if let Some((free_size, block)) = target_block {
            let mut frame = storage_mgr.pin(block.clone()).unwrap();
            HeapPage::new(frame, &block, self.layout.clone())
        } else {
            let blkid = storage_mgr.extend_file(self.table_blocks[0].filename.as_str());
            let mut frame = storage_mgr.pin(blkid.clone()).unwrap();
            HeapPage::new_from_empty(frame, &blkid, self.layout.clone())
        };
        target_page.insert_tuple(tuple);
    }
    fn vacuum(&mut self) {
        let mut storage_mgr = self.storage_mgr.borrow_mut();
        for block in &self.table_blocks {
            let frame = storage_mgr.pin(block.clone()).unwrap();
            let mut heap_page = HeapPage::new(frame.clone(), block, self.layout.clone());
            heap_page.vacuum();
            storage_mgr.unpin(frame);
        }
    }

    fn heapscan_iter(&self) -> TableIter {
        TableIter::new(
            &self.table_blocks,
            self.storage_mgr.clone(),
            self.layout.clone()
        )
    }
}

struct TableIter<'tblmgr> {
    table_blocks: &'tblmgr Vec<BlockId>,
    storage_mgr: Rc<RefCell<StorageManager>>,
    layout: Rc<Layout>,
    current_block_index : usize,
    current_page : HeapPage,
    current_tuple_index: usize,
    current_page_pointer_count : usize
}
impl<'tblmgr> TableIter<'tblmgr>{
    pub fn new(table_blocks:&'tblmgr  Vec<BlockId>,storage_mgr:Rc<RefCell<StorageManager>>,layout:Rc<Layout>)-> Self{
        let frame = storage_mgr.borrow_mut().pin(table_blocks[0].clone()).unwrap();
        let heap_page = HeapPage::new(frame,&table_blocks[0],layout.clone());
        Self{
            table_blocks,
            storage_mgr,
            layout,
            current_block_index:0,
            current_tuple_index :0,
            current_page_pointer_count : heap_page.pointer_count(),
            current_page : heap_page,
        }
    }

    fn next(&mut self) -> Option<Vec<u8>>{
        while self.current_block_index != (self.table_blocks.len() -1) {
            while self.current_tuple_index < self.current_page_pointer_count {
                let (pointer_exist, tuple_exist) =
                    self.current_page.pointer_and_tuple_exist(self.current_tuple_index);
                if tuple_exist{
                    let tuple = self.current_page.get_tuple(self.current_tuple_index);
                    self.current_tuple_index += 1;
                    return Some(tuple)
                }
                self.current_tuple_index += 1;
            }
            self.current_tuple_index = 0;
            self.current_block_index +=1;
            let mut storage_mgr = self.storage_mgr.borrow_mut();
            storage_mgr.unpin(self.current_page.frame.clone());
            let block = &self.table_blocks[self.current_block_index];
            let frame = storage_mgr.pin(block.clone()).unwrap();
            self.current_page = HeapPage::new(frame,block,self.layout.clone());
        }
        None
    }

}
