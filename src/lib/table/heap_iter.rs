use crate::schema::schema::Layout;
use crate::storage::blockid::BlockId;
use crate::storage::heap::HeapPage;
use crate::storage::storagemgr::StorageManager;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

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

impl<'tblmgr> Iterator for TableIter<'tblmgr> {
    type Item = HashMap<String, Option<Vec<u8>>>;

    fn next(&mut self) -> Option<Self::Item> {
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
    // pub fn next(&mut self) -> Option<HashMap<String, Option<Vec<u8>>>> {
    //     while self.current_block_index != (self.table_blocks.len()) {
    //         while self.current_tuple_index < self.current_page_pointer_count {
    //             let (pointer_exist, tuple_exist) = self
    //                 .current_page
    //                 .pointer_and_tuple_exist(self.current_tuple_index);
    //             if tuple_exist {
    //                 let tuple = self.current_page.get_tuple_fields(self.current_tuple_index);
    //                 self.current_tuple_index += 1;
    //                 return Some(tuple);
    //             }
    //             self.current_tuple_index += 1;
    //         }
    //         self.current_tuple_index = 0;
    //         self.current_block_index += 1;
    //         let mut storage_mgr = self.storage_mgr.borrow_mut();
    //         storage_mgr.unpin(self.current_page.frame.clone());
    //         if self.current_block_index == self.table_blocks.len() {
    //             break;
    //         }
    //         let block = &self.table_blocks[self.current_block_index];
    //         let frame = storage_mgr.pin(block.clone()).unwrap();
    //         self.current_page = HeapPage::new(frame, block, self.layout.clone());
    //     }
    //     None
    // }
    fn close(self) {}
    // pub fn has_next(&self) -> bool{
    //     self.current_block_index < self.table_blocks.len() - 1
    //     || ((self.current_block_index == self.table_blocks.len() - 1) && self.current_tuple_index < self.current_page_pointer_count )
    // }
}
