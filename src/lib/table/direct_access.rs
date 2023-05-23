use crate::index::Rid;
use crate::schema::schema::Layout;
use crate::storage::blockid::BlockId;
use crate::storage::heap::HeapPage;
use crate::storage::storagemgr::StorageManager;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

pub struct DirectAccessor {
    table_blocks: Vec<BlockId>,
    storage: Rc<RefCell<StorageManager>>,
    layout: Rc<Layout>,
}
impl DirectAccessor {
    pub fn new(
        table_blocks: Vec<BlockId>,
        storage: Rc<RefCell<StorageManager>>,
        layout: Rc<Layout>,
    ) -> Self {
        Self {
            table_blocks,
            storage,
            layout,
        }
    }
    pub fn get_tuple(&self, rid: Rid) -> HashMap<String, Option<Vec<u8>>> {
        let (blk, slot) = rid.rid_blk_num(&self.table_blocks[0].filename);
        let frame = self.storage.borrow_mut().pin(blk.clone()).unwrap();
        let heap = HeapPage::new(frame, &blk, self.layout.clone());
        heap.get_tuple_fields(rid.slot_num() as usize)
    }
}
