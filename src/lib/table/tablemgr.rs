use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use crate::common::btree_multimap::BTreeMultimap;
use crate::storage::blockid::BlockId;
use crate::storage::heap::HeapFile;
use crate::storage::storagemgr::StorageManager;

struct TableManager {
    free_map: BTreeMultimap<u16, BlockId>,
    heap_files: HashMap<String, HeapFile>,
    storage_mgr: Rc<RefCell<StorageManager>>,
}

impl TableManager {
    fn new(filenames: Vec<String>, storage_mgr: Rc<RefCell<StorageManager>>) -> Self {
        todo!()
    }
}