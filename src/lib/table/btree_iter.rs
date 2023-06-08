use crate::index::Rid;
use crate::storage::storagemgr::StorageManager;
use crate::table::direct_access::DirectAccessor;
use crate::table::tablemgr::TableManager;
use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::rc::Rc;
use crate::index::btree_index::BPTree;

pub struct  BtreeIter {
    direct_access: DirectAccessor,
    index: BPTree,
    rids: Vec<Rid>,
}

impl BtreeIter {
    pub fn load_key(&mut self,key:&[u8]){
        self.rids.extend(self.index.search(key.to_vec()).unwrap_or_default());
    }
    pub fn new(
        direct_access: DirectAccessor,
        index : BPTree,
    ) -> Self {
        Self {
            direct_access,
            index,
            rids: vec![],
        }
    }
}

impl Iterator for BtreeIter {
    type Item = HashMap<String, Option<Vec<u8>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(rid) = self.rids.pop() {
            Some(self.direct_access.get_tuple(rid))
        } else {
            None
        }
    }
}
