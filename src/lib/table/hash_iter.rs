use crate::index::hash_index::HashIndex;
use crate::index::Rid;
use crate::storage::storagemgr::StorageManager;
use crate::table::direct_access::DirectAccessor;
use crate::table::tablemgr::TableManager;
use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::rc::Rc;

pub struct  HashIter {
    direct_access: DirectAccessor,
    index: HashIndex,
    rids: Vec<Rid>,
}

impl HashIter {
    pub fn load_key(&mut self,key:&[u8]){
        self.rids.extend(self.index.get_rids(key,self.direct_access.get_storage().borrow_mut()));
    }
    pub fn new(
        direct_access: DirectAccessor,
        index : HashIndex,
    ) -> Self {
        Self {
            direct_access,
            index,
            rids: vec![],
        }
    }
}

impl Iterator for HashIter {
    type Item = HashMap<String, Option<Vec<u8>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(rid) = self.rids.pop() {
            Some(self.direct_access.get_tuple(rid))
        } else {
            None
        }
    }
}
