use crate::index::hash_index::HashIndex;
use crate::index::Rid;
use crate::storage::storagemgr::StorageManager;
use crate::table::tablemgr::TableManager;
use std::cell::RefMut;
use std::collections::HashMap;

pub struct HashIter<'tblmgr> {
    tblmgr: &'tblmgr TableManager,
    rids: Vec<Rid>,
}

impl<'tblmgr> HashIter<'tblmgr> {
    pub fn new(
        tblmgr: &'tblmgr TableManager,
        index: &HashIndex,
        key: &[u8],
        storage: RefMut<StorageManager>,
    ) -> Self {
        Self {
            tblmgr,
            rids: index.get_rids(key, storage),
        }
    }
}

impl<'tblmgr> Iterator for HashIter<'tblmgr> {
    type Item = HashMap<String, Option<Vec<u8>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(rid) = self.rids.pop() {
            let (blk, slot) = rid.rid_blk_num(self.tblmgr.get_heapfile_name());
            Some(self.tblmgr.get_tuple(&blk, slot))
        } else {
            None
        }
    }
}
