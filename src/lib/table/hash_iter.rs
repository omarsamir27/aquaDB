use crate::index::hash_index::HashIndex;
use crate::index::Rid;
use crate::storage::storagemgr::StorageManager;
use crate::table::direct_access::DirectAccessor;
use crate::table::tablemgr::TableManager;
use std::cell::RefMut;
use std::collections::HashMap;

pub struct HashIter {
    direct_access: DirectAccessor,
    rids: Vec<Rid>,
}

impl HashIter {
    pub fn new(
        direct_access: DirectAccessor,
        index: &HashIndex,
        key: &[u8],
        storage: RefMut<StorageManager>,
    ) -> Self {
        Self {
            direct_access,
            rids: index.get_rids(key, storage),
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
