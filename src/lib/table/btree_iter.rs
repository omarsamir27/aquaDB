use crate::index::btree_index::BPTree;
use crate::index::Rid;
use crate::storage::storagemgr::StorageManager;
use crate::table::direct_access::DirectAccessor;
use crate::table::tablemgr::TableManager;
use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::rc::Rc;

pub struct BtreeIter {
    direct_access: DirectAccessor,
    index: BPTree,
    rids: Vec<Rid>,
    op: evalexpr::Operator,
}

impl BtreeIter {
    pub fn load_key(&mut self, key: &[u8]) {
        use evalexpr::Operator::*;
        let key = key.to_vec();
        let rids = match self.op {
            Eq => self.index.search(key),
            Lt => self.index.get_less_than(key),
            Gt => self.index.get_greater_than(key),
            Leq => self.index.get_less_than_or_equal(key),
            Geq => self.index.get_greater_than_or_equal(key),
            _ => unreachable!(),
        };
        self.rids.extend(rids.unwrap_or_default());
    }
    pub fn new(direct_access: DirectAccessor, index: BPTree, op: evalexpr::Operator) -> Self {
        Self {
            direct_access,
            index,
            rids: vec![],
            op,
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
