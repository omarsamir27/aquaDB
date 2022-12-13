use crate::common::btree_multimap::BTreeMultimap;
use crate::storage::blockid::BlockId;
#[derive(Debug)]
pub struct FreeMap {
    btree: BTreeMultimap<u16, BlockId>,
}

impl FreeMap {
    pub fn new() -> Self {
        Self {
            btree: BTreeMultimap::<u16, BlockId>::new(),
        }
    }

    pub fn add_blockspace(&mut self, space: u16, blkid: &BlockId) {
        self.btree.insert_element(space, blkid.clone());
    }

    pub fn get_smallest_fit(&mut self, tuple_size: u16) -> Option<(u16, BlockId)> {
        self.btree.pop_first_range(tuple_size..(tuple_size + 20))
    }

    pub fn btree(&self) -> &BTreeMultimap<u16, BlockId> {
        &self.btree
    }
}
