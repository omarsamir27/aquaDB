use crate::common::btree_multimap::BTreeMultimap;
use crate::storage::blockid::BlockId;

/// An In-memory structure to track free space in a heap file for the purpose of inserting
/// data records
///
/// It is a wrapper around a BTreeMultimap exposing an interface specialized for managing free space
/// in a database table
#[derive(Debug)]
pub struct FreeMap {
    btree: BTreeMultimap<u16, BlockId>,
}

impl FreeMap {
    /// Creates an empty instance of a FSM
    pub fn new() -> Self {
        Self {
            btree: BTreeMultimap::<u16, BlockId>::new(),
        }
    }
    /// Adds a BlockId and its corresponding space rounded down to the nearest tens to the FSM
    /// The choice to round down to the nearest tens is to not spam the tree with thin vectors,
    /// the granularity can be further explored in the future
    pub fn add_blockspace(&mut self, space: u16, blkid: &BlockId) {
        let space = (space / 10) * 10;
        self.btree.insert_element(space, blkid.clone());
    }

    /// Get the smallest block fitting a tuple and a surplus of 10 bytes
    pub fn get_smallest_fit(&mut self, tuple_size: u16) -> Option<(u16, BlockId)> {
        self.btree.pop_first_bigger_than(tuple_size + 10)
    }

    /// Returns a reference to the internal B-Tree
    pub fn btree(&self) -> &BTreeMultimap<u16, BlockId> {
        &self.btree
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        Self {
            btree: BTreeMultimap::from_bytes(data),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.btree.to_bytes()
    }
}
