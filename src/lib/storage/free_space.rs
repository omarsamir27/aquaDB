use std::fs;
use std::path::{Path, PathBuf};
use crate::common::btree_multimap::BTreeMultimap;
use crate::common::fileops::write_file;
use crate::storage::blockid::BlockId;

/// An In-memory structure to track free space in a heap file for the purpose of inserting
/// data records
///
/// It is a wrapper around a BTreeMultimap exposing an interface specialized for managing free space
/// in a database table
#[derive(Debug)]
pub struct FreeMap {
    file : PathBuf,
    btree: BTreeMultimap<u16, BlockId>,
}

impl FreeMap {
    /// Creates an empty instance of a FSM
    pub fn new(file:PathBuf) -> Self {
        let data = fs::read(&file).unwrap();
        let btree = BTreeMultimap::from_bytes(&data);
        Self {
            file,
            btree
        }
    }
    pub fn init(file:PathBuf,space: u16, blkid: &BlockId) -> Self{
        let btree = BTreeMultimap::new();
        let mut map = Self{ file:file.clone(),
            btree
        };
        if space != 0{
            map.add_blockspace(space,blkid);
        }
        fs::write(&file,map.to_bytes()).unwrap();
        map

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

    // pub fn from_bytes(data: &[u8]) -> Self {
    //     Self {
    //         btree: BTreeMultimap::from_bytes(data),
    //     }
    // }

     fn to_bytes(&self) -> Vec<u8> {
        self.btree.to_bytes()
    }

    pub fn flush_map(&self){
        let data = self.to_bytes();
        write_file(&self.file,data);
    }
}
