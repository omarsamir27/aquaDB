use crate::common::numerical::ByteMagic;
use crate::index::btree_index::BPTree;
use crate::index::hash_index::HashIndex;
use crate::schema::schema::FieldIndex;
use crate::schema::types::Type;
use crate::sql::create_table::IndexType;
use crate::storage::blockid::BlockId;
use crate::storage::storagemgr::StorageManager;
use crate::AQUADIR;
use std::cell::{RefCell, RefMut};
use std::path::{Path, PathBuf};
use std::process::id;
use std::rc::Rc;

pub mod btree_index;
pub mod hash_index;

const GLOBAL_DEPTH: u8 = 4;

// pub trait Index {
//     fn get_rid(&self, search_key: String, storage_mgr: RefMut<StorageManager>) -> Vec<Rid>;
//
//     fn index_type(&self) -> IndexType;
// }
//
// fn init_index(index_info: IndexInfo) {
//     match index_info.index_type {
//         IndexType::Hash => HashIndex::init(
//             index_info.index_file_path.as_path(),
//             index_info.directory_file_path.as_path(),
//             GLOBAL_DEPTH,
//         ),
//         IndexType::Btree => {
//             todo!()
//         }
//     }
// }
// fn load_index(index_info: IndexInfo, blks: Vec<BlockId>) -> Box<dyn Index> {
//     match index_info.index_type {
//         IndexType::Hash => {
//             let idx: Box<dyn Index> = Box::new(HashIndex::new(
//                 index_info.directory_file_path.as_path(),
//                 index_info.index_name,
//                 blks,
//             ));
//             idx
//         }
//         IndexType::Btree => todo!(),
//     }
// }

pub enum Index {
    Hash(HashIndex),
    // Btree(Btree)
}

impl Index {
    pub fn init_index(index_info: IndexInfo, storage: Rc<RefCell<StorageManager>>) {
        match index_info.index_type {
            IndexType::Hash => HashIndex::init(
                index_info.index_file_path.as_path(),
                index_info.directory_file_path.as_path(),
                GLOBAL_DEPTH,
            ),
            IndexType::Btree => {
                BPTree::init(
                    index_info.key_type,
                    storage,
                    index_info.index_file_path.to_str().unwrap().to_string(),
                );
            }
        }
    }
    pub fn load_index(index_info: IndexInfo, blks: Vec<BlockId>) -> Self {
        match index_info.index_type {
            IndexType::Hash => Self::Hash(HashIndex::new(
                index_info.directory_file_path.as_path(),
                index_info.index_name,
                blks,
                index_info.column,
            )),
            IndexType::Btree => todo!(),
        }
    }
    pub fn get_rid(&self, search_key: &[u8], storage_mgr: RefMut<StorageManager>) -> Vec<Rid> {
        match self {
            Index::Hash(h) => h.get_rids(search_key, storage_mgr),
        }
    }
    pub fn insert_record(
        &mut self,
        data_val: &[u8],
        blk: u64,
        slot: usize,
        storage_mgr: RefMut<StorageManager>,
    ) {
        match self {
            Index::Hash(h) => h.insert_record(data_val, blk, slot as u16, storage_mgr),
        }
    }

    // pub fn flush_all(&self, mut storage_mgr: &mut RefMut<StorageManager>) {
    //     match self {
    //         Index::Hash(idx) => {
    //             idx.flush_all(storage_mgr);
    //         }
    //     }
    // }
}

pub struct IndexInfo {
    pub index_name: String,
    pub index_type: IndexType,
    pub column: String,
    pub index_file_path: PathBuf,
    pub directory_file_path: PathBuf,
    pub key_type: Type,
}

impl IndexInfo {
    pub fn new(
        db_name: &str,
        index_name: String,
        index_type: IndexType,
        column: String,
        index_file_path: PathBuf,
        directory_file_path: PathBuf,
        key_type: Type,
    ) -> Self {
        let index_file_path =
            Path::new(&AQUADIR()).join(Path::new("base").join(db_name).join(index_file_path));
        let directory_file_path =
            Path::new(&AQUADIR()).join(Path::new("base").join(db_name).join(directory_file_path));
        Self {
            index_name,
            index_type,
            column,
            index_file_path,
            directory_file_path,
            key_type,
        }
    }
}

// impl From<FieldIndex> for IndexInfo {
//     fn from(value: FieldIndex) -> Self {
//         Self{
//             index_name : value
//         }
//     }
// }

/// Record ID entity encapsulating the block number and the slot number of a certain tuple.
// #[derive(Debug, Clone, Eq, PartialEq)]
// pub struct Rid {
//     block_num: u64,
//     slot_num: u16,
// }
//
// impl Rid {
//     pub fn new(block_num: u64, slot_num: u16) -> Self {
//         Self {
//             block_num,
//             slot_num,
//         }
//     }
//     pub fn block_num(&self) -> u64 {
//         self.block_num
//     }
//     pub fn slot_num(&self) -> u16 {
//         self.slot_num
//     }
//     pub fn rid_blk_num(&self, heap_file: &str) -> (BlockId, usize) {
//         (
//             BlockId::new(heap_file, self.block_num),
//             self.slot_num as usize,
//         )
//     }
// }

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Rid {
    block_num: u64,
    slot_num: u16,
}

impl Rid {
    pub fn new(block_num: u64, slot_num: u16) -> Self {
        Self {
            block_num,
            slot_num,
        }
    }

    // Convert Rid to bytes
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.block_num.to_ne_bytes());
        bytes.extend_from_slice(&self.slot_num.to_ne_bytes());
        bytes
    }

    // Convert bytes to Rid
    fn from_bytes(bytes: &[u8]) -> Rid {
        let block_num = bytes.extract_u64(0);
        let slot_num = bytes.extract_u16(8);

        Rid {
            block_num,
            slot_num,
        }
    }
    pub fn block_num(&self) -> u64 {
        self.block_num
    }
    pub fn slot_num(&self) -> u16 {
        self.slot_num
    }
    pub fn rid_blk_num(&self, heap_file: &str) -> (BlockId, usize) {
        (
            BlockId::new(heap_file, self.block_num),
            self.slot_num as usize,
        )
    }
}
