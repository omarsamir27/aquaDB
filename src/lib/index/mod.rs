use crate::index::hash_index::HashIndex;
use crate::schema::schema::FieldIndex;
use crate::sql::create_table::IndexType;
use crate::storage::blockid::BlockId;
use crate::storage::storagemgr::StorageManager;
use crate::AQUADIR;
use std::cell::RefMut;
use std::path::{Path, PathBuf};

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
    pub fn init_index(index_info: IndexInfo) {
        match index_info.index_type {
            IndexType::Hash => HashIndex::init(
                index_info.index_file_path.as_path(),
                index_info.directory_file_path.as_path(),
                GLOBAL_DEPTH,
            ),
            IndexType::Btree => {
                todo!()
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
}

pub struct IndexInfo {
    index_name: String,
    index_type: IndexType,
    pub column: String,
    pub index_file_path: PathBuf,
    directory_file_path: PathBuf,
}

impl IndexInfo {
    pub fn new(
        db_name: &str,
        index_name: String,
        index_type: IndexType,
        column: String,
        index_file_path: PathBuf,
        directory_file_path: PathBuf,
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
#[derive(Clone, Eq, PartialEq)]
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
