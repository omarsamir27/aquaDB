use crate::index::hash_index::{HashIndex, Rid};
use crate::sql::create_table::IndexType;
use crate::storage::blockid::BlockId;
use crate::storage::storagemgr::StorageManager;
use std::cell::RefMut;
use std::path::PathBuf;
use crate::schema::schema::FieldIndex;

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

pub enum Index{
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
    pub fn load_index(index_info:IndexInfo,blks:Vec<BlockId>) -> Self{
        match index_info.index_type{
            IndexType::Hash=> Self::Hash( HashIndex::new(index_info.directory_file_path.as_path(), index_info.index_name, blks)),
            IndexType::Btree=> todo!()
        }
    }
    pub fn get_rid(&self, search_key: String, storage_mgr: RefMut<StorageManager>) -> Vec<Rid>{
        match self { Index::Hash(h) => h.get_rids(search_key,storage_mgr) }
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
        index_name: String,
        index_type: IndexType,
        column: String,
        index_file_path: PathBuf,
        directory_file_path: PathBuf,
    ) -> Self {
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