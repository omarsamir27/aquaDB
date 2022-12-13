mod common;

use aqua::storage::storagemgr::StorageManager;
use aqua::table::tablemgr::TableManager;
use aqua::RcRefCell;
use common::utils;
use std::cell::RefCell;
use std::rc::Rc;
use aqua::storage::blockid::BlockId;

const db_dir: &str = "tests/db/";

#[test]
fn freemap() {
    let test_file = "freemap_test_blks";
    let BLK_SIZE = 4096;
    let layout = Rc::new(utils::some_layout());
    let file_blocks = utils::empty_heapfile(db_dir, test_file, BLK_SIZE, 10, layout.clone());
    let storagemgr = RcRefCell!(StorageManager::new(db_dir, BLK_SIZE, 100));
    let tblmgr = TableManager::new(file_blocks.clone(), storagemgr.clone(), None, layout.clone());
    assert_eq!(tblmgr.free_map.btree().get(4090).unwrap(),&file_blocks)
}

#[test]
fn insert_tuple_update_freemap() {
    let test_file = "insert_tuple_update_freemap";
    let BLK_SIZE = 4096;
    let layout = Rc::new(utils::some_layout());
    let file_blocks = utils::empty_heapfile(db_dir, test_file, BLK_SIZE, 10, layout.clone());
    let storagemgr = RcRefCell!(StorageManager::new(db_dir, BLK_SIZE, 100));
    let mut tblmgr = TableManager::new(file_blocks.clone(), storagemgr.clone(), None, layout.clone());
    tblmgr.try_insert_tuple(vec![
        ("id".to_string(), None),
        ("name".to_string(), None),
        ("salary".to_string(), Some(5000_u32.to_ne_bytes().to_vec())),
        (
            "job".to_string(),
            Some("Engineer".to_string().as_bytes().to_vec()),
        ),
    ]);
    let blk = BlockId{
        block_num:9,
        filename:test_file.to_string()
    };
    tblmgr.flush(&blk);
    assert_eq!(tblmgr.free_map.btree().range(0..BLK_SIZE as u16).count(),2);
    let job = tblmgr.get_field(&blk,0,"job").map(|bytes| String::from_utf8(bytes).unwrap());
    assert_eq!(job,Some("Engineer".to_string()));
}
