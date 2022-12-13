mod common;

use aqua::storage::storagemgr::StorageManager;
use aqua::table::tablemgr::TableManager;
use aqua::RcRefCell;
use common::utils;
use std::cell::RefCell;
use std::rc::Rc;

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
fn freemap() {
    let test_file = "freemap_test_blks";
    let BLK_SIZE = 4096;
    let layout = Rc::new(utils::some_layout());
    let file_blocks = utils::empty_heapfile(db_dir, test_file, BLK_SIZE, 10, layout.clone());
    let storagemgr = RcRefCell!(StorageManager::new(db_dir, BLK_SIZE, 100));
    let tblmgr = TableManager::new(file_blocks.clone(), storagemgr.clone(), None, layout.clone());
    assert_eq!(tblmgr.free_map.btree().get(4090).unwrap(),&file_blocks)
}
