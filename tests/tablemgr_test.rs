mod common;

use std::cell::RefCell;
use std::rc::Rc;
use aqua::storage::storagemgr::StorageManager;
use aqua::table::tablemgr::TableManager;
use common::utils;

const db_dir: &str = "tests/db/";


#[test]
fn freemap(){
    let test_file = "freemap_test_blks";
    let blks = utils::create_blockids(10,test_file);
    let BLK_SIZE = 4096;
    let mut storagemgr = StorageManager::new(db_dir, BLK_SIZE,100);
    (0..blks.len()).for_each(|_| { storagemgr.extend_file_many(test_file,10); });
    // Init as heap page , flush then test
    let storagemgr = Rc::new(RefCell::new(storagemgr));
    let layout = utils::some_layout();
    let layout = Rc::new(layout);
    let tblmgr = TableManager::new(blks,storagemgr.clone(),None,layout.clone());
    println!("{:?}",tblmgr.free_map)


}