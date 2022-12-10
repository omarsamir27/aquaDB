use std::fs::File;
use aqua::storage;
use aqua::storage::blockid::BlockId;
use aqua::storage::storagemgr::StorageManager;
mod common;
use common::utils;

const db_dir: &str = "tests/db/";

#[test]
fn write_1000_blocks(){
    let test_file = "write_1000_blocks";
    let BLK_SIZE = 4096;
    let mut storagemgr = StorageManager::new(db_dir, BLK_SIZE,100);
    (0..1000).for_each(|_| { storagemgr.extend_file(test_file); })
}

#[test]
fn write() {
    let test_file = "write_read_test1";
    let BLK_SIZE = 32;
    let mut storagemgr = StorageManager::new(db_dir, BLK_SIZE,100);
    storagemgr.extend_file(test_file);
    let blk = BlockId {
        filename: test_file.to_string(),
        block_num: 0,
    };
    let frame = storagemgr.pin(blk).unwrap();
    (*frame).borrow_mut().write(b"test");
    storagemgr.flush_frame(frame);
    let written = utils::readfile(format!("{}{}",db_dir,test_file).as_str());
    assert_eq!(b"test",written[0..b"test".len()].as_ref())
}

#[test]
fn fill_buffer(){
    let test_file = "fill_buffer";
    let BLK_SIZE = 4096;
    let mut storagemgr = StorageManager::new(db_dir, BLK_SIZE,3);
    (0..3).for_each(|_| { storagemgr.extend_file(test_file); });
    let mut blks = utils::create_blockids(3, test_file);
    let mut frames = Vec::new();
    for i in 0..3{
        let frame = storagemgr.pin(blks.pop().unwrap()).unwrap();
        frames.push(frame)
    }
}

#[test]
#[should_panic]
fn overload_buffer(){
    let test_file = "overload_buffer";
    let BLK_SIZE = 4096;
    let mut storagemgr = StorageManager::new(db_dir, BLK_SIZE,100);
    (0..105).for_each(|_| { storagemgr.extend_file(test_file); });
    let mut blks = utils::create_blockids(105, test_file);
    let mut frames = Vec::new();
    for i in 0..105{
        let frame = storagemgr.pin(blks[i].clone()).unwrap();
        frames.push(frame)
    }
}

