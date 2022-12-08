use aqua::storage;
use aqua::storage::blockid::BlockId;
use aqua::storage::storagemgr::StorageManager;

#[test]
fn write_read() {
    let db_dir = "tests/db";
    let test_file = "write_read_test1";
    let BLK_SIZE = 32;
    let mut storagemgr = StorageManager::new(db_dir, BLK_SIZE);
    storagemgr.extend_file(test_file);
    let blk = BlockId {
        filename: test_file.to_string(),
        block_num: 0,
    };
    let frame = storagemgr.pin(blk).unwrap();
    (*frame).borrow_mut().write(b"test");
    storagemgr.flush_frame(frame);
}
