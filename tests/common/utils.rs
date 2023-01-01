use aqua::schema::schema::{Layout, Schema};
use aqua::schema::types::CharType::VarChar;
use aqua::schema::types::NumericType::{Integer, SmallInt};
use aqua::schema::types::Type;
use aqua::storage::blockid::BlockId;
use aqua::storage::heap::HeapPage;
use aqua::storage::storagemgr::StorageManager;
use std::rc::Rc;

pub fn create_blockids(blk_cnt: u64, filename: &str) -> Vec<BlockId> {
    (0..blk_cnt)
        .map(|num| BlockId::new(filename, num))
        .collect()
}

pub fn readfile(filename: &str) -> Vec<u8> {
    std::fs::read(filename).unwrap()
}

pub fn some_schema() -> Schema {
    let mut schema = Schema::new();
    let schema_vec = vec![
        ("id", Type::Numeric(SmallInt), false, None),
        ("name", Type::Character(VarChar), false, None),
        ("salary", Type::Numeric(Integer), false, None),
        ("job", Type::Character(VarChar), false, None),
    ];
    for attr in schema_vec {
        schema.add_field(attr.0, attr.1, attr.2, attr.3);
    }
    schema
}

pub fn some_layout() -> Layout {
    some_schema().to_layout()
}

pub fn empty_heapfile(
    db_dir: &str,
    filename: &str,
    blk_size: usize,
    blk_cnt: u32,
    layout: Rc<Layout>,
) -> Vec<BlockId> {
    let mut storagemgr = StorageManager::new(db_dir, blk_size, blk_cnt);
    let blks = storagemgr.extend_file_many(filename, blk_cnt);
    let mut frames = vec![];
    for blk in &blks {
        let frame = storagemgr.pin(blk.clone()).unwrap();
        HeapPage::new_from_empty(frame.clone(), blk, layout.clone());
        frames.push(frame.clone());
    }
    frames
        .iter()
        .for_each(|frame| storagemgr.flush_frame(frame.clone()));
    let v = blks.clone();
    v
}
