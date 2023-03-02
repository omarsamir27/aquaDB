mod common;

use crate::common::random::distill_schema;
use aqua::schema::schema::Schema;
use aqua::schema::types::CharType::VarChar;
use aqua::schema::types::NumericType::{Integer, SmallInt};
use aqua::schema::types::Type;
use aqua::storage::blockid::BlockId;
use aqua::storage::storagemgr::StorageManager;
use aqua::table::tablemgr::TableManager;
use aqua::RcRefCell;
use common::utils;
use std::cell::RefCell;
use std::rc::Rc;

#[cfg(windows)]
const db_dir: &str = "tests\\db\\";

#[cfg(unix)]
const db_dir: &str = "tests/db/";

#[test]
fn freemap() {
    let test_file = "freemap_test_blks";
    let BLK_SIZE = 4096;
    let layout = Rc::new(utils::some_layout());
    let file_blocks = utils::empty_heapfile(db_dir, test_file, BLK_SIZE, 10, layout.clone());
    let storagemgr = RcRefCell!(StorageManager::new(db_dir, BLK_SIZE, 100));
    let tblmgr = TableManager::new(
        file_blocks.clone(),
        storagemgr.clone(),
        None,
        layout.clone(),
    );
    assert_eq!(tblmgr.free_map.btree().get(4090).unwrap(), &file_blocks)
}

#[test]
fn insert_tuple_update_freemap() {
    let test_file = "insert_tuple_update_freemap";
    let BLK_SIZE = 4096;
    let layout = Rc::new(utils::some_layout());
    let file_blocks = utils::empty_heapfile(db_dir, test_file, BLK_SIZE, 10, layout.clone());
    let storagemgr = RcRefCell!(StorageManager::new(db_dir, BLK_SIZE, 100));
    let mut tblmgr = TableManager::new(
        file_blocks.clone(),
        storagemgr.clone(),
        None,
        layout.clone(),
    );
    tblmgr.try_insert_tuple(vec![
        ("id".to_string(), None),
        ("name".to_string(), None),
        ("salary".to_string(), Some(5000_u32.to_ne_bytes().to_vec())),
        (
            "job".to_string(),
            Some("Engineer".to_string().as_bytes().to_vec()),
        ),
    ]);
    let blk = BlockId {
        block_num: 9,
        filename: test_file.to_string(),
    };
    tblmgr.flush(&blk);
    assert_eq!(tblmgr.free_map.btree().range(0..BLK_SIZE as u16).count(), 2);
    let job = tblmgr
        .get_field(&blk, 0, "job")
        .map(|bytes| String::from_utf8(bytes).unwrap());
    assert_eq!(job, Some("Engineer".to_string()));
}
#[test]
fn insert_tuples_then_scan() {
    let test_file = "insert_tuples_then_scan";
    let BLK_SIZE = 4096;
    let mut schema = Schema::new();
    let schema_vec = vec![
        ("id", Type::Numeric(SmallInt), false, None),
        ("name", Type::Character(VarChar), false, None),
        ("salary", Type::Numeric(Integer), false, None),
        ("job", Type::Character(VarChar), false, None),
    ];
    for attr in schema_vec {
        schema.add_field_default_constraints(attr.0,attr.1,attr.3);
    }
    let layout = schema.to_layout();
    let layout = Rc::new(layout);
    let file_blocks = utils::empty_heapfile(db_dir, test_file, BLK_SIZE, 1, layout.clone());
    let storagemgr = RcRefCell!(StorageManager::new(db_dir, BLK_SIZE, 100));
    let mut tblmgr = TableManager::new(
        file_blocks.clone(),
        storagemgr.clone(),
        None,
        layout.clone(),
    );
    let schema = distill_schema(schema);
    let tuples = common::random::generate_random_tuples(&schema, 100);
    for t in &tuples {
        tblmgr.try_insert_tuple(t.clone())
    }
    tblmgr.flush_all();
    let mut table_iter = tblmgr.heapscan_iter();
    let mut tuples_check = vec![];
    loop {
        let tuple = table_iter.next();
        match tuple {
            None => break,
            Some(t) => tuples_check.push(Some(t)),
        }
    }
    assert_eq!(tuples_check.len(), tuples.len())
}

#[test]
fn mark_delete() {
    let test_file = "mark_delete";
    let BLK_SIZE = 4096;
    let mut schema = Schema::new();
    let schema_vec = vec![
        ("id", Type::Numeric(SmallInt), false, None),
        ("name", Type::Character(VarChar), false, None),
        ("salary", Type::Numeric(Integer), false, None),
        ("job", Type::Character(VarChar), false, None),
    ];
    for attr in schema_vec {
        schema.add_field_default_constraints(attr.0,attr.1,attr.3);
    }
    let layout = schema.to_layout();
    let layout = Rc::new(layout);
    let file_blocks = utils::empty_heapfile(db_dir, test_file, BLK_SIZE, 1, layout.clone());
    let storagemgr = RcRefCell!(StorageManager::new(db_dir, BLK_SIZE, 100));
    let mut tblmgr = TableManager::new(
        file_blocks.clone(),
        storagemgr.clone(),
        None,
        layout.clone(),
    );
    let schema = distill_schema(schema);
    let tuples = common::random::generate_random_tuples(&schema, 100);
    for t in &tuples {
        tblmgr.try_insert_tuple(t.clone())
    }
    tblmgr.flush_all();
    let blk = file_blocks[0].clone();
    tblmgr.delete_tuple(&blk, 0);
    tblmgr.flush(&blk)
}

#[test]
fn delete_vacuum_test() {
    let test_file = "delete_vacuum_test";
    let BLK_SIZE = 4096;
    let mut schema = Schema::new();
    let schema_vec = vec![
        ("id", Type::Numeric(SmallInt), false, None),
        ("name", Type::Character(VarChar), false, None),
        ("salary", Type::Numeric(Integer), false, None),
        ("job", Type::Character(VarChar), false, None),
    ];
    for attr in schema_vec {
        schema.add_field_default_constraints(attr.0,attr.1,attr.3);
    }
    let layout = schema.to_layout();
    let layout = Rc::new(layout);
    let file_blocks = utils::empty_heapfile(db_dir, test_file, BLK_SIZE, 1, layout.clone());
    let storagemgr = RcRefCell!(StorageManager::new(db_dir, BLK_SIZE, 100));
    let mut tblmgr = TableManager::new(
        file_blocks.clone(),
        storagemgr.clone(),
        None,
        layout.clone(),
    );
    let schema = distill_schema(schema);
    let tuples = common::random::generate_random_tuples(&schema, 50);
    for t in &tuples {
        tblmgr.try_insert_tuple(t.clone())
    }
    tblmgr.flush_all();
    let blk = file_blocks[0].clone();
    tblmgr.delete_tuple(&blk, 0);
    tblmgr.flush(&blk);

    tblmgr.vacuum();
    tblmgr.flush(&blk)
}
