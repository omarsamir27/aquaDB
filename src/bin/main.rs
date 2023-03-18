#![allow(non_snake_case)]

use aqua::common::btree_multimap::BTreeMultimap;
use aqua::query::concrete_types::ConcreteType;
use aqua::schema::null_bitmap::NullBitMap;
use aqua::schema::schema::{Layout, Schema};
use aqua::schema::types::CharType::VarChar;
use aqua::schema::types::NumericType::{Integer, SmallInt};
use aqua::schema::types::Type;
use aqua::storage::blockid::BlockId;
use aqua::storage::heap::HeapPage;
use aqua::storage::storagemgr::StorageManager;
use aqua::storage::tuple::Tuple;
use aqua::index::hash_index;
use bincode::*;
use evalexpr::*;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use aqua::index::hash_index::HashIndex;
use aqua::RcRefCell;
use aqua::table::tablemgr::TableManager;
use sdbm::sdbm_hash;

fn btree_write_test() {
    let mut tree = BTreeMultimap::new();
    tree.insert_vec(10, &[1, 2, 3, 4, 5, 6, 7]);
    tree.insert_vec(5, &[8, 9, 10]);
    let bytes = tree.to_bytes();
    std::fs::write("btree", bytes).unwrap()
}

fn btree_read_test() {
    let bytes = std::fs::read("btree").unwrap();
    let tree: BTreeMultimap<i32, i32> = BTreeMultimap::from_bytes(bytes.as_slice());
    tree.print_all()
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

fn main() {
    println!("{}", sdbm_hash("Ahmed") % 100);
}
