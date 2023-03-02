#![allow(non_snake_case)]

use aqua::common::btree_multimap::BTreeMultimap;
use aqua::query::concrete_types::ConcreteType;
use aqua::schema::null_bitmap::NullBitMap;
use aqua::schema::schema::Schema;
use aqua::schema::types::CharType::VarChar;
use aqua::schema::types::NumericType::{Integer, SmallInt};
use aqua::schema::types::Type;
use aqua::storage::blockid::BlockId;
use aqua::storage::heap::HeapPage;
use aqua::storage::storagemgr::StorageManager;
use aqua::storage::tuple::Tuple;
use bincode::*;
use evalexpr::*;
use std::any::Any;
use std::collections::HashMap;
use std::hint::black_box;
use std::rc::Rc;
use aqua::sql::parser::parse_query;

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

fn main() {
    let query = "create table omar( samir int primary key,koko smallint references oo(bad))";
    let query = parse_query(query);
    dbg!(query);
}
