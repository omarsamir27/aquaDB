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
    // btree_test()
    // btree_read_test();
    // let mut schema = Schema::new();
    // let schema_vec = vec![
    //     ("id",Type::Numeric(SmallInt),false,None),
    //     ("name",Type::Character(VarChar),false,None),
    //     ("salary",Type::Numeric(Integer),false,None),
    //     ("job",Type::Character(VarChar),false,None)
    // ];
    // for attr in schema_vec{
    //     schema.add_field(
    //         attr.0,
    //         attr.1,
    //         attr.2,
    //         attr.3
    //     );
    // }
    // let layout = schema.to_layout();
    // let layout = Rc::new(layout);
    // let tuple = vec![
    //     // ("name".to_string(), Some("Omar".to_string().as_bytes().to_vec())),
    //     ("name".to_string(), None),
    //     ("id".to_string(), None),
    //     // ("id".to_string(), Some(100_u16.to_ne_bytes().to_vec())),
    //     // ("salary".to_string(), Some(5000_u32.to_ne_bytes().to_vec())),
    //     ("salary".to_string(), None),
    //     ("job".to_string(), None),
    //     // ("job".to_string(), Some("Engineer".to_string().as_bytes().to_vec()))
    // ];
    // let mut tuple = Tuple::new(tuple, layout.clone());
    // let tuple_bytes = tuple.to_bytes();
    // println!("{:?}", tuple_bytes);

    // let mut bitmap = NullBitMap::new()
    // println!("{:?}", bitmap);
    // for mut byte in 0..4_u8{
    //     byte = 0_u8;
    //     for bit in 0..8_u8{
    //         if bit%2 == 1 {
    //             byte |= 1 << bit;
    //         }
    //     }
    //     bitmap.push(byte);
    // }
    // println!("{:?}", bitmap);

    // const db_dir: &str = "tests/db/";
    //
    // let test_file = "write_read_tuples";
    // let BLK_SIZE = 4096;
    // let mut storagemgr = StorageManager::new(db_dir, BLK_SIZE, 100);
    // let blk = BlockId {
    //     filename: test_file.to_string(),
    //     block_num: 0,
    // };
    // let mut schema = Schema::new();
    // let schema_vec = vec![
    //     ("id", Type::Numeric(SmallInt), false, None),
    //     ("name", Type::Character(VarChar), false, None),
    //     ("salary", Type::Numeric(Integer), false, None),
    //     ("job", Type::Character(VarChar), false, None),
    // ];
    // for attr in schema_vec {
    //     schema.add_field(attr.0, attr.1, attr.2, attr.3);
    // }
    // let layout = schema.to_layout();
    // let layout = Rc::new(layout);
    // let frame = storagemgr.pin(blk.clone()).unwrap();
    // let mut heap_page = HeapPage::new_from_empty(frame.clone(), &blk, layout.clone());
    // let tuples = vec![vec![
    //     ("id".to_string(), None),
    //     (
    //         "name".to_string(),
    //         Some("Omar".to_string().as_bytes().to_vec()),
    //     ),
    //     ("salary".to_string(), Some(5000_u32.to_ne_bytes().to_vec())),
    //     (
    //         "job".to_string(),
    //         Some("Engineer".to_string().as_bytes().to_vec()),
    //     ),
    // ]];
    // for tuple in tuples {
    //     let tuple = Tuple::new(tuple, layout.clone());
    //     heap_page.insert_tuple(tuple)
    // }
    // storagemgr.flush_frame(frame.clone());
    // let field_names = vec!["name".to_string(), "job".to_string(), "salary".to_string()];
    // let retrieved_fields = heap_page.get_multiple_fields(field_names.clone(), 0);
    // println!("{:?}", retrieved_fields);
    //
    // let mut v : Vec<Box<dyn Any>> = vec![];
    // let x = Box::new(
    //     | x:&dyn Any | x.downcast_ref::<i32>().unwrap() > &500
    // );
    // let b = Box::new(
    //     | x:&dyn Any | x.downcast_ref::<String>().unwrap().contains("hello")
    // );
    //
    // v.push(x);
    // v.push(b);
    //
    // let e = (10,"hello world".to_string());
    //
    // let q = v.remove(0).downcast::<Box<fn(&dyn Any) -> bool>>().unwrap();
    // println!("{}", q(&10 as &dyn Any) );

    // let mut context = evalexpr::HashMapContext::new();
    // context.set_value("salary".to_string(),Value::Int(2 as IntType)).unwrap();
    // context.set_value("name".to_string(),Value::String("omar".to_string())).unwrap();
    // println!("{:?}",eval_boolean_with_context(" salary > 0 && name == \"omar\" ",&context).unwrap())

    let tree = evalexpr::build_operator_tree(" id > 0 && salary > 0 && name == \"omar\" ").unwrap();
    for x in tree.iter_read_variable_identifiers() {
        println!("{x}");
    }

    // let x = ConcreteType::VarChar("omar".to_string());
    // let y = ConcreteType::Char("omar".to_string());
    // let z = ConcreteType::Integer(2);
    // let v = ConcreteType::Integer(66);
    // let abc = vec![Some(x),Some(y),Some(z),None];
    // let x = ConcreteType::VarChar("samir".to_string());
    // let y = ConcreteType::Char("samir".to_string());
    // let z = ConcreteType::Integer(27);
    // let v = ConcreteType::Integer(-33);
    // let xyz = vec![Some(x),Some(y),None,Some(v)];
    // let abc = vec![abc,xyz];
    // let a = bincode::encode_to_vec(abc,bincode::config::standard()).unwrap();
    // let b : Vec<Vec<Option<ConcreteType>>> = bincode::decode_from_slice(a.as_slice(),bincode::config::standard()).unwrap().0;
    //
    // let r = vec![1,2];
}
