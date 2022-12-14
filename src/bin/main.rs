#![allow(non_snake_case)]

use aqua::schema::null_bitmap::NullBitMap;
use aqua::schema::schema::Schema;
use aqua::schema::types::CharType::VarChar;
use aqua::schema::types::NumericType::{Integer, SmallInt};
use aqua::schema::types::Type;
use aqua::storage::blockid::BlockId;
use aqua::storage::heap::HeapPage;
use aqua::storage::storagemgr::StorageManager;
use aqua::storage::tuple::Tuple;
use std::rc::Rc;

fn main() {
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

    const db_dir: &str = "tests/db/";

    let test_file = "write_read_tuples";
    let BLK_SIZE = 4096;
    let mut storagemgr = StorageManager::new(db_dir, BLK_SIZE, 100);
    let blk = BlockId {
        filename: test_file.to_string(),
        block_num: 0,
    };
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
    let layout = schema.to_layout();
    let layout = Rc::new(layout);
    let frame = storagemgr.pin(blk.clone()).unwrap();
    let mut heap_page = HeapPage::new_from_empty(frame.clone(), &blk, layout.clone());
    let tuples = vec![vec![
        ("id".to_string(), None),
        ("name".to_string(), Some("Omar".to_string().as_bytes().to_vec())),
        ("salary".to_string(), Some(5000_u32.to_ne_bytes().to_vec())),
        (
            "job".to_string(),
            Some("Engineer".to_string().as_bytes().to_vec()),
        ),
    ]];
    for tuple in tuples {
        let tuple = Tuple::new(tuple, layout.clone());
        heap_page.insert_tuple(tuple)
    }
    storagemgr.flush_frame(frame.clone());
    let field_names = vec!["name".to_string(), "job".to_string(), "salary".to_string()];
    let retrieved_fields = heap_page.get_multiple_fields(field_names.clone(), 0);
    println!("{:?}", retrieved_fields);
}
