use std::rc::Rc;
use aqua::schema::schema::Schema;
use aqua::schema::types::CharType::VarChar;
use aqua::schema::types::NumericType::{Integer, SmallInt};
use aqua::schema::types::Type;
use aqua::storage::blockid::BlockId;
use aqua::storage::heap::{HeapPage, PageHeader};
use aqua::storage::storagemgr::StorageManager;
use aqua::storage::tuple::Tuple;

mod common;
use common::utils;
use crate::common::random;

const db_dir: &str = "tests/db/";
#[test]
fn empty_page(){

    let test_file = "empty_page";
    let BLK_SIZE = 4096;
    let mut storagemgr = StorageManager::new(db_dir, BLK_SIZE,100);
    let blk = BlockId {
        filename: test_file.to_string(),
        block_num: 0,
    };
    let mut schema = Schema::new();
    let schema_vec = vec![
        ("id",Type::Numeric(SmallInt),false,None),
        ("name",Type::Character(VarChar),false,None),
        ("salary",Type::Numeric(Integer),false,None),
        ("job",Type::Character(VarChar),false,None)
    ];
    for attr in schema_vec{
        schema.add_field(
            attr.0,
            attr.1,
            attr.2,
            attr.3
        );
    }
    let layout = schema.to_layout();
    let frame = storagemgr.pin(blk.clone()).unwrap();
    let heap_page = HeapPage::new_from_empty(frame, &blk, Rc::new(layout));
    println!("{:?}", heap_page);
    let page_header = PageHeader{
        space_start: 4,
        space_end: 4095,
    };
    assert_eq!(heap_page.header, page_header);
}

#[test]
fn filling_page(){
    let test_file = "filling_page";
    let BLK_SIZE = 4096;
    let mut storagemgr = StorageManager::new(db_dir, BLK_SIZE,100);
    let blk = BlockId {
        filename: test_file.to_string(),
        block_num: 0,
    };
    let mut schema = Schema::new();
    let schema_vec = vec![
        ("id",Type::Numeric(SmallInt),false,None),
        ("name",Type::Character(VarChar),false,None),
        ("salary",Type::Numeric(Integer),false,None),
        ("job",Type::Character(VarChar),false,None)
    ];
    for attr in schema_vec{
        schema.add_field(
            attr.0,
            attr.1,
            attr.2,
            attr.3
        );
    }
    let layout = schema.to_layout();
    let layout = Rc::new(layout);
    let frame = storagemgr.pin(blk.clone()).unwrap();
    let mut heap_page = HeapPage::new_from_empty(frame, &blk, layout.clone());
    let schema = random::distill_schema(schema);
    let tups = random::generate_random_tuples(&schema,5);
    for tuple in tups {
        let tuple = Tuple::new(tuple, layout.clone());
        heap_page.insert_tuple(tuple)
    }
    storagemgr.flush_frame(heap_page.frame.clone());
    println!("{:?}", heap_page);
}

#[test]
fn write_read_tuples() {
    let test_file = "filling_page";
    let BLK_SIZE = 4096;
    let mut storagemgr = StorageManager::new(db_dir, BLK_SIZE,100);
    let blk = BlockId {
        filename: test_file.to_string(),
        block_num: 0,
    };
    let mut schema = Schema::new();
    let schema_vec = vec![
        ("id",Type::Numeric(SmallInt),false,None),
        ("name",Type::Character(VarChar),false,None),
        ("salary",Type::Numeric(Integer),false,None),
        ("job",Type::Character(VarChar),false,None)
    ];
    for attr in schema_vec{
        schema.add_field(
            attr.0,
            attr.1,
            attr.2,
            attr.3
        );
    }
    let layout = schema.to_layout();
    let layout = Rc::new(layout);
    let frame = storagemgr.pin(blk.clone()).unwrap();
    let mut heap_page = HeapPage::new_from_empty(frame, &blk, layout.clone());
    let tuples = vec![
        vec![
            ("id".to_string(), 100_u16.to_ne_bytes().to_vec()),
            ("name".to_string(), "Omar".to_string().as_bytes().to_vec()),
            ("salary".to_string(), 5000_u32.to_ne_bytes().to_vec()),
            ("job".to_string(), "Engineer".to_string().as_bytes().to_vec())
        ],
        // vec![
        //     ("id".to_string(), 101_u16.to_ne_bytes().to_vec()),
        //     ("name".to_string(), "Abdallah".to_string().as_bytes().to_vec()),
        //     ("salary".to_string(), 5000_u32.to_ne_bytes().to_vec()),
        //     ("job".to_string(), "Student".to_string().as_bytes().to_vec())
        // ]
    ];
    for tuple in tuples {
        let tuple = Tuple::new(tuple, layout.clone());
        heap_page.insert_tuple(tuple)
    }
    storagemgr.flush_frame(heap_page.frame.clone());
    println!("{:?}", heap_page);
    let retrieved_name = heap_page.get_field("job", 0);
    println!("{:?}", retrieved_name);
}