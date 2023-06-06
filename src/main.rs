#![allow(non_snake_case)]
mod init;

use std::cell::RefCell;
use aqua::database::server::DatabaseServer;
use aqua::sql::parser::parse_query;
use std::env;
use std::process::exit;
use std::rc::Rc;
use aqua::index::btree_index::{BPTree, Rid};
use aqua::schema::types::NumericType::Integer;
use aqua::schema::types::Type;
use aqua::sql::create_table::IndexType::Btree;
use aqua::storage::blockid::BlockId;
use aqua::storage::storagemgr::StorageManager;


fn main() {
    // let query = "create table omar( samir int primary key,koko smallint references oo(bad) , create index hash mazen on (id))";
    // let query = parse_query(query);
    // dbg!(query);

    // let query = "insert into omar (ok) values (123)";
    // let query = parse_query(query);
    // dbg!(query);

    // let opts = env::args().collect::<Vec<_>>();
    // if let Some(init) = opts.get(1) {
    //     if init == "init" {
    //         init::init_aqua();
    //     }
    // } else {
    //     init::init_homedir();
    // }
    // let server = DatabaseServer::new("hi", vec!["127.0.0.1:2710".to_string()]);
    // server.run()

    /*let mut btree = BPTree::new();
    for i in 0..12 {
        let rid = Rid::new(0, i);
        btree.insert((i * 4) as u32, rid);
        println!("Inserted {}", i);
    }
    for i in 12..100 {
        let rid = Rid::new(0, i);
        btree.insert((i * 4) as u32, rid);
        println!("Inserted {}", i);
    }
    for i in 0..100 {
        println!("{:?}", btree.search(i * 4));
    }*/



    const DB_DIR: &str = "tests/db/";
    let test_file = "test_btree";
    let BLK_SIZE = 4096;
    let mut storage_manager = StorageManager::new(DB_DIR, BLK_SIZE, 500);
    let root_block = storage_manager.extend_file(test_file);
    let mut btree = BPTree::init(root_block.clone(), Type::Numeric(Integer), Rc::new(RefCell::new(storage_manager)));

    for slot in 0..50000_u16 {
        let rid = Rid::new(root_block.clone().block_num,slot);
        btree.insert(((slot) as i32).to_ne_bytes().to_vec(), rid);
    }

    btree.print_root();

    println!("///////////////////////////");


    for key in 20000..21000_i32 {
        let res = btree.search((key).to_ne_bytes().to_vec());
        dbg!(res);
    }
}
