use aqua::query::executor::Executor;
use aqua::query::seq_scan::SeqScan;
use aqua::storage::storagemgr::StorageManager;
use aqua::table::tablemgr::TableManager;
use aqua::RcRefCell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
mod common;
use crate::common::random::distill_schema;
use common::{random, utils};

#[cfg(windows)]
const db_dir: &str = "tests\\db\\";

#[cfg(unix)]
const db_dir: &str = "tests/db";

fn create_table(table_name: &str) -> TableManager {
    let test_file = table_name;
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
    tblmgr
}

fn populate_table(tblmgr: &mut TableManager) {
    let some_schema = utils::some_schema();
    let schema = distill_schema(some_schema);
    let tuples = random::generate_random_tuples(&schema, 1000);
    tuples.into_iter().for_each(|t| tblmgr.try_insert_tuple(t));
    tblmgr.flush_all();
}

#[test]
fn projection_test() {
    let table_name = "projection";
    let mut table = create_table(table_name);
    populate_table(&mut table);
    let projection = SeqScan {
        table: "projection".to_string(),
        fields: vec!["id".to_string(), "name".to_string()],
    };
    let max_mem = 4e3 as usize;
    let table_map = HashMap::from([(table_name.to_string(), table)]);
    let mut executor = Executor::new(max_mem, &table_map);
    executor.execute_seqscan(projection);
}
