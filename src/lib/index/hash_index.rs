use std::collections::HashMap;
use crate::query::concrete_types::ConcreteType;
use crate::schema::schema::Layout;
use crate::storage::blockid::BlockId;
use crate::table::tablemgr::{TableIter, TableManager};
use sdbm::sdbm_hash;

#[derive(Clone)]
struct Rid {
    block_num: BlockId,
    slot_num: u16
}

impl Rid {
    pub fn new(block_num: BlockId, slot_num: u16) -> Self {
        Self { block_num, slot_num }
    }
}

type Column = ConcreteType;
type Row = Vec<Column>;

struct IdxRecord {
    deleted: u8,
    rid: Rid,
    data_val: String,
}

impl IdxRecord {
    pub fn new(rid: Rid, data_val: String) -> Self {
        Self { deleted: 0, rid, data_val }
    }
}

struct Bucket {
    name: String,
    id: u16,
    num_records: u16,
    level: u8,
    data: Vec<IdxRecord>,
    overflow: Option<u16>,
}

impl Bucket {
    pub fn new(name: String, id: u16, num_records: u16, data: Vec<IdxRecord>) -> Self {
        Self {
            name,
            id,
            num_records,
            level: 0,
            data,
            overflow: None }
    }

    pub fn get_rids(&self, search_key: String) -> Option<Vec<Rid>> {
        let mut rids = Vec::new();
        for record in &self.data {
            if record.data_val == search_key{
                rids.push(record.rid.clone());
            }
        }
        Some(rids)
    }
}

struct HashIndex {
    num_buckets: u16,
    idx_name: String,
    search_key: Option<String>,
    layout: Layout,
    tbl_mgr: TableManager,
    buckets: HashMap<u16, Bucket>,
}

impl HashIndex {
    pub fn new(num_buckets: u16, idx_name: String, layout: Layout, tbl_mgr: TableManager) -> Self {
        Self {
            num_buckets,
            idx_name,
            search_key: None,
            layout,
            tbl_mgr,
            buckets: HashMap::new()}
    }

    pub fn hash_code(&self, data_val: String) -> u16 {
        let bucket_id = (sdbm_hash(data_val.as_str()) % (self.num_buckets as u32) ) as u16;
        bucket_id
    }

    pub fn get_rids(&self, data_val: String) -> Option<Vec<Rid>> {
        self.buckets.get(&self.hash_code(data_val.clone())).unwrap().get_rids(data_val)
    }

    pub fn get_tuples_fields(&mut self, rids: Vec<Rid>, field_names: Vec<String>) -> Vec<HashMap<String, Option<Vec<u8>>>> {
        let mut records = Vec::new();
        for rid in rids {
            records.push(self.tbl_mgr.get_fields(&rid.block_num, rid.slot_num.clone() as usize, field_names.clone()))
        }
        records
    }
}