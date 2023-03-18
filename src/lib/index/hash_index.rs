use std::cell::RefCell;
use std::collections::HashMap;
use std::process::id;
use std::rc::Rc;
use positioned_io2::WriteAt;
use crate::query::concrete_types::ConcreteType;
use crate::schema::schema::Layout;
use crate::storage::blockid::BlockId;
use crate::table::tablemgr::{TableIter, TableManager};
use sdbm::sdbm_hash;
use crate::common::numerical::ByteMagic;
use crate::RcRefCell;
use crate::storage::buffermgr::FrameRef;
use crate::storage::storagemgr::StorageManager;

const IDX_RECORD_SIZE: usize = 15;

#[derive(Clone, Eq, PartialEq)]
pub struct Rid {
    block_num: u64,
    slot_num: u16
}

impl Rid {
    pub fn new(block_num: u64, slot_num: u16) -> Self {
        Self { block_num, slot_num }
    }
}

type Column = ConcreteType;
type Row = Vec<Column>;

pub struct IdxRecord {
    deleted: u8,
    rid: Rid,
    hash_val: u32,
}

impl IdxRecord {
    pub fn new(rid: Rid, hash_val: u32) -> Self {
        Self { deleted: 0, rid, hash_val }
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        let deleted = data[0];
        let block_num = data.extract_u64(1);
        let slot_num = data.extract_u16(9);
        let hash_val = data.extract_u32(11);
        let rid = Rid::new(block_num, slot_num);
        Self {
            deleted,
            rid,
            hash_val
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(IDX_RECORD_SIZE);
        data.push(self.deleted);
        data.extend(self.rid.block_num.to_ne_bytes());
        data.extend(self.rid.slot_num.to_ne_bytes());
        data.extend(self.hash_val.to_ne_bytes());
        data
    }
}

/*
pub struct Bucket {
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
}
*/

pub struct HashIndex {
    num_buckets: u16,
    idx_name: String,
    search_key: Option<String>,
    blocks: Vec<BlockId>,
    storage_mgr: Rc<RefCell<StorageManager>>,
}

impl HashIndex {
    pub fn new(num_buckets: u16, idx_name: String, blocks: Vec<BlockId>, storage_mgr: Rc<RefCell<StorageManager>>) -> Self {
        Self {
            num_buckets,
            idx_name,
            search_key: None,
            blocks,
            storage_mgr,
        }
    }

    pub fn hash_value(&self, data_val: String) -> u32 {
        let hash_val = sdbm_hash(data_val.as_str());
        hash_val
    }

    pub fn hash_code(&self, data_val: String) -> u64 {
        let bucket_id = (sdbm_hash(data_val.as_str()) % (self.num_buckets as u32) ) as u64;
        bucket_id
    }

    /*
    // pub fn get_rids(&self, data_val: String) -> Option<Vec<Rid>> {
    //     self.buckets.get(&self.hash_code(data_val.clone())).unwrap().get_rids(data_val)
    // } */

    /*
    pub fn get_tuples_fields(&mut self, rids: Vec<Rid>, field_names: Vec<String>) -> Vec<HashMap<String, Option<Vec<u8>>>> {
        let mut records = Vec::new();
        for rid in rids {
            records.push(self.tbl_mgr.get_fields(&rid.block_num, rid.slot_num.clone() as usize, field_names.clone()))
        }
        records
    }
    */

    /*
    // pub fn create_bucket(&mut self, data_val: String) -> &Bucket {
    //     let id = self.hash_code(data_val);
    //     let bucket_name = self.idx_name.clone() + id.to_string().as_str();
    //     self.buckets.insert(self.hash_code(data_val.clone()), Bucket::new(
    //         bucket_name,
    //         id,
    //         0,
    //         Vec::new()
    //     )).as_ref().unwrap()
    // }
    */

    fn create_bucket(&self, data_val: String) -> BucketPage {
        let hash_val = self.hash_code(data_val);
        let block = self.blocks.iter().find(|block| block.block_num == (hash_val)).unwrap();
        let frame = self.storage_mgr.borrow_mut().pin(block.clone()).unwrap();
        BucketPage::new(frame)
    }

    // double overflows --> Splitting
    // notify the tblmgr by the new block result from overflow which notifies the catalog
    pub fn insert_record(&mut self, data_val: String, blk_num: u64, slot_num: u16) {
        let mut bucket_page = self.create_bucket(data_val.clone());
        let rid = Rid::new(blk_num, slot_num);
        let hash_val = self.hash_value(data_val.clone());
        let idx_record = IdxRecord::new(rid, hash_val);
        if let Err(_) = bucket_page.insert_record(&idx_record) {
            let block = self.storage_mgr.borrow_mut().extend_file(self.blocks[0].filename.as_str());
            let frame = self.storage_mgr.borrow_mut().pin(block.clone()).unwrap();
            let mut new_bucket_page = BucketPage::new_from_empty(frame, 1);
            new_bucket_page.insert_record(&idx_record);
            bucket_page.set_overflow(block.block_num as u16);
            self.blocks.push(block);
        }
    }
}

struct BucketPage {
    pub frame: FrameRef,
    depth: u8,
    num_records: u16,
    overflow: Option<u16>,
    vacuuming: bool,
}

impl BucketPage {
    pub fn new(frame: FrameRef) -> Self {
        let hash_frame = frame.clone();
        let mut frame_ref = hash_frame.borrow_mut();
        frame_ref.update_replace_stats();
        let depth = frame_ref.page.payload.as_slice()[0];
        let num_records = frame_ref.page.payload.as_slice().extract_u16(1);
        let overflow = frame_ref.page.payload.as_slice().extract_u16(3);
        let overflow = if overflow == 0 {None} else { Some(overflow) };
        Self {
            frame,
            depth,
            num_records,
            overflow,
            vacuuming: false
        }
    }
    fn init(frame: &FrameRef, depth: u8) {
        let mut frame = frame.borrow_mut();
        let metadata = [1, 0, 0, 0, 0];
        frame.update_replace_stats();
        frame.write(metadata.as_slice())
    }

    /// Creates an empty Heap Page and returns it
    pub fn new_from_empty(frame: FrameRef, depth: u8) -> Self {
        BucketPage::init(&frame, depth);
        BucketPage::new(frame)
    }

    pub fn set_overflow(&mut self, overflow: u16) {
        self.overflow = Some(overflow);
        self.frame.borrow_mut().page.write_bytes(overflow.to_ne_bytes().as_slice(), 1);
    }

    // Check overflow
    fn find_all(&self, hash_val: u32) -> Vec<Rid> {
        let frame = self.frame.borrow();
        let mut rids = Vec::new();
        let mut pos = 5_usize;
        for i in 0..self.num_records {
            let idx_record = IdxRecord::from_bytes(&frame.page.payload[pos..pos+IDX_RECORD_SIZE]);
            if idx_record.hash_val == hash_val && idx_record.deleted == 0{
                rids.push(idx_record.rid);
            }
            pos += IDX_RECORD_SIZE;
        }
        rids
    }

    fn insert_record(&mut self, record: &IdxRecord) -> Result<(), String> {
        let mut frame = self.frame.borrow_mut();
        let pos = 5_usize + self.num_records as usize * IDX_RECORD_SIZE;
        if frame.page.payload.len() - pos < IDX_RECORD_SIZE{
            return Err("Insufficient Space".to_string());
        }
        self.num_records += 1;
        frame.page.write_bytes(self.num_records.to_ne_bytes().as_slice(), 3 as u64);
        frame.page.write_bytes(record.to_bytes().as_slice(), pos as u64);
        Ok(())
    }
}