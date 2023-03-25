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
use power_of_two::power_of_two;

const IDX_RECORD_SIZE: usize = 15;

/// Record ID entity encapsulating the block number and the slot number of a certain tuple.
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

/// The record as stored in a hash index with the Rid and the hash value.
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

/// The hash index which is most likely treated as a table.
/// The hash index has some specific attributes such as number of buckets inside the index;
/// the index name; the search key of the index;
/// The global depth of the index which resembles the greatest local depth of a bucket inside it.
/// The index is given its block ids by the table manager, a reference to the storage manager to extend a file with some blocks.
/// Also given a reference to the table manager to notify it by the newly added bucket blocks.
pub struct HashIndex {
    num_buckets: u16,
    idx_name: String,
    search_key: Option<String>,
    global_depth: u8,
    blocks: Vec<BlockId>,
    storage_mgr: Rc<RefCell<StorageManager>>,
    tbl_mgr: Rc<RefCell<TableManager>>,
}

impl HashIndex {
    pub fn new(num_buckets: u16,
               idx_name: String,
               blocks: Vec<BlockId>,
               storage_mgr: Rc<RefCell<StorageManager>>,
               tbl_mgr: Rc<RefCell<TableManager>>,
    ) -> Self {
        Self {
            num_buckets,
            idx_name,
            search_key: None,
            global_depth: 8,
            blocks,
            storage_mgr,
            tbl_mgr,
        }
    }

    /// Encoding the raw string to a 32 bit hash value.
    pub fn hash_value(&self, data_val: String) -> u32 {
        let hash_val = sdbm_hash(data_val.as_str());
        hash_val
    }

    /// After encoding the string, returning the bucket number corresponding to the hash value.
    /// The bucket number is the hash value modulus 2^(the global depth).
    /// As an initial value, the global depth and local depths of all bucket will be set to 8 which is 1 byte.
    /// The depth shows the number of least significant bits shared by the same Index records hash values inside a certain bucket.
    /// Initially, the least significant byte will be the same for each record inside the same bucket which will then
    /// increase by splitting the bucket when there is no more space.
    /// for ex: 5, 261, 517, 773, 1029, 1285 at first share the same bucket with depth 8 (0 0 0 0 0 1 0 1),
    /// Assume the bucket is now full and inserting  1541 hashes to the same bucket, now we need to split the bucket
    /// into 2 new buckets... the depth should now increase to 9 bits which means that one split bucket will have
    /// hash values sharing (0 0 0 0 0 0 1 0 1) and the other will have hash values sharing (1 0 0 0 0 0 1 0 1).
    /// Now 5, 517, 1029 and the newly inserted 1541 after the modulus by 2^9 will hash to the same bucket of ID "5",
    /// while 261, 773, and 1285 after the modulus by 2^9 will hash to the other bucket of ID "261".
    pub fn hash_code(&self, data_val: String) -> u64 {
        let hash_val = sdbm_hash(data_val.as_str());
        let bucket_id = ((hash_val) % (2_u32.pow(self.global_depth as u32))) as u64;
        let bucket = self.create_bucket(data_val);
        if bucket.is_some() {
            bucket_id
        }
        else {
            let mut global_depth = self.global_depth;
            let mut new_bucket_id = bucket_id;
            loop {
                global_depth -= 1;
                new_bucket_id = ((hash_val) % (2_u32.pow(self.global_depth as u32))) as u64;
                let new_bucket = self.create_bucket_from_hash_value(bucket_id as u32);
                if new_bucket.is_some() {
                    return bucket_id;
                }
            }
        }
    }

    /// A helper function to get the bucket ID from a hash value.
    pub fn hash_val_to_bucket(&self, hash_val: u32) -> u64 {
        (hash_val % (2_u32.pow(self.global_depth as u32))) as u64
    }

    /// Creates a bucket page from an existing block by hashing the data value to get bucket ID.
    fn create_bucket(&self, data_val: String) -> Option<BucketPage> {
        let hash_val = self.hash_code(data_val);
        let block = self.blocks.iter().find(|block| block.block_num == (hash_val));
        if block.is_some() {
            let frame = self.storage_mgr.borrow_mut().pin(block.unwrap().clone()).unwrap();
            return Some(BucketPage::new(frame));
        }
        None
    }

    /// A helper function that creates a bucket page by taking the data value already hashed.
    fn create_bucket_from_hash_value(&self, hash_val: u32) -> Option<BucketPage> {
        let block = self.blocks.iter().find(|block| block.block_num == (self.hash_val_to_bucket(hash_val)));
        if block.is_some() {
            let frame = self.storage_mgr.borrow_mut().pin(block.unwrap().clone()).unwrap();
            return Some(BucketPage::new(frame));
        }
        None
    }

    /// Inserting an index record inside a bucket after hashing the data value, trying to insert in the
    /// corresponding bucket, if there is no space, try inserting in the overflow bucket, if there is even no
    /// space in the overflow bucket, now we need to split it and re insert the record after splitting.
    pub fn insert_record(&mut self, data_val: String, blk_num: u64, slot_num: u16) {
        let mut bucket_page = self.create_bucket(data_val.clone()).unwrap();
        let rid = Rid::new(blk_num, slot_num);
        let hash_val = self.hash_value(data_val);
        let idx_record = IdxRecord::new(rid, hash_val);
        if bucket_page.insert_record(&idx_record).is_err() {
            if bucket_page.overflow.is_some() {
                let block = self.blocks.iter()
                    .find(|block| block.block_num == bucket_page.overflow.unwrap() as u64).unwrap();
                let frame = self.storage_mgr.borrow_mut().pin(block.clone()).unwrap();
                let mut overflow_bucket = BucketPage::new(frame);
                if overflow_bucket.insert_record(&idx_record).is_err() {
                    self.split_bucket(&bucket_page, idx_record);
                }
            } else {
                let block = self.storage_mgr.borrow_mut().extend_file(self.blocks[0].filename.as_str());
                let frame = self.storage_mgr.borrow_mut().pin(block.clone()).unwrap();
                let mut new_bucket_page = BucketPage::new_from_empty(frame, bucket_page.depth);
                new_bucket_page.insert_record(&idx_record);
                bucket_page.set_overflow(block.block_num as u16);
                self.tbl_mgr.borrow_mut().add_index_block(&block);
                self.blocks.push(block);
            }
        }
    }

    /// Reinserting the records after splitting a bucket.
    fn reinsert_record(&mut self, idx_record: IdxRecord) {
        let mut bucket_page = self.create_bucket_from_hash_value(idx_record.hash_val).unwrap();
        if bucket_page.insert_record(&idx_record).is_err() {
            if bucket_page.overflow.is_some() {
                let block = self.blocks.iter()
                    .find(|block| block.block_num == bucket_page.overflow.unwrap() as u64).unwrap();
                let frame = self.storage_mgr.borrow_mut().pin(block.clone()).unwrap();
                let mut overflow_bucket = BucketPage::new(frame);
                overflow_bucket.insert_record(&idx_record);
            } else {
                let block = self.storage_mgr.borrow_mut().extend_file(self.blocks[0].filename.as_str());
                let frame = self.storage_mgr.borrow_mut().pin(block.clone()).unwrap();
                let mut new_bucket_page = BucketPage::new_from_empty(frame, bucket_page.depth);
                new_bucket_page.insert_record(&idx_record);
                bucket_page.set_overflow(block.block_num as u16);
                self.tbl_mgr.borrow_mut().add_index_block(&block);
                self.blocks.push(block);
            }
        }
    }

    /// Reading all index records inside a bucket and its overflow, then creating 2 new buckets and
    /// reinserting each index record again after the new hash coding.
    fn split_bucket(&mut self, bucket_page: &BucketPage, idx_record: IdxRecord) {
        if bucket_page.depth == self.global_depth {
            self.global_depth += 1;
        }
        let block_num = self.hash_val_to_bucket(idx_record.hash_val);
        let blk_idx = self.blocks.iter()
            .position(|block| block.block_num == block_num).unwrap();
        self.blocks.remove(blk_idx);
        self.tbl_mgr.borrow_mut().remove_index_block(block_num);
        let filename = self.blocks[0].filename.as_str();
        let mut bucket_records = bucket_page.read_all_bucket_records();
        if bucket_page.overflow.is_some() {
            let block = self.blocks.iter()
                .find(|block| block.block_num == bucket_page.overflow.unwrap() as u64).unwrap();
            let frame = self.storage_mgr.borrow_mut().pin(block.clone()).unwrap();
            let mut overflow_bucket = BucketPage::new(frame);
            bucket_records.append(overflow_bucket.read_all_bucket_records().as_mut());
        }
        let mut block_one = self.storage_mgr.borrow_mut().extend_file(filename);
        block_one.block_num = (bucket_records[0].hash_val % bucket_page.depth as u32) as u64;
        let frame_one = self.storage_mgr.borrow_mut().pin(block_one.clone()).unwrap();
        let bucket_split_one = BucketPage::new_from_empty(frame_one, bucket_page.depth+1);
        let mut block_two = self.storage_mgr.borrow_mut().extend_file(filename);
        block_two.block_num = (bucket_records[0].hash_val % bucket_page.depth as u32) as u64
            + 2_u32.pow(bucket_page.depth as u32) as u64;
        let frame_two = self.storage_mgr.borrow_mut().pin(block_two.clone()).unwrap();
        let bucket_split_two = BucketPage::new_from_empty(frame_two, bucket_page.depth+1);
        self.tbl_mgr.borrow_mut().add_index_block(&block_one);
        self.tbl_mgr.borrow_mut().add_index_block(&block_two);

        self.blocks.push(block_one);
        self.blocks.push(block_two);
        for record in bucket_records {
            self.reinsert_record(record);
        }
        self.reinsert_record(idx_record);
    }

    /// Get all the rids of the matched index records with the search key.
    pub fn get_rids(&self, search_key: String) -> Vec<Rid> {
        let hash_val = self.hash_value(search_key.clone());
        let mut bucket_page = self.create_bucket(search_key).unwrap();
        let mut rids = bucket_page.find_all(hash_val);
        if bucket_page.overflow.is_some() {
            let block = self.blocks.iter()
                .find(|block| block.block_num == bucket_page.overflow.unwrap() as u64).unwrap();
            let frame = self.storage_mgr.borrow_mut().pin(block.clone()).unwrap();
            let mut overflow_bucket = BucketPage::new(frame);
            rids.append(overflow_bucket.find_all(hash_val).as_mut());
        }
        rids
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
        let metadata = [depth, 0, 0, 0, 0];
        frame.update_replace_stats();
        frame.write(metadata.as_slice())
    }

    /// Creates an empty Hash Index Page and returns it
    pub fn new_from_empty(frame: FrameRef, depth: u8) -> Self {
        BucketPage::init(&frame, depth);
        BucketPage::new(frame)
    }

    /// Set the overflow bucket by writing the overflow bucket ID.
    pub fn set_overflow(&mut self, overflow: u16) {
        self.overflow = Some(overflow);
        self.frame.borrow_mut().page.write_bytes(overflow.to_ne_bytes().as_slice(), 1);
    }

    /// Retrieving the Rids with hash value similar to the query's hash value.
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

    /// Writing the index record bytes inside the bucket if there is enough space, else returning error.
    fn insert_record(&mut self, record: &IdxRecord) -> Result<(), String> {
        let mut frame = self.frame.borrow_mut();
        let pos = 5_usize + self.num_records as usize * IDX_RECORD_SIZE;
        if frame.page.payload.len() - pos < IDX_RECORD_SIZE{
            return Err("Insufficient Space".to_string());
        }
        self.num_records += 1;
        frame.page.write_bytes(self.num_records.to_ne_bytes().as_slice(), 3);
        frame.page.write_bytes(record.to_bytes().as_slice(), pos as u64);
        Ok(())
    }

    /// Retrieve all the index records inside a bucket.
    fn read_all_bucket_records(&self) -> Vec<IdxRecord> {
        let frame = self.frame.borrow();
        let mut pos = 5_usize;
        let mut idx_records = Vec::new();
        for i in 0..self.num_records {
            let idx_record = IdxRecord::from_bytes(&frame.page.payload[pos..pos + IDX_RECORD_SIZE]);
            idx_records.push(idx_record);
            pos += IDX_RECORD_SIZE;
        }
        idx_records
    }
}