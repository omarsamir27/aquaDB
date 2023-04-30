use crate::common::fileops::{read_file, write_file};
use crate::common::numerical::ByteMagic;
use crate::index::Index;
use crate::query::concrete_types::ConcreteType;
use crate::schema::schema::Layout;
use crate::sql::create_table::IndexType;
use crate::storage::blockid::BlockId;
use crate::storage::buffermgr::FrameRef;
use crate::storage::storagemgr::StorageManager;
use crate::table::tablemgr::{TableIter, TableManager};
use crate::RcRefCell;
use positioned_io2::WriteAt;
use sdbm::sdbm_hash;
use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::process::id;
use std::ptr::addr_of_mut;
use std::rc::Rc;

const IDX_RECORD_SIZE: usize = 15;

/// Record ID entity encapsulating the block number and the slot number of a certain tuple.
#[derive(Clone, Eq, PartialEq)]
pub struct Rid {
    block_num: u64,
    slot_num: u16,
}

impl Rid {
    pub fn new(block_num: u64, slot_num: u16) -> Self {
        Self {
            block_num,
            slot_num,
        }
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
        Self {
            deleted: 0,
            rid,
            hash_val,
        }
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
            hash_val,
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

pub struct BucketDirectory {
    index_dir_file: PathBuf,
    global_depth: u8,
    buckets_num: u16,
    bucket_map: HashMap<u32, u64>,
    size_in_bytes: usize,
}

impl BucketDirectory {
    pub fn new(mut index_dir_file: &Path) -> Self {
        let mut buffer = fs::read(index_dir_file).unwrap();
        let buffer = buffer.as_slice();
        let size = buffer.len();
        let global_depth = buffer[0];
        let buckets_num = buffer.extract_u16(1);
        let mut bucket_map = HashMap::new();
        let mut pos = 3_usize;
        for i in 0..buckets_num {
            let bucket_id = buffer.extract_u32(pos);
            pos += 4;
            let block_num = buffer.extract_u64(pos);
            pos += 8;
            bucket_map.insert(bucket_id, block_num);
        }
        Self {
            index_dir_file: index_dir_file.to_owned(),
            global_depth,
            buckets_num,
            bucket_map,
            size_in_bytes: size,
        }
    }

    pub fn init(mut index_dir_file: &Path, initial_global_depth: u8) {
        let global_depth = initial_global_depth;
        let buckets_num = 2_u16.pow(global_depth as u32);
        let mut data = Vec::new();
        data.extend_from_slice(global_depth.to_ne_bytes().as_slice());
        data.extend_from_slice(buckets_num.to_ne_bytes().as_slice());
        let mut starting_blk_num = 0_u64;
        for (starting_blk_num, i) in (0..buckets_num as u32).enumerate() {
            data.extend_from_slice(i.to_ne_bytes().as_slice());
            data.extend_from_slice((starting_blk_num as u64).to_ne_bytes().as_slice());
        }
        write_file(index_dir_file, data);
    }

    pub fn insert_bucket(&mut self, bucket_id: u32, blk_num: u64) {
        self.bucket_map.insert(bucket_id, blk_num);
    }

    pub fn remove_bucket(&mut self, bucket_id: u32) -> Option<u64> {
        self.bucket_map.remove(&bucket_id)
    }

    pub fn get(&mut self, bucket_id: u32) -> Option<&u64> {
        self.bucket_map.get(&bucket_id)
    }

    pub fn bucket_map_to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(self.global_depth.to_ne_bytes().as_slice());
        for (k, v) in self.bucket_map.clone() {
            data.extend_from_slice(k.to_ne_bytes().as_slice());
            data.extend_from_slice(v.to_ne_bytes().as_slice());
        }
        data
    }

    pub fn flush(&mut self) {
        write_file(self.index_dir_file.as_path(), self.bucket_map_to_bytes());
    }
}

/// The hash index which is most likely treated as a table.
/// The hash index has some specific attributes such as number of buckets inside the index;
/// the index name; the search key of the index;
/// The global depth of the index which resembles the greatest local depth of a bucket inside it.
/// The index is given its block ids by the table manager, a reference to the storage manager to extend a file with some blocks.
/// Also given a reference to the table manager to notify it by the newly added bucket blocks.
pub struct HashIndex {
    bucket_dir: BucketDirectory,
    num_buckets: u16,
    idx_name: String,
    search_key: Option<String>,
    global_depth: u8,
    blocks: Vec<BlockId>,
}

impl HashIndex {
    pub fn new(bucket_dir_path: &Path, idx_name: String, blocks: Vec<BlockId>) -> Self {
        let bucket_dir = BucketDirectory::new(bucket_dir_path);
        Self {
            global_depth: bucket_dir.global_depth,
            num_buckets: 2_u16.pow(bucket_dir.global_depth as u32),
            bucket_dir,
            idx_name,
            search_key: None,
            blocks,
        }
    }

    pub fn init(index_file: &Path, dir_file: &Path, global_depth: u8) {
        let blk_size = 4096;
        let data = vec![0_u8; blk_size * 2_u32.pow(global_depth as u32) as usize];
        write_file(index_file, data);
        BucketDirectory::init(dir_file, global_depth);
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
    pub fn hash_code(&self, data_val: String) -> u32 {
        let hash_val = sdbm_hash(data_val.as_str());
        let bucket_id = ((hash_val) % (2_u32.pow(self.global_depth as u32)));
        let block_num = self.bucket_dir.bucket_map.get(&bucket_id);
        if let Some(..) = block_num {
            bucket_id
        } else {
            let mut global_depth = self.global_depth;
            let mut new_bucket_id = bucket_id;
            loop {
                global_depth -= 1;
                new_bucket_id = ((hash_val) % (2_u32.pow(self.global_depth as u32)));
                let new_block_num = self.bucket_dir.bucket_map.get(&new_bucket_id);
                if let Some(..) = new_block_num {
                    return new_bucket_id;
                }
            }
        }
    }

    /// A helper function to get the bucket ID from a hash value.
    pub fn hash_val_to_bucket(&self, hash_val: u32) -> u32 {
        (hash_val % (2_u32.pow(self.global_depth as u32)))
    }

    /// Creates a bucket page from an existing block by hashing the data value to get bucket ID.
    fn create_bucket(
        &self,
        data_val: String,
        mut storage_mgr: &mut RefMut<StorageManager>,
    ) -> BucketPage {
        let bucket_id = self.hash_code(data_val);
        let block_num = self.bucket_dir.bucket_map.get(&bucket_id);
        let block = BlockId::new(self.idx_name.as_str(), *block_num.unwrap());
        let frame = storage_mgr.pin(block).unwrap();
        BucketPage::new(frame)
    }

    /// A helper function that creates a bucket page by taking the data value already hashed.
    fn create_bucket_from_hash_value(
        &self,
        hash_val: u32,
        mut storage_mgr: &mut RefMut<StorageManager>,
    ) -> BucketPage {
        let bucket_id = self.hash_val_to_bucket(hash_val);
        let block_num = self.bucket_dir.bucket_map.get(&bucket_id);
        let block = BlockId::new(self.idx_name.as_str(), *block_num.unwrap());
        let frame = storage_mgr.pin(block).unwrap();
        BucketPage::new(frame)
    }

    /// Inserting an index record inside a bucket after hashing the data value, trying to insert in the
    /// corresponding bucket, if there is no space, try inserting in the overflow bucket, if there is even no
    /// space in the overflow bucket, now we need to split it and re insert the record after splitting.
    pub fn insert_record(
        &mut self,
        data_val: String,
        blk_num: u64,
        slot_num: u16,
        mut storage_mgr: RefMut<StorageManager>,
    ) {
        let mut bucket_page = self.create_bucket(data_val.clone(), &mut storage_mgr);
        let rid = Rid::new(blk_num, slot_num);
        let hash_val = self.hash_value(data_val);
        let idx_record = IdxRecord::new(rid, hash_val);
        if bucket_page.insert_record(&idx_record).is_err() {
            if bucket_page.overflow.is_some() {
                let block = self
                    .blocks
                    .iter()
                    .find(|block| block.block_num == bucket_page.overflow.unwrap())
                    .unwrap();
                let frame = storage_mgr.pin(block.clone()).unwrap();
                let mut overflow_bucket = BucketPage::new(frame);
                if overflow_bucket.insert_record(&idx_record).is_err() {
                    self.split_bucket(&bucket_page, idx_record, storage_mgr);
                }
            } else {
                let block = storage_mgr.extend_file(self.blocks[0].filename.as_str());
                let frame = storage_mgr.pin(block.clone()).unwrap();
                let mut new_bucket_page = BucketPage::new_from_empty(frame, bucket_page.depth);
                new_bucket_page.insert_record(&idx_record);
                bucket_page.set_overflow(block.block_num);
                self.blocks.push(block);
            }
        }
    }

    /// Reinserting the records after splitting a bucket.
    fn reinsert_record(
        &mut self,
        idx_record: IdxRecord,
        mut storage_mgr: &mut RefMut<StorageManager>,
    ) {
        let mut bucket_page = self.create_bucket_from_hash_value(idx_record.hash_val, storage_mgr);
        if bucket_page.insert_record(&idx_record).is_err() {
            if bucket_page.overflow.is_some() {
                let block = self
                    .blocks
                    .iter()
                    .find(|block| block.block_num == bucket_page.overflow.unwrap())
                    .unwrap();
                let frame = storage_mgr.pin(block.clone()).unwrap();
                let mut overflow_bucket = BucketPage::new(frame);
                overflow_bucket.insert_record(&idx_record);
            } else {
                let block = storage_mgr.extend_file(self.blocks[0].filename.as_str());
                let frame = storage_mgr.pin(block.clone()).unwrap();
                let mut new_bucket_page = BucketPage::new_from_empty(frame, bucket_page.depth);
                new_bucket_page.insert_record(&idx_record);
                bucket_page.set_overflow(block.block_num);
                self.blocks.push(block);
            }
        }
    }

    /// Reading all index records inside a bucket and its overflow, then creating 2 new buckets and
    /// reinserting each index record again after the new hash coding.
    fn split_bucket(
        &mut self,
        bucket_page: &BucketPage,
        idx_record: IdxRecord,
        mut storage_mgr: RefMut<StorageManager>,
    ) {
        if bucket_page.depth == self.global_depth {
            self.global_depth += 1;
        }
        let bucket_id = self.hash_val_to_bucket(idx_record.hash_val);
        let block_num = self.bucket_dir.remove_bucket(bucket_id).unwrap();
        let blk_idx = self
            .blocks
            .iter()
            .position(|block| block.block_num == block_num)
            .unwrap();
        self.blocks.remove(blk_idx);

        let filename = self.blocks[0].filename.as_str();
        let mut bucket_records = bucket_page.read_all_bucket_records();
        if bucket_page.overflow.is_some() {
            let block = self
                .blocks
                .iter()
                .find(|block| block.block_num == bucket_page.overflow.unwrap())
                .unwrap();
            let frame = storage_mgr.pin(block.clone()).unwrap();
            let mut overflow_bucket = BucketPage::new(frame);
            bucket_records.append(overflow_bucket.read_all_bucket_records().as_mut());
        }
        let mut block_one = storage_mgr.extend_file(filename);
        let bucket_one_id = (bucket_records[0].hash_val % bucket_page.depth as u32);
        self.bucket_dir
            .insert_bucket(bucket_one_id, block_one.block_num);
        let frame_one = storage_mgr.pin(block_one.clone()).unwrap();
        let bucket_split_one = BucketPage::new_from_empty(frame_one, bucket_page.depth + 1);
        let mut block_two = storage_mgr.extend_file(filename);
        let bucket_two_id = (bucket_records[0].hash_val % bucket_page.depth as u32)
            + 2_u32.pow(bucket_page.depth as u32);
        self.bucket_dir
            .insert_bucket(bucket_two_id, block_two.block_num);
        let frame_two = storage_mgr.pin(block_two.clone()).unwrap();
        let bucket_split_two = BucketPage::new_from_empty(frame_two, bucket_page.depth + 1);

        self.blocks.push(block_one);
        self.blocks.push(block_two);
        for record in bucket_records {
            self.reinsert_record(record, &mut storage_mgr);
        }
        self.reinsert_record(idx_record, &mut storage_mgr);
        self.flush_directory();
    }

    /// Get all the rids of the matched index records with the search key.
    pub fn get_rids(
        &self,
        search_key: String,
        mut storage_mgr: RefMut<StorageManager>,
    ) -> Vec<Rid> {
        let hash_val = self.hash_value(search_key.clone());
        let mut bucket_page = self.create_bucket(search_key, &mut storage_mgr);
        let mut rids = bucket_page.find_all(hash_val);
        if bucket_page.overflow.is_some() {
            let block = self
                .blocks
                .iter()
                .find(|block| block.block_num == bucket_page.overflow.unwrap())
                .unwrap();
            let frame = storage_mgr.pin(block.clone()).unwrap();
            let mut overflow_bucket = BucketPage::new(frame);
            rids.append(overflow_bucket.find_all(hash_val).as_mut());
        }
        rids
    }

    pub fn flush_directory(&mut self) {
        self.bucket_dir.flush();
    }
}

struct BucketPage {
    pub frame: FrameRef,
    depth: u8,
    num_records: u16,
    overflow: Option<u64>,
    vacuuming: bool,
}

impl BucketPage {
    pub fn new(frame: FrameRef) -> Self {
        let hash_frame = frame.clone();
        let mut frame_ref = hash_frame.borrow_mut();
        frame_ref.update_replace_stats();
        let depth = frame_ref.page.payload.as_slice()[0];
        let num_records = frame_ref.page.payload.as_slice().extract_u16(1);
        let overflow = frame_ref.page.payload.as_slice().extract_u64(3);
        let overflow = if overflow == 0 { None } else { Some(overflow) };
        Self {
            frame,
            depth,
            num_records,
            overflow,
            vacuuming: false,
        }
    }
    fn init(frame: &FrameRef, depth: u8) {
        let mut frame = frame.borrow_mut();
        let metadata = [depth, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        frame.update_replace_stats();
        frame.write(metadata.as_slice())
    }

    /// Creates an empty Hash Index Page and returns it
    pub fn new_from_empty(frame: FrameRef, depth: u8) -> Self {
        BucketPage::init(&frame, depth);
        BucketPage::new(frame)
    }

    /// Set the overflow bucket by writing the overflow bucket ID.
    pub fn set_overflow(&mut self, overflow: u64) {
        self.overflow = Some(overflow);
        self.frame
            .borrow_mut()
            .page
            .write_bytes(overflow.to_ne_bytes().as_slice(), 3);
    }

    /// Retrieving the Rids with hash value similar to the query's hash value.
    fn find_all(&self, hash_val: u32) -> Vec<Rid> {
        let frame = self.frame.borrow();
        let mut rids = Vec::new();
        let mut pos = 11_usize;
        for i in 0..self.num_records {
            let idx_record = IdxRecord::from_bytes(&frame.page.payload[pos..pos + IDX_RECORD_SIZE]);
            if idx_record.hash_val == hash_val && idx_record.deleted == 0 {
                rids.push(idx_record.rid);
            }
            pos += IDX_RECORD_SIZE;
        }
        rids
    }

    /// Writing the index record bytes inside the bucket if there is enough space, else returning error.
    fn insert_record(&mut self, record: &IdxRecord) -> Result<(), String> {
        let mut frame = self.frame.borrow_mut();
        let pos = 11_usize + self.num_records as usize * IDX_RECORD_SIZE;
        if frame.page.payload.len() - pos < IDX_RECORD_SIZE {
            return Err("Insufficient Space".to_string());
        }
        self.num_records += 1;
        frame
            .page
            .write_bytes(self.num_records.to_ne_bytes().as_slice(), 3);
        frame
            .page
            .write_bytes(record.to_bytes().as_slice(), pos as u64);
        Ok(())
    }

    /// Retrieve all the index records inside a bucket.
    fn read_all_bucket_records(&self) -> Vec<IdxRecord> {
        let frame = self.frame.borrow();
        let mut pos = 11_usize;
        let mut idx_records = Vec::new();
        for i in 0..self.num_records {
            let idx_record = IdxRecord::from_bytes(&frame.page.payload[pos..pos + IDX_RECORD_SIZE]);
            idx_records.push(idx_record);
            pos += IDX_RECORD_SIZE;
        }
        idx_records
    }
}
