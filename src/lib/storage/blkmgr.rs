use crate::storage::blockid::BlockId;
use crate::storage::page::Page;
use positioned_io2::{RandomAccessFile, ReadAt, Size, WriteAt};
use std::collections::hash_map::{Entry, HashMap};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct BlockManager {
    db_dir: PathBuf,
    block_size: usize,
    open_files: HashMap<String, File>,
}
impl BlockManager {
    pub fn new(db_dir: &str, block_size: usize) -> Self {
        BlockManager {
            db_dir: PathBuf::from(db_dir),
            block_size,
            open_files: HashMap::new(),
        }
    }

    pub fn read_raw(&mut self, blockid: &BlockId, byte_count: usize) -> Vec<u8> {
        let blk_size = self.block_size;
        let mut filepath = String::from(self.db_dir.to_str().unwrap());
        filepath.push_str(&blockid.filename.as_str());
        let file = self.get_file(filepath.as_str());
        let mut vec = vec![0_u8; byte_count];
        file.read_exact(vec.as_mut_slice());
        vec
    }

    pub fn read(&mut self, blockid: &BlockId, page: &mut Page) {
        let blk_size = self.block_size;
        let mut filepath = String::from(self.db_dir.to_str().unwrap());
        filepath.push_str(&blockid.filename.as_str());
        let file = self.get_file(filepath.as_str());
        file.read_at(blockid.block_num * blk_size as u64, &mut page.payload)
            .unwrap();
    }

    pub fn write(&mut self, blockid: &BlockId, page: &mut Page) {
        let blk_size = self.block_size;
        let mut filepath = String::from(self.db_dir.to_str().unwrap());
        filepath.push_str(&blockid.filename.as_str());
        let file = self.get_file(filepath.as_str());
        file.write_at(blockid.block_num * blk_size as u64, &page.payload)
            .unwrap();
        file.sync_all().unwrap()
    }

    pub fn extend_file(&mut self, filename: &str) -> BlockId {
        let blk_size = self.block_size;
        let mut filepath = String::from(self.db_dir.to_str().unwrap());
        filepath.push_str(filename);
        let file = self.get_file(filepath.as_str());
        file.seek(SeekFrom::End(0)).unwrap();
        let size = vec![0 as u8; blk_size];
        file.write(size.as_slice()).unwrap();
        file.sync_all().unwrap();
        BlockId {
            block_num: (file.metadata().unwrap().len() / blk_size as u64) - 1,
            filename: filename.to_string(),
        }
    }

    pub fn extend_file_many(&mut self, filename: &str,count:u32) -> Vec<BlockId> {
        let blk_size = self.block_size;
        let mut filepath = String::from(self.db_dir.to_str().unwrap());
        filepath.push_str(filename);
        let file = self.get_file(filepath.as_str());
        file.seek(SeekFrom::End(0)).unwrap();
        let size = vec![0 as u8; blk_size*count as usize];
        file.write(size.as_slice()).unwrap();
        file.sync_all().unwrap();
        let idx_first_new =  (file.metadata().unwrap().len() / blk_size as u64) - 1 ;
        (idx_first_new..(idx_first_new + count as u64)).map(|idx|
            BlockId::new(filepath.as_str(),idx)).collect()
    }

    fn get_file(&mut self, filename: &str) -> &mut File {
        self.open_files
            .entry(filename.to_string())
            .or_insert_with(|| {
                File::create(filename).unwrap();
                File::options()
                    .read(true)
                    .write(true)
                    .open(filename)
                    .unwrap()
            })
    }
}
