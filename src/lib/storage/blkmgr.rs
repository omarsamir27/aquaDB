use crate::storage::blockid::BlockId;
use crate::storage::page::Page;
use positioned_io2::{RandomAccessFile, ReadAt, Size, WriteAt};
use std::collections::hash_map::{Entry, HashMap};
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
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
    pub fn read(&mut self, blockid: &BlockId, page: &mut Page) {
        let blk_size = self.block_size ;
        let file = self.get_file(blockid.filename.as_str());
        file.read_at(blockid.block_num * blk_size as u64 , &mut page.payload)
            .unwrap();
    }

    pub fn write(&mut self, blockid: &BlockId, page: &mut Page) {
        let blk_size = self.block_size ;
        let file = self.get_file(blockid.filename.as_str());
        file.write_at(blockid.block_num * blk_size as u64, &page.payload)
            .unwrap();
        file.sync_all().unwrap()
    }

    pub fn extend_file(&mut self, filename: &str) {
        let blk_size = self.block_size ;
        let file = self.get_file(filename);
        file.seek(SeekFrom::End(0)).unwrap();
        let size = vec![0 as u8; blk_size];
        file.write(size.as_slice()).unwrap();
        file.sync_all().unwrap();
    }

    fn get_file(&mut self, filename: &str) -> &mut File {
        self.open_files
            .entry(filename.to_string())
            .or_insert_with(|| {
                File::create(filename).unwrap();
                File::open(filename).unwrap()

            })
    }
}
