use crate::storage::blockid::BlockId;
use crate::storage::page::Page;
use positioned_io2::{RandomAccessFile, ReadAt, Size, WriteAt};
use std::collections::hash_map::{Entry, HashMap};
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::linux::fs::MetadataExt;
use std::path::{Path, PathBuf};

/// BlockManager is an entity owned by a storage manager that is responsible for dealing with the
/// raw block bytes on the disk
pub struct BlockManager {
    db_dir: PathBuf,
    block_size: usize,
    open_files: HashMap<PathBuf, File>,
}
impl BlockManager {
    pub fn new(db_dir: &str, block_size: usize) -> Self {
        BlockManager {
            db_dir: PathBuf::from(db_dir),
            block_size,
            open_files: HashMap::new(),
        }
    }

    /// Returns raw bytes written in a file given a certain block id and the number of bytes needed
    pub fn read_raw(&mut self, blockid: &BlockId, byte_count: usize) -> Vec<u8> {
        let blk_size = self.block_size;
        let mut filepath = self.db_dir.join(blockid.filename.as_str());
        // filepath.push_str(&blockid.filename.as_str());
        let file = self.get_file(filepath.to_str().unwrap());
        let mut vec = vec![0_u8; byte_count];
        file.read_at(blockid.block_num * 4096, vec.as_mut());
        vec
    }

    /// Fills a certain page with a specific block content from the disk to be loaded into memory
    pub fn read(&mut self, blockid: &BlockId, page: &mut Page) {
        let blk_size = self.block_size;
        let mut filepath = self.db_dir.join(blockid.filename.as_str());
        // filepath.push_str(&blockid.filename.as_str());
        let file = self.get_file(filepath.to_str().unwrap());
        match file.read_at(blockid.block_num * blk_size as u64, &mut page.payload) {
            Ok(x) => {
                if x != blk_size {
                    dbg!(x, blockid);
                }
            }
            Err(e) => {
                dbg!(e, blockid);
            }
        }
    }

    /// Writes a certain page's content from memory into a specific block on the disk
    pub fn write(&mut self, blockid: &BlockId, page: &mut Page) {
        let blk_size = self.block_size;
        let mut filepath = self.db_dir.join(blockid.filename.as_str());
        // filepath.push_str();
        let file = self.get_file(filepath.to_str().unwrap());
        file.write_at(blockid.block_num * blk_size as u64, &page.payload)
            .unwrap();
        file.sync_all().unwrap()
    }

    /// Append to a full file an extra empty block (Initialized by zeros)
    ///
    /// Returns the Block Id of the appended block
    pub fn extend_file(&mut self, filename: &str) -> BlockId {
        let blk_size = self.block_size;
        let mut filepath = self.db_dir.join(filename);
        let file = self.get_file(filepath.to_str().unwrap());
        file.seek(SeekFrom::End(0)).unwrap();
        let size = vec![0 as u8; blk_size];
        file.write(size.as_slice()).unwrap();
        file.sync_all().unwrap();
        BlockId {
            block_num: (file.metadata().unwrap().len() / blk_size as u64) - 1,
            filename: filename.to_string(),
        }
    }

    /// Append to a full file extra empty N blocks where N is the number passed by the caller as
    /// a function parameter
    ///
    /// Returns a vector containing the Block Ids of the appended blocks
    pub fn extend_file_many(&mut self, filename: &str, count: u32) -> Vec<BlockId> {
        let blk_size = self.block_size;
        let mut filepath = self.db_dir.join(filename);
        // filepath.push_str(filename);
        let file = self.get_file(filepath.to_str().unwrap());
        file.seek(SeekFrom::End(0)).unwrap();
        let size = vec![0 as u8; blk_size * count as usize];
        let idx_first_new = (file.metadata().unwrap().len() / blk_size as u64);
        file.write(size.as_slice()).unwrap();
        file.sync_all().unwrap();
        (idx_first_new..(idx_first_new + count as u64))
            .map(|idx| BlockId::new(filename, idx))
            .collect()
    }

    /// Returns a specific file from the open files setting its RW modes to true
    ///
    /// If the file name is not in the open files, it creates a new file and returns it with
    /// RW modes set to true
    fn get_file(&mut self, filename: &str) -> &mut File {
        self.open_files
            .entry(PathBuf::from(filename))
            .or_insert_with(|| match Path::exists(filename.as_ref()) {
                false => {
                    File::create(filename).unwrap();
                    File::options()
                        .read(true)
                        .write(true)
                        .open(filename)
                        .unwrap()
                }
                true => File::options()
                    .read(true)
                    .write(true)
                    .open(filename)
                    .unwrap(),
            })
    }
}
