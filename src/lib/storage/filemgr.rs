use std::collections::hash_map::{HashMap,Entry};
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use positioned_io2::{RandomAccessFile, ReadAt, Size, WriteAt};
use crate::storage::blockid::BlockId;
use crate::storage::page::Page;

struct FileManager{
    db_dir : PathBuf,
    block_size : u64,
    open_files : HashMap<String, RandomAccessFile>

}
impl FileManager{
    fn new(db_dir:&str,block_size:u64)-> Self{
        FileManager{db_dir:PathBuf::from(db_dir),block_size,open_files:HashMap::new()}
    }
    fn read(&mut self,blockid:&BlockId,page:&mut Page){
        let file = self.get_file(blockid.filename.as_str());
        file.read_at(blockid.block_num * self.block_size  , &mut page.payload).unwrap();
    }

    fn write(&mut self, blockid:&BlockId, page:&mut Page) {
        let file = self.get_file(blockid.filename.as_str());
        file.write_at(blockid.block_num * self.block_size , &page.payload).unwrap();
        let raw_file = file.try_into_inner().unwrap();
        raw_file.sync_all().unwrap();
    }

    fn extend_file(&mut self,filename:&str) {
        let file = self.get_file(blockid.filename.as_str());
        let mut raw_file  = file.try_into_inner().unwrap();
        raw_file.seek(SeekFrom::End(0)).unwrap();
        let size = vec![0 as u8; self.block_size as usize];
        raw_file.write(size.as_slice()).unwrap();
        raw_file.sync_all().unwrap();
    }

    fn get_file(&mut self, filename:&str) -> &mut RandomAccessFile {
        self.open_files.entry(filename.to_string()).or_insert_with(|| {
                    File::create(filename).unwrap() ;
                    RandomAccessFile::open(filename).unwrap()
        })
    }
    }