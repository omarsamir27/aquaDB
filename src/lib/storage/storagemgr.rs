use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::path::PathBuf;
use crate::storage::{logmgr::LogManager, buffermgr::BufferManager, blkmgr::BlockManager};
use crate::storage::blockid::BlockId;
use crate::storage::frame::Frame;
use crate::storage::buffermgr::{FrameRef};

static MAX_BUFFER_SLOTS : u32 = 100 ;

struct DatabaseInfo{
    db_dir: PathBuf,
    block_size: usize,
    // add log mechanism
}
impl DatabaseInfo{
    fn new(db_dir:&str,block_size:usize) -> Self{
        DatabaseInfo{db_dir:PathBuf::from(db_dir),block_size}
    }
}

pub struct StorageManager{
    database_info : DatabaseInfo,
    buffer_manager : BufferManager,
    pub block_manager : BlockManager,
    log_manager : LogManager
}

impl StorageManager {
    pub fn new(db_dir:&str,block_size:usize) -> Self{
        Self{
            database_info : DatabaseInfo::new(db_dir,block_size),
            buffer_manager : BufferManager::new(block_size,MAX_BUFFER_SLOTS),
            block_manager : BlockManager::new(db_dir,block_size) ,
            log_manager : LogManager::new()
        }
    }

    pub fn pin(&mut self, blk:BlockId) -> Option<FrameRef> {
        self.buffer_manager.pin(blk, &mut self.block_manager)
    }

    pub fn unpin(&mut self, frame:FrameRef){
        self.buffer_manager.unpin(frame);
    }

    pub fn extend_file(&mut self,filename:&str){
        self.block_manager.extend_file(filename);
    }


    // fn get_frame(&mut self,idx:usize) -> Option<&mut Frame>{
    //     self.buffer_manager.get_frame(idx)
    // }

    pub fn flush_frame(&mut self,  frame:FrameRef){
        let mut frm = frame.try_borrow_mut().unwrap();
        frm.flush(&mut self.block_manager)
    }

    // pub fn write_frame(&mut self,frame_idx:usize,data:&[u8]){
    //     let frame = self.get_frame(frame_idx).unwrap();
    //     frame.write(data);
    // }
}