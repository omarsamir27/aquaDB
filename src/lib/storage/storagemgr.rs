use std::path::PathBuf;
use crate::storage::{logmgr::LogManager, buffermgr::BufferManager, blkmgr::BlockManager};
use crate::storage::blockid::BlockId;
use crate::storage::buffermgr::Frame;

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

struct Database{
    database_info : DatabaseInfo,
    buffer_manager : BufferManager,
    block_manager : BlockManager,
    log_manager : LogManager
}

impl Database {
    fn new(db_dir:&str,block_size:usize) -> Self{
        Database{
            database_info : DatabaseInfo::new(db_dir,block_size),
            buffer_manager : BufferManager::new(block_size,MAX_BUFFER_SLOTS),
            block_manager : BlockManager::new(db_dir,block_size) ,
            log_manager : LogManager::new()
        }
    }

    fn pin(&mut self,blk:BlockId){
        self.buffer_manager.pin(blk, &mut self.block_manager);
        todo!()
    }

    fn unpin(&mut self, frame:&mut Frame){
        self.buffer_manager.unpin(frame);
        todo!()
    }
}