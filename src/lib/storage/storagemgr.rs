use crate::common::fileops::file_size;
use crate::storage::blockid::BlockId;
use crate::storage::buffermgr::FrameRef;
use crate::storage::frame::Frame;
use crate::storage::heap::HeapPage;
use crate::storage::{blkmgr::BlockManager, buffermgr::BufferManager, logmgr::LogManager};
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::path::PathBuf;

/// Represents database configuration
///
/// This is hardcoded but can be loaded from a file in the future or a mix of both
struct DatabaseInfo {
    db_dir: PathBuf,
    block_size: usize,
    // add log mechanism
}
impl DatabaseInfo {
    fn new(db_dir: &str, block_size: usize) -> Self {
        DatabaseInfo {
            db_dir: PathBuf::from(db_dir),
            block_size,
        }
    }
}
/// StorageManager is an abstraction of the storage engine of the database,
/// it abstracts disk-page access from higher level modules and deals with it internally
/// to maximize performance and ensure correctness
///
/// It is consists of:
///
/// Buffer Manager : responsible for bookkeeping of in-memory pages
///
/// Block Manager : reads and writes disk blocks to/from in-memory pages
///
/// Log Manager : responsible for log creation and maintenance for Database recovery purposes
///
/// Database Info : Information related to the Storage Engine
pub struct StorageManager {
    database_info: DatabaseInfo,
    buffer_manager: BufferManager,
    pub block_manager: BlockManager,
    log_manager: LogManager,
}

impl StorageManager {
    /// Creates a StorageManager instance, this should be called by the DatabaseManager and typically
    /// only one will exist per database
    pub fn new(db_dir: &str, block_size: usize, max_buffer_slots: u32) -> Self {
        Self {
            database_info: DatabaseInfo::new(db_dir, block_size),
            buffer_manager: BufferManager::new(block_size, max_buffer_slots),
            block_manager: BlockManager::new(db_dir, block_size),
            log_manager: LogManager::new(),
        }
    }
    /// Tries to pin disk block to a frame and returns a reference to that frame
    ///
    /// Note that throughout the database , Frame references are wrapped in RcRefcell and will panic
    /// if 2 or more users try to mutably borrow it concurrently, it is the responsibility of the
    /// **Requester** to avoid that
    pub fn pin(&mut self, blk: BlockId) -> Option<FrameRef> {
        self.buffer_manager.pin(blk, &mut self.block_manager)
    }

    /// Unpins a frame
    pub fn unpin(&mut self, frame: FrameRef) {
        let frm = frame.borrow().blockid.as_ref().unwrap().clone();
        // dbg!(&frm);
        if frm.block_num == 0 && frm.filename.ends_with("s_id_idx_file") {
            return;
        }
        self.buffer_manager.unpin(frame);
    }

    /// Add 1 empty unformatted block to a file
    pub fn extend_file(&mut self, filename: &str) -> BlockId {
        self.block_manager.extend_file(filename)
    }

    /// Add **count** unformatted blocks to a file
    pub fn extend_file_many(&mut self, filename: &str, count: u32) -> Vec<BlockId> {
        self.block_manager.extend_file_many(filename, count)
    }

    /// Reads n bytes from a Block
    pub fn read_raw(&mut self, blockid: &BlockId, byte_count: usize) -> Vec<u8> {
        self.block_manager.read_raw(blockid, byte_count)
    }


    pub fn flush_all(&mut self) {
        self.buffer_manager.flush_all(&mut self.block_manager);
    }

    pub fn force_flush_all(&mut self){
        self.buffer_manager.force_flush_all(&mut self.block_manager);

    }
    
    /// Flushes a memory frame to the disk block it is currently pinned to , resetting its stats
    pub fn flush_frame(&mut self, frame: FrameRef) {
        let mut frm = frame.try_borrow_mut().unwrap();
        frm.flush(&mut self.block_manager)
    }

    fn force_flush(&mut self, frame: FrameRef) {
        let mut frm = frame.try_borrow_mut().unwrap();
        frm.dirty = true;
        frm.flush(&mut self.block_manager)
    }

    /// Returns the disk block size used for the database
    pub fn blk_size(&self) -> usize {
        self.database_info.block_size
    }

    pub fn empty_heap_pages(&mut self, filename: &str, count: u32) -> Vec<BlockId> {
        let blks = self.extend_file_many(filename, count);
        for blk in &blks {
            let frame = self.pin(blk.clone());
            HeapPage::init_heap(frame.as_ref().unwrap(), 0);
            self.force_flush(frame.unwrap());
        }
        blks
    }

    pub fn file_blks(&self, filepath: PathBuf) -> Vec<BlockId> {
        if !filepath.exists() {
            return vec![];
        }
        let size = file_size(&filepath);
        let num_blks = size / self.blk_size() as u64;
        let filename = filepath.to_str().unwrap();
        (0..num_blks).map(|n| BlockId::new(filename, n)).collect()
    }

    pub fn show_available_slots(&self) -> u32 {
        self.buffer_manager.print_pinned_blocks();
        self.buffer_manager.get_available_slots()
    }
}
