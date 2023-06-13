use super::blockid::BlockId;
use super::page::Page;
use crate::storage::blkmgr;
use crate::storage::blkmgr::BlockManager;
use crate::storage::frame::Frame;
use chrono::prelude::Utc;
use std::cell::RefCell;
use std::char::MAX;
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::process::id;
use std::rc::Rc;
use std::thread::sleep;

pub type FrameRef = Rc<RefCell<Frame>>;

/// Page replacement policy: LIRS
/// BufferManager is an entity owned by the database that acts as cache for the pages most used by the database
/// instead of reading directly from the disk.
pub struct BufferManager {
    frame_pool: Vec<FrameRef>,
    max_slots: u32,
    available_slots: u32,
    page_size: usize, //timeout : Chrono Time
    block_map: HashMap<BlockId, usize>,
}
impl BufferManager {
    /// Create an instance of the buffer manager and determines its maximum size (maximum number of pages it can hold)
    /// It also determines the page size that the buffer manager will deal with
    pub fn new(page_size: usize, max_slots: u32) -> Self {
        let mut frame_pool = Vec::with_capacity(max_slots as usize);
        for _ in 0..max_slots {
            frame_pool.push(FrameRef::new(RefCell::new(Frame::new(page_size))));
        }
        BufferManager {
            frame_pool,
            max_slots,
            available_slots: max_slots,
            page_size,
            block_map: HashMap::new(),
        }
    }

    /// Returns a reference to the frame corresponding to the given frame index
    #[inline(always)]
    pub fn get_frame(&mut self, idx: usize) -> Option<FrameRef> {
        let frame = self.frame_pool.get(idx).unwrap().clone();
        Some(frame)
    }

    /// Uses the frame API to write the frame to the disk.
    /// This function writes the block corresponding to the frame we want to flush only
    pub fn flush_frame(&mut self, frame_idx: usize, blk_mgr: &mut BlockManager) {
        self.frame_pool[frame_idx]
            .try_borrow_mut()
            .unwrap()
            .flush(blk_mgr);
    }

    pub fn flush_all(&mut self, blk_mgr: &mut BlockManager) {
        for frame in &self.frame_pool {
            frame.borrow_mut().flush(blk_mgr);
        }
    }
    pub fn force_flush_all(&mut self, blk_mgr: &mut BlockManager) {
        for frame in &self.frame_pool {
            frame.borrow_mut().force_flush(blk_mgr)
        }
    }

    /// Try to find if an unpinned page is still in memory and has not been replaced out
    /// if it still exists, pin it and return index to its frame,
    /// else load it into memory and pin it then return index to the frame it got written to.
    pub fn try_pin(&mut self, blk: &BlockId, blkmgr: &mut BlockManager) -> Option<usize> {
        let mut idx = self.locate_existing_block(blk);
        if idx.is_none() {
            idx = self.find_victim_page();
            if idx.is_none() {
                return None;
            }
            let idx = idx.unwrap();
            let mut frame = self.frame_pool[idx].borrow_mut();
            if let Some(block) = frame.blockid.as_ref() {
                self.block_map.remove(block);
            }
            frame.load_block(&blk, blkmgr);
            self.block_map.insert(blk.to_owned(), idx);
            if frame.is_free() {
                self.available_slots -= 1;
            }
            frame.num_pins += 1;
            Some(idx)
        } else {
            let idx = idx.unwrap();
            let mut frame = self.frame_pool[idx].borrow_mut();
            if frame.is_free() {
                self.available_slots -= 1;
            }
            frame.num_pins += 1;
            Some(idx)
        }
    }

    ///  pin a block to a frame and return a reference to this frame
    pub fn pin(&mut self, blk: BlockId, blkmgr: &mut BlockManager) -> Option<FrameRef> {
        let time_stamp = Utc::now().timestamp_millis();
        let mut idx = self.try_pin(&blk, blkmgr);
        while idx.is_none() && !self.timeout(time_stamp) {
            //sleep(1);
            idx = self.try_pin(&blk, blkmgr);
        }
        match idx {
            None => None,
            Some(idx) => Some(self.frame_pool.get(idx).unwrap().clone()),
        }
    }

    /// Timelimit for the buffer to keep looking for an empty frame
    pub fn timeout(&self, starttime: i64) -> bool {
        Utc::now().timestamp_millis() - starttime > 10000
    }

    /// Unpins a frame in the buffer. This doesn't remove the frame from memory, It just becomes unpinned
    pub fn unpin(&mut self, frame: FrameRef) {
        let mut frame = frame.borrow_mut();
        if frame.num_pins as i32 - 1 < 0 {
            frame.num_pins = 0;
        }
        else {
            frame.num_pins -= 1;
        }
        if frame.is_free() {
            self.available_slots += 1;
        }
    }

    /// Returns an index to a frame that has no block assigned to it.
    pub fn find_clean_frame(&self) -> Option<usize> {
        self.frame_pool
            .iter()
            .position(|frame| frame.borrow().blockid.is_none())
    }

    /// Returns the index to the frame used for replacement.
    /// if there are empty frames in the buffer manager, they will be used.
    /// else, the page replacement policy gets us the index
    pub fn find_victim_page(&self) -> Option<usize> {
        let clean_frame = self.find_clean_frame();
        if clean_frame.is_some() {
            clean_frame
        } else {
            self.lirs_victim()
        }
    }

    /// Returns an index to the page to be replaced using LIRS basis.
    pub fn lirs_victim(&self) -> Option<usize> {
        let now = Utc::now().timestamp_millis();
        let victim = self
            .frame_pool
            .iter()
            .filter(|&frame| frame.borrow().is_free())
            .enumerate()
            .max_by_key(|&(x, y)| y.borrow().lirs_weight(now));
        match victim {
            None => None,
            Some((idx, _)) => Some(idx),
        }
    }

    /// Find if a block exists in the frame pool and returns its index
    pub fn locate_existing_block(&self, blk: &BlockId) -> Option<usize> {
        self.block_map.get(blk).copied()
    }
}
