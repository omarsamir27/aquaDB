use super::blockid::BlockId;
use super::page::Page;
use crate::storage::blkmgr;
use crate::storage::blkmgr::BlockManager;
use crate::storage::frame::Frame;
use chrono::prelude::Utc;
use std::cell::RefCell;
use std::char::MAX;
use std::cmp::min;
use std::collections::HashSet;
use std::rc::Rc;
use std::thread::sleep;

pub type FrameRef = Rc<RefCell<Frame>>;

pub struct BufferManager {
    frame_pool: Vec<FrameRef>,
    max_slots: u32,
    available_slots: u32,
    page_size: usize, //timeout : Chrono Time
    block_set: HashSet<BlockId>,
}
impl BufferManager {
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
            block_set: HashSet::new(),
        }
    }

    #[inline(always)]
    pub fn get_frame(&mut self, idx: usize) -> Option<FrameRef> {
        let frame = self.frame_pool.get(idx).unwrap().clone();
        Some(frame)
    }

    pub fn flush_frame(&mut self, frame_idx: usize, blk_mgr: &mut BlockManager) {
        self.frame_pool[frame_idx]
            .try_borrow_mut()
            .unwrap()
            .flush(blk_mgr);
    }

    /// try to find if an unpinned page is still in memory and has not been replaced out
    /// if it still exists , pin it and return its contents,
    /// else load it into memory and pin it then return its contents
    pub fn try_pin(&mut self, blk: &BlockId, blkmgr: &mut BlockManager) -> Option<usize> {
        let mut idx = self.locate_existing_block(blk);
        if idx.is_none() {
            idx = self.find_victim_page();
            if idx.is_none() {
                return None;
            }
            else {
                let victim_frame = self.frame_pool.get_mut(idx.unwrap()).unwrap();
                let victim_frame = victim_frame.borrow();
                let blk = victim_frame.blockid.as_ref();
                if blk.is_some() {
                    self.block_set.remove(&blk.unwrap());
                }
            }
        }
        let idx = idx.unwrap();
        let mut frame = self.frame_pool.get_mut(idx).unwrap();
        let mut frame = frame.try_borrow_mut().unwrap();
        frame.load_block(blk, blkmgr);
        self.block_set.insert(blk.clone());
        if frame.is_free() {
            self.available_slots -= 1;
        }
        frame.num_pins += 1;
        // frame.timestamp = Some(Utc::now().timestamp_millis());
        debug_print::debug_println!("buffer position chosen : {}", idx);
        Some(idx)
    }

    //  pin a block to a frame
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

    pub fn timeout(&self, starttime: i64) -> bool {
        Utc::now().timestamp_millis() - starttime > 10000
    }

    // remove pin from frame
    pub fn unpin(&mut self, frame: FrameRef) {
        let mut frame = frame.borrow_mut();
        frame.num_pins -= 1;
        if frame.is_free() {
            self.available_slots += 1;
        }
    }

    // find a frame that is not pinned by any tx
    // pub fn find_unused_frame(&self) -> Option<usize> {
    //     let mut minimum_index = None;
    //     let mut minimum = Some(i64::MAX);
    //     for i in 0..self.frame_pool.len() {
    //         let frame = self.frame_pool[i].borrow_mut();
    //         if frame.is_free() && frame.timestamp < minimum {
    //             minimum = frame.timestamp;
    //             minimum_index = Some(i);
    //         }
    //     }
    //     log::debug!(
    //         "chosen frame index for replacement:{}",
    //         minimum_index.unwrap()
    //     );
    //     minimum_index
    // }

    pub fn find_clean_frame(&self) -> Option<usize> {
        self.frame_pool
            .iter()
            .position(|frame| frame.borrow().blockid.is_none())
    }

    pub fn find_victim_page(&self) -> Option<usize> {
        let clean_frame = self.find_clean_frame();
        if clean_frame.is_some() {
            clean_frame
        } else {
            self.lirs_victim()
        }
    }

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

    // find if a block exists in the frame pool and returns it
    pub fn locate_existing_block(&self, blk: &BlockId) -> Option<usize> {
        //     self.frame_pool
        //         .iter()
        //         .position(|&frame:FrameRef| frame.borrow_mut().blockid.as_ref() == Some(blk))
        // }
        for i in 0..self.frame_pool.len() {
            if self.frame_pool[i].borrow().blockid.as_ref() == Some(blk) {
                return Some(i);
            }
        }
        None
    }

    // pub fn lru_replacement(&self, blk: &BlockId) {
    //     // search for the smallest timestamp in frames
    // }
}
