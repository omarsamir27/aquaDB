use super::blockid::BlockId;
use super::page::Page;
use chrono::prelude::Utc;
use std::char::MAX;
use std::cmp::min;
use std::thread::sleep;
use crate::storage::blkmgr;
use crate::storage::blkmgr::BlockManager;
use crate::storage::frame::Frame;


pub struct BufferManager<>  {
    frame_pool: Vec<Frame>,
    max_slots: u32,
    available_slots: u32,
    page_size: usize
    //timeout : Chrono Time
}
impl BufferManager {
    pub fn new(page_size: usize, max_slots: u32) -> Self {
        let mut frame_pool = Vec::with_capacity(max_slots as usize);
        for _ in 0..max_slots {
            frame_pool.push(Frame::new(page_size));
        }
        BufferManager {
            frame_pool,
            max_slots,
            available_slots: max_slots,
            page_size
        }
    }

    #[inline(always)]
    pub fn get_frame(&mut self,idx:usize)-> Option<&mut Frame>{
        self.frame_pool.get_mut(idx)
    }

    pub fn flush_frame(&mut self,frame_idx:usize,blk_mgr:&mut BlockManager){
        self.frame_pool[frame_idx].flush(blk_mgr);
    }

    /// try to find if an unpinned page is still in memory and has not been replaced out
    /// if it still exists , pin it and return its contents,
    /// else load it into memory and pin it then return its contents
    pub fn try_pin(&mut self, blk: &BlockId, blkmgr: &mut BlockManager) -> Option<usize> {
        let mut idx = self.locate_existing_block(blk);
        if idx.is_none() {
            idx = self.find_unused_frame();
            if idx.is_none() {
                return None;
            }
        }
        let idx = idx.unwrap();
        let mut frame = self.frame_pool.get_mut(idx).unwrap();
        frame.load_block(blk,blkmgr);
        if frame.is_free() {
            self.available_slots -= 1;
        }
        frame.num_pins += 1;
        frame.timestamp = Some(Utc::now().timestamp_millis());
        Some(idx)
    }

    //  pin a block to a frame
    pub fn pin(&mut self, blk: BlockId, blkmgr:&mut BlockManager) -> Option<usize> {
        let time_stamp = Utc::now().timestamp_millis();
        let mut idx = self.try_pin(&blk,blkmgr);
        while idx.is_none() && !self.timeout(time_stamp) {
            //sleep(1);
            idx = self.try_pin(&blk,blkmgr);
        }
       idx
    }

    pub fn timeout(&self, starttime: i64) -> bool {
        Utc::now().timestamp_millis() - starttime > 10000
    }

    // remove pin from frame
    pub fn unpin(&mut self, frame_idx:usize) {
        let frame = self.get_frame(frame_idx).unwrap();
        frame.num_pins -= 1;
        if frame.is_free() {
            self.available_slots += 1;
        }
    }

    // find a frame that is not pinned by any tx
    pub fn find_unused_frame(&self) -> Option<usize> {
        let mut minimum_index = None;
        let mut minimum = Some(i64::MAX);
        for i in 0..self.frame_pool.len() {
            if self.frame_pool[i].is_free() && self.frame_pool[i].timestamp < minimum {
                minimum = self.frame_pool[i].timestamp ;
                minimum_index = Some(i);
            }
        }
        log::debug!("chosen frame index for replacement:{}",minimum_index.unwrap());
        minimum_index
    }

    // find if a block exists in the frame pool and returns it
    pub fn locate_existing_block(&self, blk: &BlockId) -> Option<usize> {
        self.frame_pool
            .iter()
            .position(|frame| frame.blockid.as_ref() == Some(blk))
    }

    // pub fn lru_replacement(&self, blk: &BlockId) {
    //     // search for the smallest timestamp in frames
    // }
}
