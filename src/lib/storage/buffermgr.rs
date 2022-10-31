use std::char::MAX;
use std::cmp::min;
use chrono::prelude::Utc;
use std::thread::sleep;
use super::page::Page;
use super::blockid::BlockId;

struct Frame{
    page : Page,
    num_pins : u32,
    blockid : Option<BlockId>,
    transaction_num : Option<u32> ,
    timestamp: Option<i64>,
    //garbage_frame : bool
    // log sequence number
}

impl Frame {
    fn new(page_size:usize) -> Self{
        Frame{
            page : Page::new(page_size),
            num_pins : 0 ,
            blockid : None,
            transaction_num : None ,
            timestamp: None,
        }
    }
}

// impl Clone for Frame {
//     fn clone(&self) -> Self {
//         Frame{
//             page : Page::new(page_size),
//             num_pins : self.num_pins ,
//             blockid : self.blockid,
//             transaction_num : self.transaction_num ,
//         }
//     }
// }

pub struct BufferManager{
    frame_pool : Vec<Frame>,
    max_slots : u32,
    available_slots : u32,
    page_size : usize,
    //timeout : Chrono Time
}
impl BufferManager{
    pub fn new(page_size:usize,max_slots:u32)-> Self{
        let mut frame_pool  = Vec::with_capacity(max_slots as usize);
        for _ in 0..max_slots{
            frame_pool.push(Frame::new(page_size));
        }
        BufferManager{
            frame_pool ,
            max_slots,
            available_slots : max_slots,
            page_size
        }
    }

    /// try to find if an unpinned page is still in memory and has not been replaced out
    /// if it still exists , pin it and return its contents,
    /// else load it into memory and pin it then return its contents
    pub fn try_pin(&mut self, blk : &BlockId) -> Option<usize>{
        let mut idx = self.locate_existing_block(blk);
        if idx.is_none(){
            idx = self.find_unused_frame();
            if idx.is_none(){
                return None
            }
        }
        let idx = idx.unwrap();
        let mut frame = self.frame_pool.get_mut(idx).unwrap();
        if frame.num_pins == 0{
            self.available_slots -= 1;
        }
        frame.num_pins += 1;
        frame.timestamp = Some(Utc::now().timestamp_millis());
        Some(idx)
    }

    //  pin a block to a frame
    pub fn pin(&mut self, blk: BlockId) -> Option<&mut Frame>{
        let time_stamp = Utc::now().timestamp_millis();
        let mut idx = self.try_pin(&blk);
        while idx.is_none() && !self.timeout(time_stamp){
            //sleep(1);
            idx = self.try_pin(&blk);
        }
        if idx.is_none(){
            return None;
        }
        self.frame_pool.get_mut(idx.unwrap())
    }

    pub fn timeout(&self, starttime: i64) -> bool{
        Utc::now().timestamp_millis()-starttime > 10000
    }

    // remove pin from frame
    pub fn unpin(&mut self, mut frame : Frame){
        frame.num_pins -= 1;
        if frame.num_pins == 0{
            self.available_slots += 1;
        }
    }

    // find a frame that is not pinned by any tx
    pub fn find_unused_frame(&self) -> Option<usize>{
        //self.frame_pool.iter().position(|frame| frame.num_pins == 0)
        let mut minimum_index = None;
        let mut minimum = Some(i64::MAX);
        for i in 0..self.frame_pool.len(){
            if self.frame_pool[i].num_pins == 0 && self.frame_pool[i].timestamp < minimum {
                minimum_index = Some(i);
            }
        }
        minimum_index
    }

    // find if a block exists in the frame pool and returns it
    pub fn locate_existing_block(&self, blk : &BlockId) -> Option<usize>{
        self.frame_pool.iter().position(|frame| frame.blockid.as_ref() == Some(blk))
    }

    pub fn lru_replacement(&self, blk: &BlockId){
        // search for the smallest timestamp in frames

    }
}