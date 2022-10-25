use nix::sys::timerfd::ClockId;
use super::page::Page;
use super::blockid::BlockId;

struct Frame{
    page : Page,
    num_pins : u32,
    blockid : Option<BlockId>,
    transaction_num : Option<u32> ,
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
    //  pin a block to a frame
    pub fn pin(){}

    // remove pin from frame
    pub fn unpin(){}

    // find a frame that is not pinned by any tx
    pub fn find_unused_frame(){}

    // find if a block exists in the frame pool and returns it
    pub fn locate_existing_block(){}

    /// try to find if an unpinned page is still in memory and has not been replaced out
    /// if it still exists , pin it and return its contents,
    /// else load it into memory and pin it then return its contents
    pub fn try_pin(){}

}