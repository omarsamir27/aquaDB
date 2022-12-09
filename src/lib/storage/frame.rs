use std::cmp::max;
use chrono::Utc;
use crate::storage::blkmgr::BlockManager;
use crate::storage::blockid::BlockId;
use crate::storage::page::Page;

pub struct Frame {
    pub page: Page,
    pub num_pins: u32,
    pub blockid: Option<BlockId>,
    pub dirty: bool,
    pub transaction_num: Option<u32>,
    pub last_access_time:i64,
    pub second_last_access_time : i64,
    pub reuse_distance : i64,
    //garbage_frame : bool
    // log sequence number
}

impl Frame {
    pub fn new(page_size: usize) -> Self {
        Frame {
            page: Page::new(page_size),
            num_pins: 0,
            blockid: None,
            dirty: false,
            transaction_num: None,
            last_access_time: -1,
            second_last_access_time : -1 ,
            reuse_distance : -1
        }
    }
    #[inline(always)]
    pub fn is_free(&self) -> bool {
        self.num_pins == 0
    }

    #[inline(always)]
    pub fn update_replace_stats(&mut self){
        // self.timestamp = Some(Utc::now().timestamp_millis());
        if self.last_access_time == -1{
            self.reuse_distance = i64::MAX;
        }
        else{
            self.reuse_distance = self.last_access_time - self.second_last_access_time ;
        }
        self.second_last_access_time = self.last_access_time;
        self.last_access_time = Utc::now().timestamp_millis();
    }

    #[inline(always)]
    pub fn reset_time_stats(&mut self){
        self.second_last_access_time = -1;
        self.last_access_time = -1;
        self.reuse_distance = -1;
    }

    #[inline(always)]
    pub fn lirs_weight(&self,utc_now:i64) -> i64{
        max(self.reuse_distance,utc_now - self.last_access_time)
    }

    pub fn load_block(&mut self, blk: &BlockId, blkmgr: &mut BlockManager) {
        if self.dirty {
            self.flush(blkmgr);
        }
        blkmgr.read(&blk, &mut self.page);
        self.blockid = Some(blk.clone());
        self.num_pins = 0;
        self.reset_time_stats();
    }

    pub fn flush(&mut self, blkmgr: &mut BlockManager) {
        // log manager flush here
        let blk = self.blockid.as_ref().unwrap();
        blkmgr.write(blk, &mut self.page);
        self.transaction_num = None;
        self.dirty = false;
    }

    pub fn write(&mut self, data: &[u8]) {
        self.write_at(data, 0);
    }

    pub fn write_at(&mut self, data: &[u8], offset: u64) {
        // check if data.len > page.len
        self.page.write_bytes(data, offset);
        self.dirty = true;
    }
}
