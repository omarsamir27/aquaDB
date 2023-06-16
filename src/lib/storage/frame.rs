use crate::storage::blkmgr::BlockManager;
use crate::storage::blockid::BlockId;
use crate::storage::page::Page;
use chrono::Utc;
use std::cmp::max;

/// A container for a page stored in memory, it contains the page alongside some metadata.
///
/// Metadata is used in: indicating whether the frame can be replaced or not, the block that is written in this frame,
/// whether this frame has been modified or not and data used by the page replacement algorithm
#[derive(Debug)]
pub struct Frame {
    pub page: Page,
    pub num_pins: u32,
    pub blockid: Option<BlockId>,
    pub dirty: bool,
    pub transaction_num: Option<u32>,
    pub last_access_time: i64,
    pub second_last_access_time: i64,
    pub reuse_distance: i64,
    //garbage_frame : bool
    // log sequence number
}

impl Frame {
    /// Creates an instance of the frame and instantiate the metadata with the default values.
    pub fn new(page_size: usize) -> Self {
        Frame {
            page: Page::new(page_size),
            num_pins: 0,
            blockid: None,
            dirty: false,
            transaction_num: None,
            last_access_time: -1,
            second_last_access_time: -1,
            reuse_distance: -1,
        }
    }

    /// Returns whether the frame can be replaced or not based on how many transactions are currently pinning this frame.
    #[inline(always)]
    pub fn is_free(&self) -> bool {
        self.num_pins == 0
    }

    /// Updates the stats used by the page replacement policy(LIRS)
    #[inline(always)]
    pub fn update_replace_stats(&mut self) {
        // self.timestamp = Some(Utc::now().timestamp_millis());
        if self.last_access_time == -1 {
            self.reuse_distance = i64::MAX;
        } else {
            self.reuse_distance = self.last_access_time - self.second_last_access_time;
        }
        self.second_last_access_time = self.last_access_time;
        self.last_access_time = Utc::now().timestamp_millis();
    }

    /// Sets the default values of the stats used by the page replacement policy.
    /// This function is used when loading a new block into memory.
    #[inline(always)]
    pub fn reset_time_stats(&mut self) {
        self.second_last_access_time = -1;
        self.last_access_time = -1;
        self.reuse_distance = -1;
    }

    /// Locality quantification for the page replacement algorithm.
    #[inline(always)]
    pub fn lirs_weight(&self, utc_now: i64) -> i64 {
        max(self.reuse_distance, utc_now - self.last_access_time)
    }

    /// Loads a new disk block into the memory frame.
    /// Resets the metadata in the frame after reading the block.
    pub fn load_block(&mut self, blk: &BlockId, blkmgr: &mut BlockManager) {
        if self.dirty {
            self.flush(blkmgr);
        }
        blkmgr.read(&blk, &mut self.page);
        self.blockid = Some(blk.clone());
        self.num_pins = 0;
        self.reset_time_stats();
    }

    /// Writes the page contained in the frame to the disk and resets the necessary stats in the frame.
    pub fn flush(&mut self, blkmgr: &mut BlockManager) {
        // log manager flush here
        if self.dirty {
            let blk = self.blockid.as_ref().unwrap();
            blkmgr.write(blk, &mut self.page);
            self.transaction_num = None;
            self.dirty = false;
        }
    }

    pub fn force_flush(&mut self, blkmgr: &mut BlockManager) {
        self.dirty = true;
        self.flush(blkmgr);
    }

    /// Writes data(as bytes) to the page contained in the frame.
    pub fn write(&mut self, data: &[u8]) {
        self.write_at(data, 0);
    }

    /// Writes data(as bytes) to the page contained in the frame at a specific offset.
    pub fn write_at(&mut self, data: &[u8], offset: u64) {
        // check if data.len > page.len
        self.page.write_bytes(data, offset);
        self.dirty = true;
    }
}
