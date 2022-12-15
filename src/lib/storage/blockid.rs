use std::cmp::Ordering;
use std::fmt::{format, Display, Formatter};
use std::hash::Hash;

/// A Unique Identifier for the block by the file name containing the block and the block number
/// inside that file
#[derive(Clone, Debug, Eq, Hash)]
pub struct BlockId {
    pub filename: String,
    pub block_num: u64,
}

impl BlockId {
    pub fn new(filename: &str, block_num: u64) -> Self {
        BlockId {
            filename: filename.to_string(),
            block_num,
        }
    }
}

/// Checking whether two blocks are the same block returning true if they are
impl PartialEq for BlockId {
    fn eq(&self, other: &Self) -> bool {
        self.filename == other.filename && self.block_num == other.block_num
    }
}

/// Converting the block identifier to readable strings and returning them
impl ToString for BlockId {
    fn to_string(&self) -> String {
        format!("{},{}", self.filename, self.block_num)
    }
}