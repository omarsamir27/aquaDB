use std::cmp::Ordering;
use std::fmt::format;
use std::hash::Hash;

pub struct BlockId{
    pub filename: String,
    pub block_num: u64
}

impl BlockId {
    pub fn new(filename:&str,block_num:u64) -> Self{
        BlockId{filename:filename.to_string(),block_num}
    }

}
impl PartialEq for BlockId{
    fn eq(&self, other: &Self) -> bool {
        self.filename == other.filename && self.block_num == other.block_num
    }
}

impl ToString for BlockId{
    fn to_string(&self) -> String {
        format!("{},{}",self.filename,self.block_num)
    }
}