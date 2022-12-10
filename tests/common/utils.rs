use crate::BlockId;

pub fn create_blockids(blk_cnt:u64,filename:&str) -> Vec<BlockId>{
    (0..blk_cnt).map(|num| BlockId::new(filename,num)).collect()
}

pub fn readfile(filename:&str) -> Vec<u8>{
    std::fs::read(filename).unwrap()
}