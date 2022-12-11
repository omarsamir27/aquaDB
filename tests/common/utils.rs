use aqua::schema::schema::{Layout, Schema};
use aqua::schema::types::CharType::VarChar;
use aqua::schema::types::NumericType::{Integer, SmallInt};
use aqua::schema::types::Type;
use aqua::storage::blockid::BlockId;

pub fn create_blockids(blk_cnt:u64,filename:&str) -> Vec<BlockId>{
    (0..blk_cnt).map(|num| BlockId::new(filename,num)).collect()
}

pub fn readfile(filename:&str) -> Vec<u8>{
    std::fs::read(filename).unwrap()
}

pub fn some_layout() -> Layout{
    let mut schema = Schema::new();
    let schema_vec = vec![
        ("id",Type::Numeric(SmallInt),false,None),
        ("name",Type::Character(VarChar),false,None),
        ("salary",Type::Numeric(Integer),false,None),
        ("job",Type::Character(VarChar),false,None)
    ];
    for attr in schema_vec{
        schema.add_field(
            attr.0,
            attr.1,
            attr.2,
            attr.3
        );
    }
    schema.to_layout()
}