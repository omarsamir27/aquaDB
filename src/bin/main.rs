#![allow(non_snake_case)]


use std::rc::Rc;
use aqua::schema::null_bitmap::NullBitMap;
use aqua::schema::schema::Schema;
use aqua::schema::types::CharType::VarChar;
use aqua::schema::types::NumericType::{Integer, SmallInt};
use aqua::schema::types::Type;
use aqua::storage::tuple::Tuple;

fn main() {

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
    let layout = schema.to_layout();
    let layout = Rc::new(layout);
    let tuple = vec![
        // ("id".to_string(), None),
        ("id".to_string(), Some(100_u16.to_ne_bytes().to_vec())),
        // ("name".to_string(), Some("Omar".to_string().as_bytes().to_vec()))
        ("name".to_string(), None),
        // ("salary".to_string(), Some(5000_u32.to_ne_bytes().to_vec())),
        ("salary".to_string(), None),
        // ("job".to_string(), None)
        ("job".to_string(), Some("Engineer".to_string().as_bytes().to_vec()))
    ];
    let mut tuple = Tuple::new(tuple, layout.clone());
    let tuple_bytes = tuple.to_bytes();
    println!("{:?}", tuple_bytes);

    // let mut bitmap = NullBitMap::new()
    // println!("{:?}", bitmap);
    // for mut byte in 0..4_u8{
    //     byte = 0_u8;
    //     for bit in 0..8_u8{
    //         if bit%2 == 1 {
    //             byte |= 1 << bit;
    //         }
    //     }
    //     bitmap.push(byte);
    // }
    // println!("{:?}", bitmap);
}
