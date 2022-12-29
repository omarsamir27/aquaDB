use std::fmt::{Display, Formatter};
use std::io::Read;
use bincode::{Decode, Encode};
use crate::common::numerical::ByteMagic;
use crate::query::concrete_types::ConcreteType::{BigInt, Double, Integer, Serial, Single, SmallInt};
use crate::schema::types::{CharType, NumericType, Type as SchemaType};

#[derive(Debug,Clone,Encode,Decode)]
pub enum ConcreteType{
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Single(f32),
    Double(f64),
    Serial(i32),
    VarChar(String),
    Char(String)
}
impl ConcreteType{
    pub fn from_bytes(datatype:SchemaType,bytes :&[u8]) -> Self{
        match datatype {
            SchemaType::Numeric(num) =>
                match num {
                    NumericType::SmallInt => SmallInt(bytes.to_i16()),
                    NumericType::Integer => Integer(bytes.to_i32()),
                    NumericType::BigInt => BigInt(bytes.to_i64()),
                    NumericType::Single => Single(bytes.to_f32()),
                    NumericType::Double => Double(bytes.to_f64()),
                    NumericType::Serial => Serial(bytes.to_i32())
                },
            SchemaType::Character(char) =>
                match char {
                    CharType::Char => ConcreteType::Char( String::from_utf8_lossy(bytes).to_string() ),
                    CharType::VarChar => ConcreteType::VarChar( String::from_utf8_lossy(bytes).to_string())
                }
        }
    }
}

impl Display for ConcreteType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SmallInt(x) => write!(f,"{x}"),
            Integer(x) => write!(f,"{x}"),
            BigInt(x) => write!(f,"{x}"),
            Single(x) => write!(f,"{x}"),
            Double(x) => write!(f,"{x}"),
            Serial(x) => write!(f,"{x}"),
            ConcreteType::VarChar(sth) => write!(f,"{}",sth),
            ConcreteType::Char(sth) => write!(f,"{}",sth),
        }
    }
}

