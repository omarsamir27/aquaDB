use std::cmp::Ordering;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::fmt::{Display, Formatter, write};
use std::io::Read;
use bincode::{Decode, Encode};
use crate::common::numerical::ByteMagic;
use crate::query::concrete_types::ConcreteType::{BigInt, Double, Integer, Serial, Single, SmallInt,VarChar,Char};
use crate::schema::types::{CharType, NumericType, Type as SchemaType};
use num_order::NumOrd;

#[derive(Debug,Clone,Encode,Decode)]
pub enum ConcreteType{
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Single(f32),
    Double(f64),
    Serial(i32),
    VarChar(String),
    Char(String),
    NULL
}
impl ConcreteType{



    pub fn is_numeric(&self)-> bool{
        match self {
            SmallInt(_) | Integer(_) | BigInt(_) | Single(_) | Double(_) | Serial(_) => true,
            _ => false
        }
    }
    pub fn is_text(&self) -> bool{
        match self {
            VarChar(_) | Char(_) => true,
            _ => false
        }
    }

    pub fn is_null(&self) -> bool{
        match self {
            Self::NULL => true,
            _ => false
        }
    }

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
            ConcreteType::VarChar(sth) => write!(f,"{sth}"),
            ConcreteType::Char(sth) => write!(f,"{sth}"),
            ConcreteType::NULL => write!(f,"")
        }
    }
}

impl Default for ConcreteType {
    fn default() -> Self {
        Self::NULL
    }
}

impl PartialEq<Self> for ConcreteType {
    fn eq(&self, other: &ConcreteType) -> bool {
        match self {
            SmallInt(num1) => match other {
                SmallInt(num2) => NumOrd::num_eq(num1, num2),
                Integer(num2) => NumOrd::num_eq(num1, num2),
                BigInt(num2) => NumOrd::num_eq(num1, num2),
                Single(num2) => NumOrd::num_eq(num1, num2),
                Double(num2) => NumOrd::num_eq(num1, num2),
                Serial(num2) => NumOrd::num_eq(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => false,
            },
            Integer(num1) => match other {
                SmallInt(num2) => NumOrd::num_eq(num1, num2),
                Integer(num2) => NumOrd::num_eq(num1, num2),
                BigInt(num2) => NumOrd::num_eq(num1, num2),
                Single(num2) => NumOrd::num_eq(num1, num2),
                Double(num2) => NumOrd::num_eq(num1, num2),
                Serial(num2) => NumOrd::num_eq(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => false,
            },
            BigInt(num1) => match other {
                SmallInt(num2) => NumOrd::num_eq(num1, num2),
                Integer(num2) => NumOrd::num_eq(num1, num2),
                BigInt(num2) => NumOrd::num_eq(num1, num2),
                Single(num2) => NumOrd::num_eq(num1, num2),
                Double(num2) => NumOrd::num_eq(num1, num2),
                Serial(num2) => NumOrd::num_eq(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => false,
            },
            Single(num1) => match other {
                SmallInt(num2) => NumOrd::num_eq(num1, num2),
                Integer(num2) => NumOrd::num_eq(num1, num2),
                BigInt(num2) => NumOrd::num_eq(num1, num2),
                Single(num2) => NumOrd::num_eq(num1, num2),
                Double(num2) => NumOrd::num_eq(num1, num2),
                Serial(num2) => NumOrd::num_eq(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => false,
            },
            Double(num1) => match other {
                SmallInt(num2) => NumOrd::num_eq(num1, num2),
                Integer(num2) => NumOrd::num_eq(num1, num2),
                BigInt(num2) => NumOrd::num_eq(num1, num2),
                Single(num2) => NumOrd::num_eq(num1, num2),
                Double(num2) => NumOrd::num_eq(num1, num2),
                Serial(num2) => NumOrd::num_eq(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => false,
            }
            Serial(num1) => match other {
                SmallInt(num2) => NumOrd::num_eq(num1, num2),
                Integer(num2) => NumOrd::num_eq(num1, num2),
                BigInt(num2) => NumOrd::num_eq(num1, num2),
                Single(num2) => NumOrd::num_eq(num1, num2),
                Double(num2) => NumOrd::num_eq(num1, num2),
                Serial(num2) => NumOrd::num_eq(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => false,
            },
            VarChar(char1) | Char(char1) => match other {
                VarChar(char2) | Char(char2) => char1.eq(char2),
                ConcreteType::NULL => false,
                _ => unreachable!()
            },
            ConcreteType::NULL => other.is_null()
        }
}
}

impl Eq for ConcreteType{}


impl PartialOrd for ConcreteType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            SmallInt(num1) => match other {
                SmallInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Integer(num2) => NumOrd::num_partial_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Single(num2) => NumOrd::num_partial_cmp(num1, num2),
                Double(num2) => NumOrd::num_partial_cmp(num1, num2),
                Serial(num2) => NumOrd::num_partial_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => Some(Less),
            },
            Integer(num1) => match other {
                SmallInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Integer(num2) => NumOrd::num_partial_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Single(num2) => NumOrd::num_partial_cmp(num1, num2),
                Double(num2) => NumOrd::num_partial_cmp(num1, num2),
                Serial(num2) => NumOrd::num_partial_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => Some(Less),
            },
            BigInt(num1) => match other {
                SmallInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Integer(num2) => NumOrd::num_partial_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Single(num2) => NumOrd::num_partial_cmp(num1, num2),
                Double(num2) => NumOrd::num_partial_cmp(num1, num2),
                Serial(num2) => NumOrd::num_partial_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => Some(Less),
            },
            Single(num1) => match other {
                SmallInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Integer(num2) => NumOrd::num_partial_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Single(num2) => NumOrd::num_partial_cmp(num1, num2),
                Double(num2) => NumOrd::num_partial_cmp(num1, num2),
                Serial(num2) => NumOrd::num_partial_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => Some(Less),
            },
            Double(num1) => match other {
                SmallInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Integer(num2) => NumOrd::num_partial_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Single(num2) => NumOrd::num_partial_cmp(num1, num2),
                Double(num2) => NumOrd::num_partial_cmp(num1, num2),
                Serial(num2) => NumOrd::num_partial_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => Some(Less),
            }
            Serial(num1) => match other {
                SmallInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Integer(num2) => NumOrd::num_partial_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Single(num2) => NumOrd::num_partial_cmp(num1, num2),
                Double(num2) => NumOrd::num_partial_cmp(num1, num2),
                Serial(num2) => NumOrd::num_partial_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => Some(Less),
            },
            VarChar(char1) | Char(char1) => match other {
                VarChar(char2) | Char(char2) => char1.partial_cmp(char2),
                ConcreteType::NULL => Some(Less),
                _ => unreachable!()
            },
            ConcreteType::NULL => if other.is_null(){ Some(Ordering::Equal) }else{ Some(Greater) }
        }
    }
}

impl Ord for ConcreteType{
    fn cmp(&self, other: &Self) -> Ordering {
        match self {
            SmallInt(num1) => match other {
                SmallInt(num2) => NumOrd::num_cmp(num1, num2),
                Integer(num2) => NumOrd::num_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_cmp(num1, num2),
                Single(num2) => NumOrd::num_cmp(num1, num2),
                Double(num2) => NumOrd::num_cmp(num1, num2),
                Serial(num2) => NumOrd::num_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => Less,
            },
            Integer(num1) => match other {
                SmallInt(num2) => NumOrd::num_cmp(num1, num2),
                Integer(num2) => NumOrd::num_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_cmp(num1, num2),
                Single(num2) => NumOrd::num_cmp(num1, num2),
                Double(num2) => NumOrd::num_cmp(num1, num2),
                Serial(num2) => NumOrd::num_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => Less,
            },
            BigInt(num1) => match other {
                SmallInt(num2) => NumOrd::num_cmp(num1, num2),
                Integer(num2) => NumOrd::num_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_cmp(num1, num2),
                Single(num2) => NumOrd::num_cmp(num1, num2),
                Double(num2) => NumOrd::num_cmp(num1, num2),
                Serial(num2) => NumOrd::num_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => Less,
            },
            Single(num1) => match other {
                SmallInt(num2) => NumOrd::num_cmp(num1, num2),
                Integer(num2) => NumOrd::num_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_cmp(num1, num2),
                Single(num2) => NumOrd::num_cmp(num1, num2),
                Double(num2) => NumOrd::num_cmp(num1, num2),
                Serial(num2) => NumOrd::num_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => Less,
            },
            Double(num1) => match other {
                SmallInt(num2) => NumOrd::num_cmp(num1, num2),
                Integer(num2) => NumOrd::num_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_cmp(num1, num2),
                Single(num2) => NumOrd::num_cmp(num1, num2),
                Double(num2) => NumOrd::num_cmp(num1, num2),
                Serial(num2) => NumOrd::num_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => Less,
            }
            Serial(num1) => match other {
                SmallInt(num2) => NumOrd::num_cmp(num1, num2),
                Integer(num2) => NumOrd::num_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_cmp(num1, num2),
                Single(num2) => NumOrd::num_cmp(num1, num2),
                Double(num2) => NumOrd::num_cmp(num1, num2),
                Serial(num2) => NumOrd::num_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) => unreachable!(),
                ConcreteType::NULL => Less,
            },
            VarChar(char1) | Char(char1) => match other {
                VarChar(char2) | Char(char2) => char1.cmp(char2),
                ConcreteType::NULL => Less,
                _ => unreachable!()
            },
            ConcreteType::NULL => if other.is_null(){ Equal }else{ Greater }
        }
    }
}