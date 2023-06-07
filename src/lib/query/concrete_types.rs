use crate::common::numerical::ByteMagic;
use crate::query::concrete_types::ConcreteType::{
    BigInt, Boolean, Char, Double, Integer, Serial, Single, SmallInt, VarChar,
};
use crate::schema::types::{CharType, NumericType, Type as SchemaType};
use bincode::{Decode, Encode};
use num_order::NumOrd;
use std::cmp::Ordering;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::fmt::{write, Display, Formatter};
use std::io::{Bytes, Read};

#[derive(Debug, Clone, Encode, Decode)]
pub enum ConcreteType {
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Single(f32),
    Double(f64),
    Serial(i32),
    VarChar(String),
    Char(String),
    Boolean(bool),
    NULL,
}
impl ConcreteType {
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            SmallInt(_) | Integer(_) | BigInt(_) | Single(_) | Double(_) | Serial(_)
        )
    }
    pub fn is_text(&self) -> bool {
        matches!(self, VarChar(_) | Char(_))
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::NULL)
    }

    pub fn from_bytes(datatype: SchemaType, bytes: &[u8]) -> Self {
        if bytes.is_empty() {
            return Self::NULL;
        }
        match datatype {
            SchemaType::Numeric(num) => match num {
                NumericType::SmallInt => SmallInt(bytes.to_i16()),
                NumericType::Integer => Integer(bytes.to_i32()),
                NumericType::BigInt => BigInt(bytes.to_i64()),
                NumericType::Single => Single(bytes.to_f32()),
                NumericType::Double => Double(bytes.to_f64()),
                NumericType::Serial => Serial(bytes.to_i32()),
            },
            SchemaType::Character(char) => match char {
                CharType::Char => ConcreteType::Char(String::from_utf8_lossy(bytes).to_string()),
                CharType::VarChar => {
                    ConcreteType::VarChar(String::from_utf8_lossy(bytes).to_string())
                }
            },
            SchemaType::Boolean => Boolean(bytes[0] == 1),
        }
    }
    pub fn to_bytes(self) -> Option<Vec<u8>>{
        let bytes = Vec::from(self);
        if bytes.is_empty(){
            None
        }
        else {
            Some(bytes)
        }
    }
}

impl From<ConcreteType> for Vec<u8>{
    fn from(value: ConcreteType) -> Self {
        match value{
            SmallInt(x) => x.to_ne_bytes().to_vec(),
            Integer(x) => x.to_ne_bytes().to_vec(),
            BigInt(x) => x.to_ne_bytes().to_vec(),
            Single(x) => x.to_ne_bytes().to_vec(),
            Double(x) => x.to_ne_bytes().to_vec(),
            Serial(x) => x.to_ne_bytes().to_vec(),
            VarChar(x) => x.as_bytes().to_vec(),
            Char(x) => x.as_bytes().to_vec(),
            Boolean(x) => {if x { vec![1] }else { vec![0] }},
            ConcreteType::NULL => vec![]
        }
    }
}

impl From<ConcreteType> for String {
    fn from(value: ConcreteType) -> Self {
        match value {
            SmallInt(x) => format!("{x}"),
            Integer(x) => format!("{x}"),
            BigInt(x) => format!("{x}"),
            Single(x) => format!("{x}"),
            Double(x) => format!("{x}"),
            Serial(x) => format!("{x}"),
            ConcreteType::VarChar(sth) => format!("{sth}"),
            ConcreteType::Char(sth) => format!("{sth}"),
            Boolean(b) => format!("{b}"),
            ConcreteType::NULL => format!(""),
        }
    }
}

impl Display for ConcreteType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SmallInt(x) => write!(f, "{x}"),
            Integer(x) => write!(f, "{x}"),
            BigInt(x) => write!(f, "{x}"),
            Single(x) => write!(f, "{x}"),
            Double(x) => write!(f, "{x}"),
            Serial(x) => write!(f, "{x}"),
            ConcreteType::VarChar(sth) => write!(f, "{sth}"),
            ConcreteType::Char(sth) => write!(f, "{sth}"),
            Boolean(b) => write!(f, "{b}"),
            ConcreteType::NULL => write!(f, ""),
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
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => false,
            },
            Integer(num1) => match other {
                SmallInt(num2) => NumOrd::num_eq(num1, num2),
                Integer(num2) => NumOrd::num_eq(num1, num2),
                BigInt(num2) => NumOrd::num_eq(num1, num2),
                Single(num2) => NumOrd::num_eq(num1, num2),
                Double(num2) => NumOrd::num_eq(num1, num2),
                Serial(num2) => NumOrd::num_eq(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => false,
            },
            BigInt(num1) => match other {
                SmallInt(num2) => NumOrd::num_eq(num1, num2),
                Integer(num2) => NumOrd::num_eq(num1, num2),
                BigInt(num2) => NumOrd::num_eq(num1, num2),
                Single(num2) => NumOrd::num_eq(num1, num2),
                Double(num2) => NumOrd::num_eq(num1, num2),
                Serial(num2) => NumOrd::num_eq(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => false,
            },
            Single(num1) => match other {
                SmallInt(num2) => NumOrd::num_eq(num1, num2),
                Integer(num2) => NumOrd::num_eq(num1, num2),
                BigInt(num2) => NumOrd::num_eq(num1, num2),
                Single(num2) => NumOrd::num_eq(num1, num2),
                Double(num2) => NumOrd::num_eq(num1, num2),
                Serial(num2) => NumOrd::num_eq(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => false,
            },
            Double(num1) => match other {
                SmallInt(num2) => NumOrd::num_eq(num1, num2),
                Integer(num2) => NumOrd::num_eq(num1, num2),
                BigInt(num2) => NumOrd::num_eq(num1, num2),
                Single(num2) => NumOrd::num_eq(num1, num2),
                Double(num2) => NumOrd::num_eq(num1, num2),
                Serial(num2) => NumOrd::num_eq(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => false,
            },
            Serial(num1) => match other {
                SmallInt(num2) => NumOrd::num_eq(num1, num2),
                Integer(num2) => NumOrd::num_eq(num1, num2),
                BigInt(num2) => NumOrd::num_eq(num1, num2),
                Single(num2) => NumOrd::num_eq(num1, num2),
                Double(num2) => NumOrd::num_eq(num1, num2),
                Serial(num2) => NumOrd::num_eq(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => false,
            },
            VarChar(char1) | Char(char1) => match other {
                VarChar(char2) | Char(char2) => char1.eq(char2),
                ConcreteType::NULL => false,
                _ => unreachable!(),
            },
            ConcreteType::NULL => other.is_null(),
            Boolean(b1) => match other {
                Boolean(b2) => b1 == b2,
                ConcreteType::NULL => false,
                _ => unreachable!(),
            },
        }
    }
}

impl Eq for ConcreteType {}

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
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => Some(Less),
            },
            Integer(num1) => match other {
                SmallInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Integer(num2) => NumOrd::num_partial_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Single(num2) => NumOrd::num_partial_cmp(num1, num2),
                Double(num2) => NumOrd::num_partial_cmp(num1, num2),
                Serial(num2) => NumOrd::num_partial_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => Some(Less),
            },
            BigInt(num1) => match other {
                SmallInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Integer(num2) => NumOrd::num_partial_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Single(num2) => NumOrd::num_partial_cmp(num1, num2),
                Double(num2) => NumOrd::num_partial_cmp(num1, num2),
                Serial(num2) => NumOrd::num_partial_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => Some(Less),
            },
            Single(num1) => match other {
                SmallInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Integer(num2) => NumOrd::num_partial_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Single(num2) => NumOrd::num_partial_cmp(num1, num2),
                Double(num2) => NumOrd::num_partial_cmp(num1, num2),
                Serial(num2) => NumOrd::num_partial_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => Some(Less),
            },
            Double(num1) => match other {
                SmallInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Integer(num2) => NumOrd::num_partial_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Single(num2) => NumOrd::num_partial_cmp(num1, num2),
                Double(num2) => NumOrd::num_partial_cmp(num1, num2),
                Serial(num2) => NumOrd::num_partial_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => Some(Less),
            },
            Serial(num1) => match other {
                SmallInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Integer(num2) => NumOrd::num_partial_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_partial_cmp(num1, num2),
                Single(num2) => NumOrd::num_partial_cmp(num1, num2),
                Double(num2) => NumOrd::num_partial_cmp(num1, num2),
                Serial(num2) => NumOrd::num_partial_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => Some(Less),
            },
            VarChar(char1) | Char(char1) => match other {
                VarChar(char2) | Char(char2) => char1.partial_cmp(char2),
                ConcreteType::NULL => Some(Less),
                _ => unreachable!(),
            },
            ConcreteType::NULL => {
                if other.is_null() {
                    Some(Ordering::Equal)
                } else {
                    Some(Greater)
                }
            }
            Boolean(b1) => match other {
                Boolean(b2) => b1.partial_cmp(b2),
                ConcreteType::NULL => Some(Less),
                _ => unreachable!(),
            },
        }
    }
}

impl Ord for ConcreteType {
    fn cmp(&self, other: &Self) -> Ordering {
        match self {
            SmallInt(num1) => match other {
                SmallInt(num2) => NumOrd::num_cmp(num1, num2),
                Integer(num2) => NumOrd::num_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_cmp(num1, num2),
                Single(num2) => NumOrd::num_cmp(num1, num2),
                Double(num2) => NumOrd::num_cmp(num1, num2),
                Serial(num2) => NumOrd::num_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => Less,
            },
            Integer(num1) => match other {
                SmallInt(num2) => NumOrd::num_cmp(num1, num2),
                Integer(num2) => NumOrd::num_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_cmp(num1, num2),
                Single(num2) => NumOrd::num_cmp(num1, num2),
                Double(num2) => NumOrd::num_cmp(num1, num2),
                Serial(num2) => NumOrd::num_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => Less,
            },
            BigInt(num1) => match other {
                SmallInt(num2) => NumOrd::num_cmp(num1, num2),
                Integer(num2) => NumOrd::num_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_cmp(num1, num2),
                Single(num2) => NumOrd::num_cmp(num1, num2),
                Double(num2) => NumOrd::num_cmp(num1, num2),
                Serial(num2) => NumOrd::num_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => Less,
            },
            Single(num1) => match other {
                SmallInt(num2) => NumOrd::num_cmp(num1, num2),
                Integer(num2) => NumOrd::num_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_cmp(num1, num2),
                Single(num2) => NumOrd::num_cmp(num1, num2),
                Double(num2) => NumOrd::num_cmp(num1, num2),
                Serial(num2) => NumOrd::num_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => Less,
            },
            Double(num1) => match other {
                SmallInt(num2) => NumOrd::num_cmp(num1, num2),
                Integer(num2) => NumOrd::num_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_cmp(num1, num2),
                Single(num2) => NumOrd::num_cmp(num1, num2),
                Double(num2) => NumOrd::num_cmp(num1, num2),
                Serial(num2) => NumOrd::num_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => Less,
            },
            Serial(num1) => match other {
                SmallInt(num2) => NumOrd::num_cmp(num1, num2),
                Integer(num2) => NumOrd::num_cmp(num1, num2),
                BigInt(num2) => NumOrd::num_cmp(num1, num2),
                Single(num2) => NumOrd::num_cmp(num1, num2),
                Double(num2) => NumOrd::num_cmp(num1, num2),
                Serial(num2) => NumOrd::num_cmp(num1, num2),
                ConcreteType::VarChar(_) | ConcreteType::Char(_) | Boolean(_) => unreachable!(),
                ConcreteType::NULL => Less,
            },
            VarChar(char1) | Char(char1) => match other {
                VarChar(char2) | Char(char2) => char1.cmp(char2),
                ConcreteType::NULL => Less,
                _ => unreachable!(),
            },
            ConcreteType::NULL => {
                if other.is_null() {
                    Equal
                } else {
                    Greater
                }
            }
            Boolean(b1) => match other {
                Boolean(b2) => b1.cmp(b2),
                ConcreteType::NULL => Less,
                _ => unreachable!(),
            },
        }
    }
}
