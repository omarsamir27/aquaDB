use crate::common::numerical::ByteMagic;
use crate::schema::types::CharType::VarChar;
use std::ops::Add;

#[derive(Clone, Copy, PartialEq,Debug)]
pub enum NumericType {
    SmallInt,
    Integer,
    BigInt,
    Single,
    Double,
    Serial,
}

impl NumericType {
    #[inline(always)]
    pub fn unit_size(self) -> u8 {
        match self {
            NumericType::SmallInt => 2,
            NumericType::Integer => 4,
            NumericType::BigInt => 8,
            NumericType::Single => 4,
            NumericType::Double => 8,
            NumericType::Serial => 4,
        }
    }

    #[inline(always)]
    pub fn read_from_tuple(self, tuple: &[u8], start_byte: u16) -> &[u8] {
        let start_byte = start_byte as usize;
        match self {
            NumericType::SmallInt => &tuple[start_byte..start_byte + 2],
            NumericType::Integer => &tuple[start_byte..start_byte + 4],
            NumericType::BigInt => &tuple[start_byte..start_byte + 8],
            NumericType::Single => &tuple[start_byte..start_byte + 4],
            NumericType::Double => &tuple[start_byte..start_byte + 8],
            NumericType::Serial => &tuple[start_byte..start_byte + 4],
        }
    }
}

#[derive(Clone, Copy, PartialEq,Debug)]
pub enum CharType {
    Char,
    VarChar,
}

impl CharType {
    pub fn needs_pointer(self) -> bool {
        self == VarChar
    }
    pub fn read_from_tuple(self, tuple: &[u8], start_byte: u16) -> &[u8] {
        let start_byte = start_byte as usize;
        let string_offset = tuple.extract_u16(start_byte) as usize;
        let length = tuple.extract_u16(start_byte + 2);
        &tuple[string_offset..string_offset + length as usize]
    }
}

#[derive(Clone, Copy, PartialEq,Debug)]
pub enum Type {
    Numeric(NumericType),
    Character(CharType),
}

impl Type {
    #[inline(always)]
    pub fn unit_size(self) -> Option<u8> {
        match self {
            Type::Numeric(numeric) => Some(numeric.unit_size()),
            Type::Character(_) => None,
        }
    }
    #[inline(always)]
    pub fn needs_pointer(self) -> bool {
        match self {
            Type::Numeric(_) => false,
            Type::Character(_) => true,
        }
    }

    #[inline(always)]
    pub fn read_from_tuple(self, tuple: &[u8], start_byte: u16) -> &[u8] {
        match self {
            Type::Numeric(num) => num.read_from_tuple(tuple, start_byte),
            Type::Character(char) => char.read_from_tuple(tuple, start_byte),
        }
    }
}
