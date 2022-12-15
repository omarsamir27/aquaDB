use crate::common::numerical::ByteMagic;
use crate::schema::types::CharType::VarChar;
use std::ops::Add;

/// An enumeration for the numeric data types in the database
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum NumericType {
    SmallInt,
    Integer,
    BigInt,
    Single,
    Double,
    Serial,
}

impl NumericType {
    /// Returns the number of bytes used to store each data type
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

    /// Reads a certain data type from a tuple by the specific number of bytes used to store it
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

/// An enumeration for the String data types in the database
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum CharType {
    Char,
    VarChar,
}

impl CharType {
    /// Returns whether the current data type needs a pointer to store its offset and size (Varchar)
    pub fn needs_pointer(self) -> bool {
        self == VarChar
    }

    /// Reads a certain Char/Varchar from a tuple by its offset and size stored in its pointer
    pub fn read_from_tuple(self, tuple: &[u8], start_byte: u16) -> &[u8] {
        let start_byte = start_byte as usize;
        let string_offset = tuple.extract_u16(start_byte) as usize;
        let length = tuple.extract_u16(start_byte + 2);
        &tuple[string_offset..string_offset + length as usize]
    }
}

/// Helper type classifier into Numeric or Character
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Type {
    Numeric(NumericType),
    Character(CharType),
}

impl Type {
    #[inline(always)]
    pub fn unit_size(self) -> Option<u8> {
        match self {
            Type::Numeric(numeric) => Some(numeric.unit_size()),
            Type::Character(_) => Some(4_u8),
        }
    }

    /// Returns whether the current data type needs a pointer to store its offset and size
    ///
    /// Numeric types do not need pointers
    /// Varchars need pointers
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
