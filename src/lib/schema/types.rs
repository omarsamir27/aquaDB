use crate::common::numerical::ByteMagic;
use crate::schema::types::CharType::{Char, VarChar};
use crate::schema::types::NumericType::{BigInt, Double, Integer, Serial, Single, SmallInt};
use std::ops::Add;
use std::str::FromStr;

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

impl ToString for NumericType {
    fn to_string(&self) -> String {
        match self {
            NumericType::SmallInt => String::from("smallint"),
            NumericType::Integer => String::from("int"),
            NumericType::BigInt => String::from("bigint"),
            NumericType::Single => String::from("single"),
            NumericType::Double => String::from("double"),
            NumericType::Serial => String::from("serial"),
        }
    }
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

impl ToString for CharType {
    fn to_string(&self) -> String {
        match self {
            CharType::Char => String::from("char"),
            VarChar => String::from("varchar"),
        }
    }
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
    Boolean,
}

impl ToString for Type {
    fn to_string(&self) -> String {
        match self {
            Type::Numeric(num) => num.to_string(),
            Type::Character(char) => char.to_string(),
            Type::Boolean => String::from("bool"),
        }
    }
}

impl FromStr for Type {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "smallint" => Ok(Self::Numeric(SmallInt)),
            "int" => Ok(Self::Numeric(Integer)),
            "bigint" => Ok(Self::Numeric(BigInt)),
            "single" => Ok(Self::Numeric(Single)),
            "double" => Ok(Self::Numeric(Double)),
            "serial" => Ok(Self::Numeric(Serial)),
            "char" => Ok(Self::Character(Char)),
            "varchar" => Ok(Self::Character(VarChar)),
            "bool" => Ok(Self::Boolean),
            _ => Err("Type Unknown".to_string()),
        }
    }
}

impl Type {
    #[inline(always)]
    pub fn unit_size(self) -> Option<u8> {
        match self {
            Type::Numeric(numeric) => Some(numeric.unit_size()),
            Type::Character(_) => Some(4_u8),
            Type::Boolean => Some(1),
        }
    }

    /// Returns whether the current data type needs a pointer to store its offset and size
    ///
    /// Numeric types do not need pointers
    /// Varchars need pointers
    #[inline(always)]
    pub fn needs_pointer(self) -> bool {
        match self {
            Type::Numeric(_) | Type::Boolean => false,
            Type::Character(_) => true,
        }
    }

    #[inline(always)]
    pub fn read_from_tuple(self, tuple: &[u8], start_byte: u16) -> &[u8] {
        match self {
            Type::Numeric(num) => num.read_from_tuple(tuple, start_byte),
            Type::Character(char) => char.read_from_tuple(tuple, start_byte),
            Type::Boolean => &tuple[start_byte as usize..=start_byte as usize],
        }
    }
}
