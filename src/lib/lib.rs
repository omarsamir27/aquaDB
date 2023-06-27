// #![allow(non_snake_case)]
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused)]

// extern crate core;

use crate::schema::types::{NumericType, Type};
use crate::sql::query::select::AggregateFunc;
use bincode::{Decode, Encode};
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::str::FromStr;

pub mod common;
pub mod database;
pub mod index;
pub mod interface;
pub mod meta;
pub mod query;
pub mod schema;
pub mod sql;
pub mod storage;
pub mod table;

pub const AQUA_HOME_VAR: &str = "AQUADATA";
pub fn AQUADIR() -> String {
    std::env::var(AQUA_HOME_VAR).unwrap()
}

pub fn AQUA_TMP_DIR() -> PathBuf {
    let home = PathBuf::from(AQUADIR());
    home.join("base").join("tmp")
}

#[derive(Encode, Decode, Debug, Clone, Hash, Eq, PartialEq)]
pub struct FieldId {
    pub table: String,
    pub field: String,
}

impl FieldId {
    pub fn new(table: &str, field: &str) -> Self {
        Self {
            table: table.to_string(),
            field: field.to_string(),
        }
    }
}

impl Display for FieldId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.table, self.field)
    }
}

impl FromStr for FieldId {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // let point = s.find('.').ok_or(())?;
        let (table, var) = s.split_once('.').ok_or(())?;
        Ok(Self {
            table: table.to_string(),
            field: var.to_string(),
        })
    }
}

impl From<FieldId> for TargetItem {
    fn from(value: FieldId) -> Self {
        Self::FieldId(value)
    }
}

#[derive(Encode, Decode, Debug, Clone, Hash, Eq, PartialEq)]
pub struct AggregateField {
    op: AggregateFunc,
    field: FieldId,
}

impl AggregateField {
    pub fn new(op: AggregateFunc, field: FieldId) -> Self {
        Self { op, field }
    }
    pub fn get_result_type(&self, field_type: Type) -> Type {
        match self.op {
            AggregateFunc::Min => field_type,
            AggregateFunc::Max => field_type,
            AggregateFunc::Count => Type::Numeric(NumericType::BigInt),
            AggregateFunc::Avg => Type::Numeric(NumericType::Double),
            AggregateFunc::Sum => match field_type {
                Type::Numeric(n) => match n {
                    NumericType::SmallInt
                    | NumericType::Integer
                    | NumericType::BigInt
                    | NumericType::Serial => Type::Numeric(NumericType::BigInt),
                    NumericType::Single | NumericType::Double => Type::Numeric(NumericType::Double),
                },
                _ => unreachable!(),
            },
        }
    }
}

impl From<AggregateField> for TargetItem {
    fn from(value: AggregateField) -> Self {
        Self::AggregateField(value)
    }
}

// this is mostly a hack to not modify most of the query planner
impl From<AggregateField> for FieldId {
    fn from(value: AggregateField) -> Self {
        let AggregateField {
            op,
            field: FieldId { table, field },
        } = value;
        Self {
            table,
            field: format!("{}_{}", op.to_string(), field),
        }
    }
}

#[derive(Encode, Decode, Debug, Clone, Hash, Eq, PartialEq)]
pub enum TargetItem {
    FieldId(FieldId),
    AggregateField(AggregateField),
}

impl TargetItem {
    pub fn new_field_id(field_id: FieldId) -> Self {
        Self::FieldId(field_id)
    }
    pub fn new_aggregate_field(field: AggregateField) -> Self {
        Self::AggregateField(field)
    }
}
