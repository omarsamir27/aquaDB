// #![allow(non_snake_case)]
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused)]

// extern crate core;

use std::fmt::{Display, Formatter};
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

#[derive(Debug, Clone,Hash,Eq,PartialEq)]
pub struct FieldId {
    pub table: String,
    pub field: String,
}

impl FieldId{
    pub fn new(table: &str, field: &str) -> Self {
        Self { table:table.to_string(), field:field.to_string() }
    }
}

impl Display for FieldId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}.{})", self.table, self.field)
    }
}

impl FromStr for FieldId{
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let point = s.find('.').ok_or(())?;
        let (table,var) = s.split_at(point);
        Ok(Self { table: table.to_string(), field: var.to_string() })
    }
}
