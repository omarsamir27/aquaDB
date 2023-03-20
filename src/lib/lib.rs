// #![allow(non_snake_case)]
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused)]

extern crate core;

pub mod common;
pub mod database;
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
