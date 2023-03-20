use crate::sql::create_table::CreateTable;
use crate::sql::query::query::SqlQuery;

pub mod create_table;
pub mod eval;
pub mod parser;
pub mod query;

#[derive(Debug)]
pub enum Sql {
    Query(SqlQuery),
    CreateTable(CreateTable),
}

impl Sql {
    fn new_query(query: SqlQuery) -> Self {
        Self::Query(query)
    }
    fn new_table(table: CreateTable) -> Self {
        Self::CreateTable(table)
    }
}
