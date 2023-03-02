use crate::sql::query::{delete::SqlDelete, insert::SqlInsert, select::SqlSelect, update::SqlUpdate};

#[allow(non_snake_case, clippy::upper_case_acronyms)]
#[derive(Debug)]
pub enum SqlQuery {
    SELECT(SqlSelect),
    INSERT(SqlInsert),
    DELETE(SqlDelete),
    UPDATE(SqlUpdate),
}

#[allow(non_snake_case, clippy::upper_case_acronyms)]
impl SqlQuery {
    pub fn SELECT(query: SqlSelect) -> Self {
        Self::SELECT(query)
    }
    pub fn INSERT(query: SqlInsert) -> Self {
        Self::INSERT(query)
    }
    pub fn UPDATE(query: SqlUpdate) -> Self {
        Self::UPDATE(query)
    }
    pub fn DELETE(query: SqlDelete) -> Self {
        Self::DELETE(query)
    }
}

#[derive(Debug)]
pub enum SqlValue {
    Text(String),
    Numeric(String),
    NULL,
}
