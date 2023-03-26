use crate::sql::query::query::SqlValue;

#[derive(Debug)]
pub struct SqlInsert {
    into_table: String,
    fields: Vec<String>,
    values: Vec<SqlValue>,
}

impl SqlInsert {
    pub fn new(into_table: String, fields: Vec<String>, values: Vec<SqlValue>) -> Self {
        Self {
            into_table,
            fields,
            values,
        }
    }
}
