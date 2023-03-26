use crate::sql::query::query::SqlValue;

#[derive(Debug)]
pub struct SqlUpdate {
    table: String,
    pairs: Vec<(String, SqlValue)>,
    where_clause: Option<String>,
}

impl SqlUpdate {
    pub fn new(
        table: String,
        pairs: Vec<(String, SqlValue)>,
        where_clause: Option<String>,
    ) -> Self {
        Self {
            table,
            pairs,
            where_clause,
        }
    }
}
