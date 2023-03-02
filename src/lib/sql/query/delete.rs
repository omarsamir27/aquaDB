#[derive(Debug)]
pub struct SqlDelete {
    table: String,
    where_clause: Option<String>,
}

impl SqlDelete {
    pub fn new(table: String, where_clause: Option<String>) -> Self {
        Self {
            table,
            where_clause,
        }
    }
}
