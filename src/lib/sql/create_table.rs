use crate::schema::types::Type;

#[derive(Debug)]
pub struct CreateTable {
    table_name: String,
    fields: Vec<TableField>,
}

impl CreateTable {
    pub fn new(table_name: String, fields: Vec<TableField>) -> Self {
        Self { table_name, fields }
    }
}

#[derive(Debug)]
pub struct TableField {
    name: String,
    datatype: Type,
    constraints: Vec<Constraint>,
}

impl TableField {
    pub fn new(name: String, datatype: Type, constraints: Vec<Constraint>) -> Self {
        Self {
            name,
            datatype,
            constraints,
        }
    }
}

#[derive(Debug)]
pub enum Constraint {
    PrimaryKey,
    NotNull,
    Unique,
    References(String, String),
}
