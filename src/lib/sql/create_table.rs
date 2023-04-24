use std::any::Any;
use crate::schema::schema::Schema;
use crate::schema::types::Type;
use crate::sql::create_table::Constraint::{NotNull, PrimaryKey, References, Unique};

#[derive(Debug)]
pub struct CreateTable {
    table_name: String,
    fields: Vec<TableField>,
}

impl CreateTable {
    pub fn new(table_name: String, fields: Vec<TableField>) -> Self {
        Self { table_name, fields }
    }
    pub fn to_schema(&self)-> Schema{
        let mut schema =  Schema::new();
        schema.set_name(&self.table_name);
        for field in self.fields.iter(){
            let nullable = field.constraints.contains(&NotNull);
            let primary = field.constraints.contains(&PrimaryKey);
            let unique = field.constraints.contains(&Unique);
            let references = field.constraints
                .iter()
                .find(|c| !matches!(**c,Constraint::NotNull| Constraint::PrimaryKey|Constraint::Unique))
                .map(|c| match c {
                    PrimaryKey | NotNull | Unique => unreachable!(),
                    References(c, t) => (c.to_owned(),t.to_owned())
                });
            schema.add_field(&field.name, field.datatype, nullable , unique, references, None)
        }
        schema
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

#[derive(Debug,Eq,PartialEq)]
pub enum Constraint {
    PrimaryKey,
    NotNull,
    Unique,
    References(String, String),
}


