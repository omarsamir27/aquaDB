use crate::schema::schema::Schema;
use crate::schema::types::Type;
use crate::sql::create_table::Constraint::{NotNull, PrimaryKey, References, Unique};
use std::any::Any;
use std::collections::HashSet;
use std::str::FromStr;

#[derive(Debug)]
pub struct CreateTable {
    table_name: String,
    fields: Vec<TableField>,
    indexes: Vec<Index>,
}

impl CreateTable {
    pub fn new(table_name: String, create_entries: Vec<CreateTableEntry>) -> Self {
        let mut fields = vec![];
        let mut indexes = vec![];
        for entry in create_entries {
            match entry {
                CreateTableEntry::TableField(t) => fields.push(t),
                CreateTableEntry::Index(i) => indexes.push(i),
            }
        }
        Self {
            table_name,
            fields,
            indexes,
        }
        // Self { table_name, fields , indexes }
    }
    pub fn to_schema(&self) -> Schema {
        let mut schema = Schema::new();
        let mut field_names = HashSet::new();
        schema.set_name(&self.table_name);
        for field in self.fields.iter() {
            field_names.insert(field.name.as_str());
            let nullable = !field.constraints.contains(&NotNull);
            let primary = field.constraints.contains(&PrimaryKey);
            let unique = field.constraints.contains(&Unique);
            let references = field
                .constraints
                .iter()
                .find(|c| {
                    !matches!(
                        **c,
                        Constraint::NotNull | Constraint::PrimaryKey | Constraint::Unique
                    )
                })
                .map(|c| match c {
                    PrimaryKey | NotNull | Unique => unreachable!(),
                    References(c, t) => (c.to_owned(), t.to_owned()),
                });
            schema.add_field(
                &field.name,
                field.datatype,
                nullable,
                unique,
                references,
                None,
            )
        }
        for idx in self.indexes.iter() {
            if field_names.contains(idx.field.as_str()) {
                schema.add_index(&idx.name, &idx.field, idx.index_type)
            } else {
                todo!()
            }
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

#[derive(Debug, Eq, PartialEq)]
pub enum Constraint {
    PrimaryKey,
    NotNull,
    Unique,
    References(String, String),
}

pub enum CreateTableEntry {
    TableField(TableField),
    Index(Index),
}

#[derive(Debug, Eq, PartialEq)]
pub struct Index {
    name: String,
    field: String,
    index_type: IndexType,
}

impl Index {
    pub fn new(name: String, field: String, index_type: IndexType) -> Self {
        Self {
            name,
            field,
            index_type,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexType {
    Hash,
    Btree,
}

impl FromStr for IndexType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("hash") {
            Ok(Self::Hash)
        } else if s.eq_ignore_ascii_case("btree") {
            Ok(Self::Btree)
        } else {
            Err(())
        }
    }
}

impl ToString for IndexType {
    fn to_string(&self) -> String {
        match self {
            IndexType::Hash => String::from("hash"),
            IndexType::Btree => String::from("btree"),
        }
    }
}
