use crate::schema::schema::Schema;
use crate::schema::types::{NumericType, Type};
use crate::sql::query::query::SqlValue;
use std::num::ParseIntError;

// type Result<>

#[derive(Debug)]
pub struct SqlInsert {
    target_table: String,
    record: Vec<(String, SqlValue)>,
}

impl SqlInsert {
    pub fn new(target_table: String, fields: Vec<String>, values: Vec<SqlValue>) -> Self {
        Self {
            target_table,
            record: fields.into_iter().zip(values.into_iter()).collect(),
        }
    }
    pub fn raw_bytes(self, schema: &Schema) -> Result<Vec<(String, Option<Vec<u8>>)>, ()> {
        let mut ret = vec![];
        let field_types = schema.field_types();
        for (col_name, col_val) in self.record {
            if let Some(field_type) = field_types.get(col_name.as_str()) {
                // match Self::column_bytes(col_val,field_type.clone()){
                //     Ok(res) => ret.push(res)
                //     Err(_) => return E
                // }
                ret.push((col_name, Self::column_bytes(col_val, *field_type)?))
            } else {
                return Err(());
            }
        }
        Ok(ret)
    }
    fn column_bytes(field: SqlValue, field_type: Type) -> Result<Option<Vec<u8>>, ()> {
        match field_type {
            Type::Numeric(num) => match field {
                SqlValue::NULL => Ok(None),
                SqlValue::Numeric(txt) => match num {
                    NumericType::SmallInt => match txt.parse::<i16>() {
                        Ok(n) => Ok(Some(n.to_ne_bytes().to_vec())),
                        Err(_) => Err(()),
                    },
                    NumericType::Integer => match txt.parse::<i32>() {
                        Ok(n) => Ok(Some(n.to_ne_bytes().to_vec())),
                        Err(_) => Err(()),
                    },
                    NumericType::BigInt => match txt.parse::<i64>() {
                        Ok(n) => Ok(Some(n.to_ne_bytes().to_vec())),
                        Err(_) => Err(()),
                    },
                    NumericType::Single => match txt.parse::<f32>() {
                        Ok(n) => Ok(Some(n.to_ne_bytes().to_vec())),
                        Err(_) => Err(()),
                    },
                    NumericType::Double => match txt.parse::<f64>() {
                        Ok(n) => Ok(Some(n.to_ne_bytes().to_vec())),
                        Err(_) => Err(()),
                    },
                    NumericType::Serial => match txt.parse::<i32>() {
                        Ok(n) => Ok(Some(n.to_ne_bytes().to_vec())),
                        Err(_) => Err(()),
                    },
                },
                _ => Err(()),
            },
            Type::Character(_) => match field {
                SqlValue::NULL => Ok(None),
                SqlValue::Text(t) => Ok(Some(t.as_bytes().to_vec())),
                _ => Err(()),
            },
            Type::Boolean => match field {
                SqlValue::NULL => Ok(None),
                SqlValue::Bool(b) => Ok(Some(if b { vec![1] } else { vec![0] })),
                _ => Err(()),
            },
        }
    }
    pub fn target_table(&self) -> &str {
        &self.target_table
    }
    pub fn record(&self) -> &Vec<(String, SqlValue)> {
        &self.record
    }
}
