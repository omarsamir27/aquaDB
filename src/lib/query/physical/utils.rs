use super::{MergedRow, TypeMap};
use crate::common::numerical::ByteMagic;
use crate::query::concrete_types::ConcreteType;
use crate::schema::types::{NumericType, Type};
use crate::FieldId;
use evalexpr::{
    ContextWithMutableVariables, FloatType, HashMapContext, IntType, IterateVariablesContext, Value,
};
use std::collections::HashMap;

pub fn fill_ctx_map(ctx: &mut HashMapContext, row: &MergedRow, type_map: &TypeMap) {
    for table_var in ctx.iter_variable_names().collect::<Vec<_>>() {
        let field_id = table_var.parse::<FieldId>().unwrap();
        let val = row.get(&field_id).unwrap();
        let val = data_to_value(val.as_ref(), *type_map.get(&field_id).unwrap());
        ctx.update_or_set_value(table_var, val);
    }
}
pub fn data_to_value(data: Option<&Vec<u8>>, schema_type: Type) -> Value {
    if let Some(data) = data {
        match schema_type {
            Type::Numeric(n) => match n {
                NumericType::SmallInt => Value::Int(IntType::from(data.to_i16())),
                NumericType::Integer => Value::Int(IntType::from(data.to_i32())),
                NumericType::BigInt => Value::Int(IntType::from(data.to_i64())),
                NumericType::Single => Value::Float(data.to_f32() as FloatType),
                NumericType::Double => Value::Float(data.to_f64() as FloatType),
                NumericType::Serial => Value::Int(IntType::from(data.to_i32())),
            },
            Type::Character(c) => Value::String(String::from_utf8(data.to_vec()).unwrap()),
            Type::Boolean => Value::Boolean(data[0] == 1),
        }
    } else {
        Value::Empty
    }
}

pub fn row_to_merged_row(table: &str, row: HashMap<String, Option<Vec<u8>>>) -> MergedRow {
    row.into_iter()
        .map(|(k, v)| (FieldId::new(table, &k), v))
        .collect()
}

pub fn merge(left: &MergedRow, right: &MergedRow) -> MergedRow {
    let mut left = left.clone();
    let right = right.clone();
    left.extend(right);
    left
}

pub fn qualify_type_map() {}
