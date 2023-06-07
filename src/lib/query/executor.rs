use super::seq_scan::SeqScan;
use crate::common::numerical::ByteMagic;
use crate::query::tuple_table::TupleTable;
use crate::schema::schema::Schema;
use crate::schema::types::{NumericType, Type};
use crate::table::tablemgr::TableManager;
use evalexpr::{ContextWithMutableVariables, FloatType, HashMapContext, IntType, Node, Value};
use std::collections::HashMap;
use std::fs;
use std::fs::File;

type TupleField = Option<Vec<u8>>;
type Record = Vec<(String, Option<Vec<u8>>)>;

pub struct Executor<'db> {
    max_table_memory: usize,
    db_tables: &'db mut HashMap<String, TableManager>,
    proc_tables: Vec<TupleTable>,
}

impl<'db> Executor<'db> {
    pub fn new(max_table_memory: usize, db_tables: &'db mut HashMap<String, TableManager>) -> Self {
        Self {
            max_table_memory,
            db_tables,
            proc_tables: vec![],
        }
    }
    pub fn insert_record(&mut self, record: Record, schema: Schema) -> Result<(), String> {
        let target_table = self.db_tables.get(schema.name()).ok_or(String::default())?;
        // let available_indexes = target_table.indexes();
        let fields = schema.fields_info();
        let mut need_fullscan = false;
        for (k, v) in &fields {
            if v.unique() && !fields.contains_key(k) {
                need_fullscan = true;
                break;
            }
        }
        let foreign_referring = fields
            .iter()
            .filter(|(_, v)| v.foreign_reference().is_some());
        if !need_fullscan {
            let unique_fields = fields.iter().filter(|(_, v)| v.unique());
            for (k, v) in unique_fields {
                let search_key = record
                    .iter()
                    .find(|(f, _)| f == k)
                    .unwrap()
                    .1
                    .as_ref()
                    .unwrap();
                let mut index = target_table.hashscan_iter(k, search_key).unwrap();
                // TODO : ADD BTREE INDEXES HERE
                if index.next().is_some() {
                    return Err("DUPLICATE".to_string());
                }
            }
            for (k, v) in foreign_referring {
                let (ref_table, ref_col) = v.foreign_reference().as_ref().unwrap();
                let referred_tbl = self.db_tables.get(ref_table);
                if let Some(referred_tbl) = referred_tbl {
                    if referred_tbl.field_exists(ref_col) {
                        let search_key = record
                            .iter()
                            .find(|(f, _)| f == k)
                            .unwrap()
                            .1
                            .as_ref()
                            .unwrap();
                        if let Some(mut ref_col_idx) =
                            referred_tbl.hashscan_iter(ref_col, search_key)
                        {
                            if ref_col_idx.next().is_none() {
                                return Err("No Value for Refer".to_string());
                            }
                        } else {
                            let mut heapiter = referred_tbl.heapscan_iter();
                            if !heapiter.any(|ref_row| {
                                ref_row
                                    .get(ref_col)
                                    .unwrap()
                                    .as_ref()
                                    .unwrap()
                                    .eq(search_key)
                            }) {
                                return Err("No Value for Refer".to_string());
                            }
                        }
                    } else {
                        return Err("Where COL??".to_string());
                    }
                } else {
                    return Err("Where Table??".to_string());
                }
            }
            let target_table = self
                .db_tables
                .get_mut(schema.name())
                .ok_or(String::default())?;
            target_table.try_insert_tuple(record);
        }

        Ok(())
    }

    // pub fn execute_seqscan(&mut self, node: SeqScan) {
    //     let (table, fields) = (node.table, node.fields);
    //     let tblmgr = self.db_tables.get(&table).unwrap();
    //     let mut table_iter = tblmgr.heapscan_iter();
    //     let headers: HashMap<String, Type> = tblmgr
    //         .get_layout()
    //         .type_map()
    //         .into_iter()
    //         .filter(|(k, _)| fields.contains(k))
    //         .collect();
    //     let mut processing_table = TupleTable::new(table.as_str(), headers, self.max_table_memory);
    //     let headers = tblmgr.get_layout().type_map();
    //     while let Some(tuple) = table_iter.next() {
    //         // EXECUTES PROJECTIONS EARLY INSTEAD OF DOING IT IN THE TEMP TABLE
    //         // CONTROVERSIAL !!
    //
    //         // for var in tree.iter_identifiers(){
    //         //     let val = tuple.get(var).unwrap().as_ref().unwrap().to_vec();
    //         //     let ty = headers.get(var).unwrap().clone();
    //         //     let val  = construct_fromBytes(val.as_slice(),ty);
    //         //
    //         //     context.set_value(var.to_string(),)
    //         // }
    //         //
    //         // if !tree.eval_boolean_with_context(&context).unwrap() { continue}
    //
    //         let mut context = evalexpr::HashMapContext::new();
    //         let tree = evalexpr::build_operator_tree(" id > 0 ").unwrap();
    //
    //         // if Executor::filter(&tuple,&headers,&mut context,&tree){
    //         //     let tuple = tuple
    //         //         .into_iter()
    //         //         .filter(|(k, v)| fields.contains(k) )
    //         //         .collect();
    //         //     processing_table.add_row_map(tuple);
    //         // }
    //         let tuple = tuple
    //             .into_iter()
    //             .filter(|(k, v)| fields.contains(k))
    //             .collect();
    //         processing_table.add_row_map(tuple);
    //     }
    //     // processing_table.sort("name");
    //     processing_table.print_all();
    //     self.proc_tables.push(processing_table);
    // }

    fn filter(
        tuple: &HashMap<String, Option<Vec<u8>>>,
        type_map: &HashMap<String, Type>,
        context: &mut HashMapContext,
        expr: &Node,
    ) -> bool {
        for (field_name, value) in tuple {
            if let Some(value) = value {
                match type_map.get(field_name).unwrap() {
                    Type::Numeric(num) => match num {
                        NumericType::SmallInt => context.set_value(
                            field_name.to_string(),
                            Value::Int(value.to_i16() as IntType),
                        ),
                        NumericType::Integer => context.set_value(
                            field_name.to_string(),
                            Value::Int(value.to_i32() as IntType),
                        ),
                        NumericType::BigInt => context.set_value(
                            field_name.to_string(),
                            Value::Int(value.to_i64() as IntType),
                        ),
                        NumericType::Single => context.set_value(
                            field_name.to_string(),
                            Value::Float(value.to_f32() as FloatType),
                        ),
                        NumericType::Double => context.set_value(
                            field_name.to_string(),
                            Value::Float(value.to_f32() as FloatType),
                        ),
                        NumericType::Serial => context.set_value(
                            field_name.to_string(),
                            Value::Int(value.to_i32() as IntType),
                        ),
                    },
                    Type::Character(_) => context.set_value(
                        field_name.to_string(),
                        Value::String(String::from_utf8(value.to_vec()).unwrap()),
                    ),
                    Type::Boolean => {
                        context.set_value(field_name.to_string(), Value::Boolean(value[0] == 1))
                    }
                };
            }
        }
        expr.eval_boolean_with_context(context).unwrap()
    }

    pub fn simulate_join(&self, table1: String, table2: String, join_field: String) {
        let mut res_vec = vec![];
        let target_table1 = self.db_tables.get(table1.as_str()).unwrap();
        let target_table2 = self.db_tables.get(table2.as_str()).unwrap();
        let mut outer_iter = target_table1.heapscan_iter();
        for record in outer_iter {
            let rec_copy = record.clone();
            let value = record.get(join_field.as_str()).unwrap().as_ref().unwrap();
            let mut inner_iter = target_table2
                .hashscan_iter(join_field.as_str(), value)
                .unwrap();
            for inner_record in inner_iter {
                let mut final_copy = rec_copy.clone();
                final_copy.extend(inner_record.into_iter());
                res_vec.push(final_copy);
            }
        }
        let result: Vec<u8> = res_vec
            .into_iter()
            .flat_map(|k| {
                k.values()
                    .flat_map(|v| v.as_ref().unwrap().clone())
                    .collect::<Vec<u8>>()
            })
            .collect();
        let joined_file = fs::write("/home/ahmed/join", result);
    }
}
