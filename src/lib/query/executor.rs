use super::seq_scan::SeqScan;
use crate::common::numerical::ByteMagic;
use crate::query::tuple_table::Table;
use crate::schema::types::{NumericType, Type};
use crate::table::tablemgr::TableManager;
use evalexpr::{ContextWithMutableVariables, FloatType, HashMapContext, IntType, Node, Value};
use std::collections::HashMap;

type TupleField = Option<Vec<u8>>;
type Record = Vec<(String, Option<Vec<u8>>)>;

pub struct Executor<'db> {
    max_table_memory: usize,
    db_tables: &'db HashMap<String, TableManager>,
    proc_tables: Vec<Table>,
}

impl<'db> Executor<'db> {
    pub fn new(max_table_memory: usize, db_tables: &'db HashMap<String, TableManager>) -> Self {
        Self {
            max_table_memory,
            db_tables,
            proc_tables: vec![],
        }
    }
    pub fn insert_record(&mut self, record: Record) -> Result<(), String> {
        Ok(())
    }

    pub fn execute_seqscan(&mut self, node: SeqScan) {
        let (table, fields) = (node.table, node.fields);
        let tblmgr = self.db_tables.get(&table).unwrap();
        let mut table_iter = tblmgr.heapscan_iter();
        let headers: HashMap<String, Type> = tblmgr
            .get_layout()
            .type_map()
            .into_iter()
            .filter(|(k, _)| fields.contains(k))
            .collect();
        let mut processing_table = Table::new(table.as_str(), headers, self.max_table_memory);
        let headers = tblmgr.get_layout().type_map();
        while let Some(tuple) = table_iter.next() {
            // EXECUTES PROJECTIONS EARLY INSTEAD OF DOING IT IN THE TEMP TABLE
            // CONTROVERSIAL !!

            // for var in tree.iter_identifiers(){
            //     let val = tuple.get(var).unwrap().as_ref().unwrap().to_vec();
            //     let ty = headers.get(var).unwrap().clone();
            //     let val  = construct_fromBytes(val.as_slice(),ty);
            //
            //     context.set_value(var.to_string(),)
            // }
            //
            // if !tree.eval_boolean_with_context(&context).unwrap() { continue}

            let mut context = evalexpr::HashMapContext::new();
            let tree = evalexpr::build_operator_tree(" id > 0 ").unwrap();

            // if Executor::filter(&tuple,&headers,&mut context,&tree){
            //     let tuple = tuple
            //         .into_iter()
            //         .filter(|(k, v)| fields.contains(k) )
            //         .collect();
            //     processing_table.add_row_map(tuple);
            // }
            let tuple = tuple
                .into_iter()
                .filter(|(k, v)| fields.contains(k))
                .collect();
            processing_table.add_row_map(tuple);
        }
        // processing_table.sort("name");
        processing_table.print_all();
        self.proc_tables.push(processing_table);
    }

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
}
