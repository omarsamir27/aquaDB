use super::seq_scan::SeqScan;
use crate::common::numerical::ByteMagic;
use crate::query::tuple_table::Table;
use crate::schema::types::{NumericType, Type};
use crate::table::tablemgr::TableManager;
use evalexpr::{ContextWithMutableVariables, IntType, Value};
use std::collections::HashMap;

type TupleField = Option<Vec<u8>>;

pub struct Executor<'db> {
    max_table_memory: usize,
    db_tables: &'db HashMap<String, TableManager>,
    proc_tables: Vec<Table>,
}

impl<'db> Executor<'db> {
    // fn execute_select(&self,node:select_node){
    //     todo!()
    // }

    pub fn new(max_table_memory: usize, db_tables: &'db HashMap<String, TableManager>) -> Self {
        Self {
            max_table_memory,
            db_tables,
            proc_tables: vec![],
        }
    }

    pub fn execute_seqscan(&mut self, node: SeqScan) {
        let (table, fields) = (node.table, node.fields);
        let tblmgr = self.db_tables.get(&table).unwrap();
        let mut table_iter = tblmgr.heapscan_iter();
        let headers = tblmgr
            .get_layout()
            .type_map()
            .into_iter()
            .filter(|(k, _)| fields.contains(k))
            .collect();
        let mut processing_table = Table::new(table.as_str(), headers, self.max_table_memory);
        let mut context = evalexpr::HashMapContext::new();
        let tree = evalexpr::build_operator_tree(" id > 0 ").unwrap();
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
            let tuple = tuple
                .into_iter()
                .filter(|(k, v)| fields.contains(k))
                .collect();
            processing_table.add_row_map(tuple);
        }
        processing_table.print_all();
        self.proc_tables.push(processing_table);
    }
}

trait FromBytes {}

impl FromBytes for i16 {}
impl FromBytes for i32 {}
impl FromBytes for i64 {}
impl FromBytes for f32 {}
impl FromBytes for f64 {}
impl FromBytes for String {}

fn construct_fromBytes(data: &[u8], t: Type) -> Box<dyn FromBytes> {
    match t {
        Type::Numeric(num) => match num {
            NumericType::SmallInt => Box::new(data.to_i16()) as Box<dyn FromBytes>,
            NumericType::Integer => Box::new(data.to_i32()) as Box<dyn FromBytes>,
            NumericType::BigInt => Box::new(data.to_i64()) as Box<dyn FromBytes>,
            NumericType::Single => Box::new(data.to_f32()) as Box<dyn FromBytes>,
            NumericType::Double => Box::new(data.to_f64()) as Box<dyn FromBytes>,
            NumericType::Serial => Box::new(data.to_i32()) as Box<dyn FromBytes>,
        },
        Type::Character(char) => {
            Box::new(String::from_utf8(data.to_vec()).unwrap()) as Box<dyn FromBytes>
        }
    }
}
