use super::seq_scan::SeqScan;
use crate::table::tablemgr::TableManager;
use std::collections::HashMap;
use crate::query::tuple_table::Table;
use crate::schema::types::Type;

type TupleField = Option<Vec<u8>>;

pub struct Executor<'db> {
    max_table_memory : usize,
    db_tables: &'db HashMap<String, TableManager>,
    proc_tables : Vec<Table>
}

impl<'db> Executor<'db> {
    // fn execute_select(&self,node:select_node){
    //     todo!()
    // }

    pub fn new(max_table_memory:usize,db_tables:&'db HashMap<String,TableManager>)-> Self{
        Self{
            max_table_memory,
            db_tables,
            proc_tables : vec![]
        }
    }

    pub fn execute_seqscan(&mut self, node: SeqScan) {
        let (table, fields) = (node.table, node.fields);
        let tblmgr = self.db_tables.get(&table).unwrap();
        let mut table_iter = tblmgr.heapscan_iter();
        let headers = tblmgr
            .get_layout()
            .map()
            .iter()
            .filter(|(k,_)| fields.contains(k))
            .map(|(k, v)| (k.to_string(), v.0))
            .collect();
        let mut processing_table = Table::new(table.as_str(),headers,self.max_table_memory);
        while let Some(tuple) = table_iter.next(){
            // EXECUTES PROJECTIONS EARLY INSTEAD OF DOING IT IN THE TEMP TABLE
            // CONTROVERSIAL !!
            let tuple = tuple.into_iter().filter(|(k,v)| fields.contains(k)).collect();
            processing_table.add_row_map(tuple);
        };
        processing_table.print_all();
        self.proc_tables.push(processing_table);
    }
}
