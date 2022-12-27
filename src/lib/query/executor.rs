use std::collections::HashMap;
use crate::table::tablemgr::TableManager;
use super::seq_scan::SeqScan;

type TupleField = Option<Vec<u8>>;

struct Executor<'db> {
    tables : &'db HashMap<String,TableManager>
}

impl<'db> Executor<'db> {
    // fn execute_select(&self,node:select_node){
    //     todo!()
    // }

    // fn execute_seqscan(&self,node:SeqScan){
    //     let (table,fields) = (node.table,node.fields);
    //     let tblmgr = self.tables.get(&table).unwrap();
    //     let mut table_iter = tblmgr.heapscan_iter();
    //     let tuples : HashMap<String,Vec<TupleField>>
    // }
}
