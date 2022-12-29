use crate::query::concrete_types::ConcreteType;
use crate::query::tuple_table::TableErrors::{InvalidColumn, MissingFields};
use crate::schema::types::Type;
use std::collections::HashMap;
use std::env::current_exe;
use std::fmt::{Display, Formatter, Pointer};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::mem;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TableErrors {
    #[error("Number of Columns does not match")]
    MissingFields,
    #[error("Invalid Column Header Found")]
    InvalidColumn,
}

type TableHeaders = HashMap<String, (usize, Type)>;
type Column = Option<ConcreteType>;
type Row = Vec<Column>;
type RowMap = HashMap<String, Option<Vec<u8>>>;
type InsertResult = Result<(), TableErrors>;



struct RowPrint<'a>(&'a Row);
impl<'a> Display for RowPrint<'a>{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let len = self.0.len();
        for idx in 0..len-1{
            let col = &self.0[idx];
            let col = col.as_ref().map_or("".to_string(),|column| column.to_string());
            f.write_fmt(format_args!("{col},"));
        }
        let col = &self.0[len-1];
        let col = col.as_ref().map_or("".to_string(),|column| column.to_string());
        f.write_fmt(format_args!("{col}"))
    }
}

const  TMP_DIR: &str = "tests/db/db_tmp";

pub struct Table {
    name : String,
    num_rows: u32,
    num_cols: u32,
    index_type_map: TableHeaders,
    data: Vec<Row>,
    segments: HashMap<u32,File>,
    current_segment: u32,
    max_data_size: usize,
    current_memory_use : usize
}
impl Table {
    pub fn new(name:&str,headers: Vec<(String, Type)>, max_memory: usize) -> Self {
        let num_cols = headers.len();
        let mut table_headers = TableHeaders::new();
        for (idx, (col_name, col_type)) in headers.into_iter().enumerate() {
            table_headers.insert(col_name, (idx, col_type));
        }
        Self {
            name: name.to_string(),
            num_cols: num_cols as u32,
            num_rows: 0,
            data: vec![],
            segments: HashMap::new(),
            index_type_map: table_headers,
            current_segment: 0,
            max_data_size: max_memory,
            current_memory_use : 0
        }
    }
    pub fn add_row_map(&mut self, row_map: RowMap) -> InsertResult {
        if self.current_memory_use >= self.max_data_size{
            let tmp_dir = Path::new(TMP_DIR);
            let path = format!("tmp{}{}",self.name,self.current_segment) ;
            let path = tmp_dir.join(path.as_str());
            let mut segment = File::options().read(true).write(true).create(true).open(path.clone()).unwrap();
            let current_segment_data = std::mem::take(&mut self.data);
            bincode::encode_into_std_write(current_segment_data,&mut segment,bincode::config::standard()).unwrap();
            let  segment = File::options().read(true).write(true).open(path).unwrap();
            self.segments.insert(self.current_segment,segment);
            self.current_segment +=1;
            self.num_rows =0;
            self.current_memory_use = 0;
        }
        if row_map.len() != self.num_cols as usize {
            return Err(MissingFields);
        }
        let mut row = vec![Column::None; self.num_cols as usize];
        for (field_name, data) in row_map.into_iter() {
            match self.index_type_map.get(field_name.as_str()) {
                None => {
                    return Err(InvalidColumn);
                }
                Some((idx, schema_type)) => {
                    row[*idx] =
                        data.map(|data| {
                            self.current_memory_use += data.len();
                            ConcreteType::from_bytes(*schema_type, data.as_slice())
                        })
                }
            }
        }
        self.data.push(row);
        self.num_rows += 1;

        Ok(())
    }
    // pub fn add_row_vec(&mut self,row:Row) -> InsertResult{
    //     if row.len() != self.num_cols as usize{
    //         return Err(MissingFields)
    //     }
    //     self.data.push(row);
    //     self.num_rows +=1;
    //     Ok(())
    // }


    fn load_segment(&mut self,segment:u32){
        let file =  self.segments.get_mut(&segment).unwrap();
        self.current_memory_use = file.metadata().unwrap().len() as usize;
        let mut data = vec![];
        let v = file.read_to_end(&mut data).unwrap();
        self.data = bincode::decode_from_slice(&data,bincode::config::standard()).unwrap().0;
        self.num_rows = self.data.len() as u32;
        self.current_segment = segment;

    }
    pub fn print_all(&mut self){
        let current_seg = self.current_segment;
        for row in self.data.iter(){
            println!("{}",RowPrint(row));
        }
        let rest : Vec<u32>  =  self.segments.keys().filter(|k| **k!=current_seg).copied().collect();
        for seg in rest{
            self.load_segment(seg);
            for row in self.data.iter(){
                println!("{}",RowPrint(row));
            }
        }

    }
}


