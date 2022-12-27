use std::collections::HashMap;
use crate::schema::types::Type;
use thiserror::Error;
use crate::common::table::TableErrors::{InvalidColumn, MissingFields};

#[derive(Debug,Error)]
enum TableErrors{
    #[error("Number of Columns does not match")]
    MissingFields,
    #[error("Invalid Column Header Found")]
    InvalidColumn

}

type TableHeaders<T> = HashMap<String,(usize,T)>;
type Column = Option<Vec<u8>>;
type Row = Vec<Column>;
type RowMap = HashMap<String,Column>;
type InsertResult = Result<(),TableErrors>;

struct Table<T>{
    num_rows : u32,
    num_cols : u32,
    index_type_map: TableHeaders<T>,
    data : Vec<Row>
}
impl Table<Type>{
    pub fn new(headers:Vec<(String,Type)>) -> Self{
        let num_cols = headers.len();
        let mut table_headers = TableHeaders::new();
        for (idx,(col_name,col_type)) in headers.into_iter().enumerate(){
            table_headers.insert(col_name,(idx,col_type));
        }
        Self{
            num_cols : num_cols as u32,
            num_rows : 0,
            data : vec![],
            index_type_map : table_headers
        }
    }
    pub fn add_row_map(&mut self,row_map:RowMap) -> InsertResult{
        if row_map.len() != self.num_cols as usize{
            return Err(MissingFields)
        }
        let mut row = vec![Column::default();self.num_cols as usize];
        for (field_name,data) in row_map.into_iter(){
            match self.index_type_map.get(field_name.as_str()){
                None => { return Err(InvalidColumn); }
                Some((idx,_)) => { row[*idx] = data }
            }
        }
        self.data.push(row);
        self.num_rows += 1;

        Ok(())
    }
    pub fn add_row_vec(&mut self,row:Row) -> InsertResult{
        if row.len() != self.num_cols as usize{
            return Err(MissingFields)
        }
        self.data.push(row);
        self.num_rows +=1;
        Ok(())
    }
}