use crate::common::net::{receive_string, send_string};
use crate::meta::catalogmgr::CatalogManager;
use crate::sql::Sql;
use crate::sql::parser::{parse_query, SqlParser};
use crate::storage;
use crate::storage::storagemgr::StorageManager;
use std::cell::RefCell;
use std::io::Read;
use std::net::TcpStream;
use std::rc::Rc;
use std::time::Duration;
use crate::query::plan::{create_plan, QueryPlan};
use crate::schema::schema::Schema;

type Storage = Rc<RefCell<StorageManager>>;
type Catalog = Rc<RefCell<CatalogManager>>;

pub struct DatabaseInstance {
    name: String,
    storage: Storage,
    catalog: Catalog,
    conn: TcpStream,
}

impl DatabaseInstance {
    pub fn new(name: &str, storage: Storage, catalog: Catalog, conn: TcpStream) -> Self {
        Self {
            name: name.to_string(),
            storage,
            catalog,
            conn,
        }
    }
    pub fn handle_connection(&mut self) {
        self.conn.set_nonblocking(false);
        loop {
            let query = match receive_string(&mut self.conn) {
                Ok(s) => s,
                Err(_) => return,
            };
            match parse_query(&query) {
                Ok(parsed) => self.execute_cmd(parsed) ,
                Err(e) => send_string(&mut self.conn, &format!("{:?}", e)).unwrap()

            }
        }
    }
    fn execute_cmd(&mut self,query:Sql){
       let plan = create_plan(&query);
        match plan {
            QueryPlan::CreateTable(schema) => self.add_schema(schema)
        }
    }
    fn add_schema(&mut self, schema:Schema)  {
       match &self.catalog.borrow_mut().add_schema(&self.name,&schema) {
           Ok(_) => send_string(&mut self.conn,&format!("Table: {} created successfully",schema.name())).unwrap() ,
           Err(s) => send_string(&mut self.conn,s).unwrap()
       }
    }
}
