use crate::common::net::{receive_string, send_string};
use crate::meta::catalogmgr::CatalogManager;
// use crate::query::plan::{create_plan, QueryPlan};
use crate::query::executor::Executor;
use crate::schema::schema::Schema;
use crate::sql::parser::{parse_query, SqlParser};
use crate::sql::query::query::SqlQuery;
use crate::sql::Sql;
use crate::storage;
use crate::storage::storagemgr::StorageManager;
use crate::table::tablemgr::TableManager;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Read;
use std::net::TcpStream;
use std::rc::Rc;
use std::time::Duration;

type Storage = Rc<RefCell<StorageManager>>;
type Catalog = Rc<RefCell<CatalogManager>>;
type Record = Result<Vec<(String, Option<Vec<u8>>)>, ()>;
type DbTables = HashMap<String, TableManager>;

const MAX_WORKING_MEMORY: usize = 16000;

pub enum QueryPlan {
    CreateTable(Schema),
    Insert(Record,Schema),
}

pub struct DatabaseInstance {
    name: String,
    storage: Storage,
    catalog: Catalog,
    conn: TcpStream,
    tables: DbTables,
}

impl DatabaseInstance {
    pub fn new(name: &str, storage: Storage, catalog: Catalog, conn: TcpStream) -> Self {
        let tables = catalog.borrow().get_db_tables(name);
        Self {
            name: name.to_string(),
            storage,
            catalog,
            conn,
            tables,
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
                Ok(parsed) => self.execute_cmd(parsed),
                Err(e) => send_string(&mut self.conn, &format!("{:?}", e)).unwrap(),
            }
        }
    }
    fn execute_cmd(&mut self, query: Sql) {
        if let Ok(plan) = self.create_plan(query) {
            if let QueryPlan::CreateTable(schema) = plan {
                self.add_schema(schema);
            } else {
                let executor = Executor::new(MAX_WORKING_MEMORY, &mut self.tables);
                if let QueryPlan::Insert(record,schema) = plan {
                    if let Ok(record) = record {
                        todo!();
                        executor.insert_record(record,schema);
                    }
                }
            }
            // match plan {
            //     QueryPlan::CreateTable(schema) => self.add_schema(schema),
            //     QueryPlan::Insert(record) => todo!(),
            //     _ => todo!(),
            // }
        } else {
            send_string(&mut self.conn, "DAMNN").unwrap()
        }
    }
    fn add_schema(&mut self, schema: Schema) {
        match &self.catalog.borrow_mut().add_schema(&self.name, &schema) {
            Ok(_) => send_string(
                &mut self.conn,
                &format!("Table: {} created successfully", schema.name()),
            )
            .unwrap(),
            Err(s) => send_string(&mut self.conn, s).unwrap(),
        }
    }
    fn create_plan(&self, query_tree: Sql) -> Result<QueryPlan, ()> {
        match query_tree {
            Sql::CreateTable(ct) => Ok(QueryPlan::CreateTable(ct.to_schema())),
            Sql::Query(query) => match query {
                SqlQuery::SELECT(_) => todo!(),
                SqlQuery::INSERT(i) => {
                    let catalog = self.catalog.borrow();
                    let schema = catalog.get_schema(&self.name, i.target_table()).ok_or(())?;
                    let record = i.raw_bytes(&schema);
                    Ok(QueryPlan::Insert(record,schema))
                }
                SqlQuery::DELETE(_) => todo!(),
                SqlQuery::UPDATE(_) => todo!(),
            },
        }
    }
}
