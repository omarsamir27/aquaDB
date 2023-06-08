use crate::common::net::{receive_string, send_string};
use crate::meta::catalogmgr::CatalogManager;
// use crate::query::plan::{create_plan, QueryPlan};
use crate::interface::message::{Message, Status};
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
use crate::query::physical::PhysicalNode;

type Storage = Rc<RefCell<StorageManager>>;
type Catalog = Rc<RefCell<CatalogManager>>;
type Record = Result<Vec<(String, Option<Vec<u8>>)>, ()>;
type DbTables = HashMap<String, TableManager>;

const MAX_WORKING_MEMORY: usize = 16000;
type Row = HashMap<String, Option<Vec<u8>>>;
// type TreeNode = Box<dyn Iterator<Item = Row>>;

pub enum QueryPlan {
    CreateTable(Schema),
    Insert(Record, Schema),
    Select(PhysicalNode),
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
        // .into_iter().map(|(k,v)| (k,Rc::new(v))).collect();

        let tables = catalog.borrow().get_db_tables(name);
        Self {
            name: name.to_string(),
            storage,
            catalog,
            conn,
            tables,
        }
    }
    fn flush_everything(&self) {
        for tbl in self.tables.values() {
            tbl.flush_all();
        }
    }
    pub fn handle_connection(&mut self) {
        // ctrlc::set_handler(|| self.flush_everything());
        // let mut executor = Executor::new(MAX_WORKING_MEMORY, &mut self.tables);
        // executor.simulate_join("names".to_string(), "jobs".to_string(), "id".to_string());
        self.conn.set_nonblocking(false);
        loop {
            // let query = match receive_string(&mut self.conn) {
            //     Ok(s) => s,
            //     Err(_) => return,
            // };
            let query = match Message::receive_msg(&mut self.conn) {
                Ok(msg) => match msg.get_query() {
                    Ok(s) => s,
                    _ => return,
                },
                Err(_) => return,
            };
            if query.eq_ignore_ascii_case("exit"){
                return;
            }
            match parse_query(&query) {
                Ok(parsed) => self.execute_cmd(parsed),
                // Err(e) => send_string(&mut self.conn, &format!("{:?}", e)).unwrap(),
                Err(e) => Message::Status(Status::Generic(e.to_string()))
                    .send_msg_to(&mut self.conn)
                    .unwrap_or_default(),
            }
        }
    }
    fn execute_cmd(&mut self, query: Sql) {
        if let Ok(plan) = self.create_plan(query) {
            if let QueryPlan::CreateTable(schema) = plan {
                self.add_schema(schema);
            } else {
                let mut executor = Executor::new(&mut self.tables);
                if let QueryPlan::Insert(record, schema) = plan {
                    match record {
                        Ok(r) => match executor.insert_record(r, schema) {
                            Ok(_) => Message::Status(Status::RecordInserted)
                                .send_msg_to(&mut self.conn)
                                .unwrap_or_default(),
                            Err(e) => Message::Status(Status::RecordNotInserted(e))
                                .send_msg_to(&mut self.conn)
                                .unwrap_or_default(),
                        },
                        Err(e) => {
                            Message::Status(Status::RecordNotInserted("TODO ERROR".to_string()))
                                .send_msg_to(&mut self.conn)
                                .unwrap_or_default()
                        }
                    }

                } else if let QueryPlan::Select(s) = plan {
                    let types = s.get_type_map();
                    Message::FieldTypes(types).send_msg_to(&mut self.conn).unwrap_or_default();
                    let results = s.collect::<Vec<_>>();
                    let msg = Message::Results(results);
                    msg.send_msg_to(&mut self.conn);
                    Message::Status(Status::ResultsFinished).send_msg_to(&mut self.conn).unwrap_or_default()
                    // dbg!(v);
                }
            }

        } else {
            // send_string(&mut self.conn, "DAMNN").unwrap()
            Message::Status(Status::BadCommand)
                .send_msg_to(&mut self.conn)
                .unwrap_or_default();
        }
    }
    fn add_schema(&mut self, schema: Schema) {
        match self.catalog.borrow_mut().add_schema(&self.name, &schema) {
            Ok(table) => {
                self.tables.insert(schema.name().to_string(), table);
                // send_string(
                //     &mut self.conn,
                //     &format!("Table: {} created successfully", schema.name()),
                // )
                Message::Status(Status::TableCreated(schema.name().to_string()))
                    .send_msg_to(&mut self.conn)
                    .unwrap_or_default();
            }
            Err(e) => Message::Status(Status::TableNotCreated(schema.name().to_string(), e))
                .send_msg_to(&mut self.conn)
                .unwrap_or_default(), // Err(s) => send_string(&mut self.conn, s.as_str()).unwrap(),
        }
    }
    fn create_plan(&self, query_tree: Sql) -> Result<QueryPlan, ()> {
        match query_tree {
            Sql::CreateTable(ct) => Ok(QueryPlan::CreateTable(ct.to_schema())),
            Sql::Query(query) => match query {
                SqlQuery::SELECT(s) => Ok(QueryPlan::Select(self.plan_query(s).unwrap())),
                SqlQuery::INSERT(i) => {
                    let catalog = self.catalog.borrow();
                    let schema = catalog.get_schema(&self.name, i.target_table()).ok_or(())?;
                    let record = i.raw_bytes(&schema);
                    Ok(QueryPlan::Insert(record, schema))
                }
                SqlQuery::DELETE(_) => todo!(),
                SqlQuery::UPDATE(_) => todo!(),
            },
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn storage(&self) -> &Storage {
        &self.storage
    }
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
    pub fn tables(&self) -> &DbTables {
        &self.tables
    }
}

impl Drop for DatabaseInstance {
    fn drop(&mut self) {
        self.flush_everything()
    }
}
