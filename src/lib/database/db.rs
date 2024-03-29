use crate::common::net::{receive_string, send_string};
use crate::meta::catalogmgr::CatalogManager;
// use crate::query::plan::{create_plan, QueryPlan};
use crate::interface::message::{Message, RowMap, Status};
use crate::query::executor::Executor;
use crate::query::physical::PhysicalNode;
use crate::schema::schema::Schema;
use crate::sql::parser::{parse_query, SqlParser};
use crate::sql::query::query::SqlQuery;
use crate::sql::Sql;
use crate::{AQUA_TMP_DIR, storage};
use crate::storage::storagemgr::StorageManager;
use crate::table::tablemgr::TableManager;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Read;
use std::net::TcpStream;
use std::ops::AddAssign;
use std::rc::Rc;
use std::time::Duration;
use crate::index::Rid;

type Storage = Rc<RefCell<StorageManager>>;
type Catalog = Rc<RefCell<CatalogManager>>;
type Record = Result<Vec<(String, Option<Vec<u8>>)>, String>;
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
    pub fn flush_everything(&self) {
        for tbl in self.tables.values() {
            tbl.flush_all();
        }
    }
    pub fn handle_connection(&mut self) {
        self.conn.set_nonblocking(false);
        'outer: loop {
            let query = match Message::receive_msg(&mut self.conn) {
                Ok(msg) => match msg.get_query() {
                    Ok(s) => s,
                    _ => return,
                },
                Err(_) => return,
            };
            if query.eq_ignore_ascii_case("exit db") {
                Message::Status(Status::Generic(format!("Exit DB {}", self.name)))
                    .send_msg_to(&mut self.conn)
                    .unwrap_or_default();
                return;
            }
            if query.eq("BATCH"){
                Message::Status(Status::Generic(String::from("BATCH INSERT MODE")))
                    .send_msg_to(&mut self.conn).unwrap();

                let inserts = match Message::receive_msg(&mut self.conn) {
                    Ok(msg) => match msg.get_query() {
                        Ok(s) => s,
                        _ => return,
                    },
                    Err(_) => return,
                };
                let mut inserted_counter = 0;
                let mut insert_plans = vec![];
                for line in inserts.lines(){
                    if line.is_empty() || line == "\n"{
                        continue;
                    }
                    match parse_query(line){
                        Ok(parsed) => {
                            match self.create_plan(parsed){
                                Ok(q) => {
                                    if let QueryPlan::Insert(r,s) = &q{
                                       match r{
                                           Ok(_) => insert_plans.push(q),
                                           Err(e) => { Message::Status(Status::Generic(format!("Batch Planning Failed: {}", e))).send_msg_to(&mut self.conn).unwrap();
                                               continue 'outer
                                           }
                                       }
                                    }
                                }
                                Err(e) => {
                                    Message::Status(Status::Generic(format!("Batch Planning Failed: {}", e))).send_msg_to(&mut self.conn).unwrap();
                                    continue 'outer
                                }
                            }
                        }
                        Err(e) => { Message::Status(Status::Generic(format!("Batch Planning Failed: {}", e))).send_msg_to(&mut self.conn).unwrap();
                            continue 'outer
                        }
                    }
                }
                let mut executor = Executor::new(&mut self.tables);
                let mut table_name = String::new();
                for plan in insert_plans{
                    if let QueryPlan::Insert(r,s) = plan{
                        table_name = s.name().to_string();
                        match executor.insert_record(r.unwrap(),s){
                            Ok(_) => inserted_counter+=1,
                            Err(e) => { Message::Status(Status::Generic(format!("Batch Insertion Failed at {} : {}", inserted_counter, e))).send_msg_to(&mut self.conn).unwrap();
                                continue;
                            }
                        }

                    }
                }
                Message::Status(Status::Generic(format!("Batch Success, Inserted: {}",inserted_counter))).send_msg_to(&mut self.conn).unwrap();
                // let target = self.tables.get_mut(&table_name).unwrap();
                // dbg!(&target.table_blocks);
                // // let direct = target.direct_accessor();
                // // dbg!(direct.get_tuple(Rid::new(9,0)));
                // let iter = target.heapscan_iter();
                // let v = iter.collect::<Vec<_>>();
                // dbg!(v.len());
                continue;
            }
            match parse_query(&query) {
                Ok(parsed) => self.execute_cmd(parsed),
                Err(e) => Message::Status(Status::Generic(e.to_string()))
                    .send_msg_to(&mut self.conn)
                    .unwrap_or_default(),
            }
        }
    }
    fn execute_cmd(&mut self, query: Sql) {
        match self.create_plan(query) {
            Ok(mut plan) => {
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
                            Err(e) => Message::Status(Status::RecordNotInserted(e))
                                .send_msg_to(&mut self.conn)
                                .unwrap_or_default(),
                        }
                    } else if let QueryPlan::Select(ref mut s) = plan {
                        let types = s.get_type_map();
                        Message::FieldTypes(types)
                            .send_msg_to(&mut self.conn)
                            .unwrap_or_default();
                        let mut total = 0;
                        loop {
                            let result: Vec<RowMap> = s.take(50).collect();
                            total += result.len();
                            if result.is_empty() {
                                Message::Status(Status::ResultsFinished)
                                    .send_msg_to(&mut self.conn)
                                    .unwrap_or_default();
                                break;
                            } else {
                                let msg = Message::Results(result);
                                msg.send_msg_to(&mut self.conn);
                            }
                        }
                    }
                }
            }

            Err(s) => {
                Message::Status(Status::Generic(s))
                    .send_msg_to(&mut self.conn)
                    .unwrap_or_default();
            }
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
    fn create_plan(&self, query_tree: Sql) -> Result<QueryPlan, String> {
        match query_tree {
            Sql::CreateTable(ct) => Ok(QueryPlan::CreateTable(ct.to_schema())),
            Sql::Query(query) => match query {
                SqlQuery::SELECT(s) => Ok(QueryPlan::Select(self.plan_query(s)?)),
                SqlQuery::INSERT(i) => {
                    let catalog = self.catalog.borrow();
                    let schema = catalog
                        .get_schema(&self.name, i.target_table())
                        .ok_or("Insert Error")?;
                    let record = i.raw_bytes(&schema);
                    Ok(QueryPlan::Insert(record, schema))
                }
                SqlQuery::DELETE(_) => todo!(),
                SqlQuery::UPDATE(_) => todo!(),
            },
        }
    }
    // fn batch_insert_planner();
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
        use std::fs;
        self.flush_everything();
        //TODO if concurrency is implemented this would be BAD
        let path = AQUA_TMP_DIR();
        fs::remove_dir_all(&path).and_then(|_| fs::create_dir(&path)).unwrap();
    }
}
