use crate::common::net::{receive_string, send_string};
use crate::meta::catalogmgr::CatalogManager;
use crate::sql::parser::Rule::Sql;
use crate::sql::parser::{parse_query, SqlParser};
use crate::storage;
use crate::storage::storagemgr::StorageManager;
use std::cell::RefCell;
use std::io::Read;
use std::net::TcpStream;
use std::rc::Rc;
use std::time::Duration;
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
        println!("{}", self.name);
        // let mut msg_len = [0;8];
        // self.conn.set_read_timeout(Some(Duration::from_secs(3)));
        loop {
            // if self.conn.read_exact(&mut msg_len).is_err(){
            //     return;
            // }
            let query = match receive_string(&mut self.conn) {
                Ok(s) => s,
                Err(_) => return,
            };
            let sql = parse_query(&query);
            dbg!(&sql);
            send_string(&mut self.conn, &format!("{:?}", sql));
        }
    }
}
