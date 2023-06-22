use crate::common::net::{receive_string, send_string};
use crate::database::db::DatabaseInstance;
use crate::interface::message::{Message, Status};
use crate::meta::catalogmgr::CatalogManager;
use crate::storage::storagemgr::StorageManager;
use crate::{RcRefCell, AQUADIR};
use std::cell::RefCell;
use std::fmt::Display;
use std::io::{Read, Write};
use std::net::{Incoming, IpAddr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::rc::Rc;
use std::thread::sleep;
use std::time::Duration;

const BLK_SIZE: usize = 4096;
const BUFFER_COUNT: u32 = 100;

pub struct DatabaseServer {
    home_dir: String,
    bind_addr: Vec<String>,
    sockets: Vec<TcpListener>,
    catalog: Rc<RefCell<CatalogManager>>,
    storage: Rc<RefCell<StorageManager>>,
}

impl DatabaseServer {
    fn create_server_sockets(addrs: &[String]) -> Vec<TcpListener> {
        addrs
            .iter()
            .map(|ip| {
                let socket =
                    TcpListener::bind(ip).expect(format!("Could not bind to {ip}").as_str());
                socket.set_nonblocking(false);
                socket
            })
            .collect()
    }
    fn dispatch(&self, mut conn: TcpStream) {
        conn.set_nonblocking(false);
        loop {
            let command = match Message::receive_msg(&mut conn) {
                Ok(msg) => match msg.get_query() {
                    Ok(cmd) => cmd,
                    _ => return,
                },
                Err(_) => return,
            };
            let cmd = command.split_ascii_whitespace().collect::<Vec<_>>();
            if cmd[0].eq_ignore_ascii_case("sync") {
                self.sync();
            } else if cmd.len() != 3 {
                // send_string(&mut conn, "What do you want??").unwrap();
                Message::Status(Status::BadCommand).send_msg_to(&mut conn);
            }
            if cmd[0].eq_ignore_ascii_case("create") && cmd[1].eq_ignore_ascii_case("db") {
                match self.catalog.borrow_mut().create_database(cmd[2]) {
                    // Ok(()) => send_string(
                    //     &mut conn,
                    //     &format!("Database {} created successfully", cmd[2]),
                    // )
                    // .unwrap(),
                    // Err(s) => send_string(&mut conn, &s).unwrap()
                    Ok(()) => Message::Status(Status::DatabaseCreated(cmd[2].to_string()))
                        .send_msg_to(&mut conn)
                        .unwrap_or_default(),
                    Err(s) => Message::Status(Status::DatabaseNotCreated(cmd[2].to_string(), s))
                        .send_msg_to(&mut conn)
                        .unwrap_or_default(),
                }
            } else if cmd[0].eq_ignore_ascii_case("connect") && cmd[1].eq_ignore_ascii_case("db") {
                let has_db = self.catalog.borrow().has_db(cmd[2]);
                if has_db {
                    // send_string(
                    //     &mut conn,
                    //     &format!("Now Connected to Database {} successfully", cmd[2]),
                    // );
                    Message::Status(Status::DatabaseConnection(cmd[2].to_string()))
                        .send_msg_to(&mut conn)
                        .unwrap_or_default();
                    let mut db_instance = DatabaseInstance::new(
                        cmd[2],
                        self.storage.clone(),
                        self.catalog.clone(),
                        conn.try_clone().unwrap(),
                    );
                    db_instance.handle_connection();
                    db_instance.flush_everything();
                } else {
                    // send_string(&mut conn, "WOTT");
                    Message::Status(Status::DatabaseNotFound(cmd[2].to_string()))
                        .send_msg_to(&mut conn)
                        .unwrap_or_default();
                }
            } else {
                // send_string(&mut conn, "What do you want??");
                // conn.write_fmt(format_args!("What do you want??")).unwrap();
                Message::Status(Status::BadCommand).send_msg_to(&mut conn);
            }
        }
    }
    fn sync(&self) {
        self.storage.borrow_mut().flush_all();
    }

    pub fn new(home_dir: &str, addr: Vec<String>) -> Self {
        let sockets = Self::create_server_sockets(&addr);
        let storage = RcRefCell!(StorageManager::new(&AQUADIR(), BLK_SIZE, BUFFER_COUNT));
        let catalog = Rc::new(RefCell::from(CatalogManager::startup(storage.clone())));
        Self {
            home_dir: home_dir.to_string(),
            sockets,
            bind_addr: addr,
            storage,
            catalog,
        }
    }
    pub fn run(&self) {
        let mut socket = self.sockets[0].incoming();
        for conn in socket {
            if let Ok(net) = conn {
                self.dispatch(net)
            }
        }
        // for socket in self.sockets.iter().cycle() {
        //     if let Ok(conn) = socket.accept() {
        //         println!("accepted");
        //         self.dispatch(conn.0);
        //     } else {
        //         sleep(Duration::from_micros(200));
        //     }
        // }
    }
}
