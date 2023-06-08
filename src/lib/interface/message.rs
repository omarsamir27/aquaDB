// pub struct Message{
//
// }

use crate::schema::types::Type;
use bincode::config::Configuration;
use bincode::{Decode, Encode};
use std::collections::HashMap;
use std::fmt::{write, Display, Formatter};
use std::io::{Read, Write};
use std::net::TcpStream;
use crate::FieldId;

pub type RowMap = HashMap<FieldId, Option<Vec<u8>>>;
const CONFIG: Configuration = bincode::config::standard();

#[derive(Decode, Encode, Clone)]
pub enum Message {
    Query(String),
    Status(Status),
    Results(Vec<RowMap>),
    FieldTypes(HashMap<FieldId, Type>),
}
impl Message {
    pub fn receive_msg(conn: &mut TcpStream) -> Result<Self, ()> {
        let mut len = [0_u8; 8];
        if conn.read_exact(&mut len).is_err() {
            return Err(());
        }
        let len = u64::from_ne_bytes(len);
        let mut msg = vec![0_u8; len as usize];
        if conn.read_exact(&mut msg).is_err() {
            return Err(());
        }
        bincode::decode_from_slice(msg.as_slice(), CONFIG).map_or(Err(()), |(msg, size)| Ok(msg))
    }
    pub fn send_msg_to(self, conn: &mut TcpStream) -> std::io::Result<()> {
        let mut msg = bincode::encode_to_vec(self, CONFIG).unwrap();
        let mut len = msg.len().to_ne_bytes().to_vec();
        len.append(&mut msg);
        conn.write_all(&len)
    }
    pub fn get_query(&self) -> Result<String, ()> {
        match self {
            Message::Query(s) => Ok(s.clone()),
            _ => Err(()),
        }
    }
    pub fn get_status(&self) -> Result<Status, ()> {
        match self {
            Message::Status(s) => Ok(s.clone()),
            _ => Err(()),
        }
    }
    pub fn get_results(&self) -> Result<Vec<RowMap>, ()> {
        match self {
            Message::Results(res) => Ok(res.clone()),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Decode, Encode)]
pub enum Status {
    DatabaseNotFound(String),
    DatabaseConnection(String),
    DatabaseCreated(String),
    DatabaseNotCreated(String, String),
    TableCreated(String),
    TableNotCreated(String, String),
    RecordInserted,
    RecordNotInserted(String),
    BadCommand,
    Generic(String),
    ResultsFinished
}

impl Display for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::DatabaseNotFound(db) => write!(f, "Database {} not found", db),
            Status::DatabaseConnection(db) => write!(f, "Now connected to Database {}", db),
            Status::DatabaseCreated(db) => write!(f, "Database {} created successfully", db),
            Status::DatabaseNotCreated(s1, s2) => {
                write!(f, "Could not create Database {} : {}", s1, s2)
            }
            Status::TableCreated(s) => write!(f, "Table {} created successfully", s),
            Status::TableNotCreated(s1, s2) => write!(f, "Could not create table {} : {}", s1, s2),
            Status::RecordInserted => write!(f, "Record Inserted Successfully"),
            Status::RecordNotInserted(s) => write!(f, "Record Insertion Failed: {}", s),
            Status::BadCommand => write!(f, "Command not found"),
            Status::Generic(s) => write!(f, "{}", s),
            Status::ResultsFinished => write!(f, "")
        }
    }
}
