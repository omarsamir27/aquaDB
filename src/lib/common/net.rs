use std::io::{Error, ErrorKind, Read, Write};
use std::net::TcpStream;

pub fn send_string(conn: &mut TcpStream, s: &str) -> std::io::Result<()> {
    let msg_len = s.len().to_be_bytes().to_vec();
    conn.write_all(&msg_len);
    conn.write_all(s.as_bytes())
}

pub fn receive_string(conn: &mut TcpStream) -> Result<String, Error> {
    let mut msg_len = [0; 8];
    if let Err(e) = conn.read_exact(&mut msg_len) {
        Err(e)
    } else {
        let msg_len = usize::from_be_bytes(msg_len);
        let mut msg = vec![0; msg_len];
        if let Err(e) = conn.read_exact(&mut msg) {
            Err(e)
        } else {
            Ok(String::from_utf8(msg).unwrap())
        }
    }
}
