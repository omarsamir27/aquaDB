use std::fmt::Display;
use std::net::{Incoming, IpAddr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::thread::sleep;
use std::time::Duration;

pub struct DatabaseServer {
    home_dir: String,
    bind_addr: Vec<String>,
    sockets: Vec<TcpListener>,
}

impl DatabaseServer {
    pub fn new(home_dir: &str, addr: Vec<String>) -> Self {
        let sockets = addr
            .iter()
            .map(|ip| {
                let socket =
                    TcpListener::bind(ip).expect(format!("Could not bind to {ip}").as_str());
                socket.set_nonblocking(true);
                socket
            })
            .collect();
        Self {
            home_dir: home_dir.to_string(),
            sockets,
            bind_addr: addr,
        }
    }
    pub fn run(&self) {
        // let mut listener: Vec<Incoming> = self.sockets.iter().map(|socket| {
        //     socket.set_nonblocking(true);
        //     socket.incoming()
        // }).collect();
        for socket in self.sockets.iter().cycle() {
            if let Ok(conn) = socket.accept() {
                println!("ON{}", conn.1);
            } else {
                sleep(Duration::from_micros(200));
            }
        }
    }
}
