use aqua::interface::message::{Message, RowMap, Status};
use aqua::query::concrete_types::ConcreteType;
use aqua::schema::types::Type;
use std::collections::HashMap;
use std::io::stdin;
use std::mem;
use std::net::TcpStream;
use tabled::settings::Style;
use tabled::Table;

fn main() {
    let mut socket = TcpStream::connect("127.0.0.1:2710").unwrap();
    for line in stdin().lines() {
        if let Ok(line) = line {
            send_msg(line, &mut socket).expect("Fatal");
        } else {
            eprintln!("Could not get input");
            return;
        }
        if let Ok(response) = Message::receive_msg(&mut socket) {
            handle_msg(response, &mut socket);
        } else {
            eprintln!("Receive Message Error")
        }
    }
}

fn send_msg(line: String, mut socket: &mut TcpStream) -> std::io::Result<()> {
    let msg = Message::Query(line);
    let msg_copy = msg.clone();
    if msg.send_msg_to(&mut socket).is_err() {
        mem::swap(socket, &mut TcpStream::connect("127.0.0.1:2710").unwrap());
        msg_copy.send_msg_to(&mut socket)
    } else {
        Ok(())
    }
}

fn handle_msg(msg: Message, socket: &mut TcpStream) {
    match msg {
        Message::Status(s) => handle_status(s, socket),
        Message::FieldTypes(f) => handle_results(f, socket),
        _ => {}
    }
}

fn handle_status(status: Status, socket: &mut TcpStream) {
    match status {
        Status::DatabaseCreated(_) => {
            println!("{}", status);
            mem::swap(socket, &mut TcpStream::connect("127.0.0.1:2710").unwrap());
        }
        _ => println!("{}", status),
    }
}

fn handle_results(types: HashMap<String, Type>, socket: &mut TcpStream) {
    let keys_order = types.keys().collect::<Vec<_>>();
    let mut builder = tabled::builder::Builder::default();
    builder.set_header(types.keys());
    let mut table = builder.build();
    table.with(Style::modern().remove_bottom());
    println!("{table}");
    while let Ok(tuples) = Message::receive_msg(socket) {
        let tuples = tuples.get_results().expect("Bad Message");
        if tuples.is_empty() {
            break;
        }
        let tuples = tuples.into_iter().map(|r| decode_tuple(&types, r));
        let mut builder = tabled::builder::Builder::default();
        for row in tuples {
            builder.push_record(tuple_print_format(&keys_order, row));
        }
        let mut table = builder.build();
        table.with(Style::modern().remove_bottom().remove_top());
        println!("{table}");
    }
    let mut builder = tabled::builder::Builder::default();
    builder.set_header(types.keys());
    builder.remove_header();
    println!("{}", builder.build().with(Style::modern().remove_top()))
}

fn decode_tuple(types: &HashMap<String, Type>, tuple: RowMap) -> HashMap<String, ConcreteType> {
    tuple
        .into_iter()
        .map(|(k, v)| {
            let col = ConcreteType::from_bytes(*types.get(&k).unwrap(), &v.unwrap_or_default());
            (k, col)
        })
        .collect::<HashMap<String, ConcreteType>>()
}

fn tuple_print_format(
    keys: &Vec<&String>,
    mut tuple: HashMap<String, ConcreteType>,
) -> Vec<ConcreteType> {
    let mut row = vec![];
    for k in keys {
        row.push(tuple.remove(*k).unwrap());
    }
    row
}
