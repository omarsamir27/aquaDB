use crate::ui_msg::{self, UiMessage};
// use crate::{send_msg};
use aqua::common::net::send_string;
use aqua::interface::message::*;
use aqua::query::concrete_types::ConcreteType;
use aqua::schema::types::Type;
use aqua::FieldId;
use std::collections::HashMap;
use std::net::TcpStream;
use std::thread::JoinHandle;
use std::{mem, thread};

type DuplexChannel = ui_msg::DuplexChannel<UiMessage>;

pub fn start_backend(entry_window_channel: DuplexChannel) -> JoinHandle<()> {
    thread::spawn(move || loop {
        if let Ok(msg) = entry_window_channel.recv_ch1() {
            if msg == UiMessage::Terminate {
                break;
            }
            if let Some(connect_string) = msg.get_server_connect() {
                if let Some(mut socket) = handle_connect(&entry_window_channel, connect_string) {
                    loop {
                        if let Ok(msg) = entry_window_channel.recv_ch1() {
                            if msg == UiMessage::BackToStart {
                                break;
                            }
                            let ui_req = msg.get_ui_request().unwrap();
                            let msg = Message::Query(ui_req.to_string());
                            msg.send_msg_to(&mut socket).unwrap();
                            if let Ok(server_recv) = Message::receive_msg(&mut socket) {
                                handle_msg(&entry_window_channel, server_recv, &mut socket);
                            }
                        }
                    }
                } else {
                    continue;
                }
            }
        } else {
            break;
        }
    })
}

fn handle_connect(channel: &DuplexChannel, connect_string: &str) -> Option<TcpStream> {
    if let Ok(socket) = TcpStream::connect(connect_string) {
        channel.send_ch2(UiMessage::ServerConnectedSuccess).unwrap();
        Some(socket)
    } else {
        channel
            .send_ch2(UiMessage::ServerConnectedFail(String::from(
                "Server Connect Fail",
            )))
            .unwrap();
        None
    }
}

fn handle_msg(channel: &DuplexChannel, msg: Message, socket: &mut TcpStream) {
    match msg {
        Message::Status(s) => handle_status(channel, s, socket),
        Message::FieldTypes(f) => handle_results(channel, f, socket),
        _ => {}
    }
}

fn handle_status(channel: &DuplexChannel, status: Status, socket: &mut TcpStream) {
    match status {
        Status::DatabaseCreated(_) => {
            channel
                .send_ch2(UiMessage::DatabaseCreated(status.to_string()))
                .unwrap();
            // mem::swap(
            //     socket,
            //     &mut TcpStream::connect(socket.local_addr().unwrap()).unwrap(),
            // );
        }
        _ => channel
            .send_ch2(UiMessage::GenericStatus(status.to_string()))
            .unwrap(),
    }
}

fn handle_results(channel: &DuplexChannel, types: HashMap<FieldId, Type>, socket: &mut TcpStream) {
    let keys_order = types.keys().collect::<Vec<_>>();
    let field_names_msg =
        UiMessage::FieldsNames(keys_order.iter().map(|f| f.to_string()).collect());
    channel.send_ch2(field_names_msg).unwrap();
    while let Ok(msg) = Message::receive_msg(socket) {
        if let Message::Results(tuples) = msg {
            let tuples = tuples
                .into_iter()
                .map(|r| tuple_print_format(&keys_order, decode_tuple(&types, r)))
                .collect();
            let result_set = UiMessage::ResultSet(tuples);
            channel.send_ch2(result_set).unwrap();
        } else if matches!(msg, Message::Status(Status::ResultsFinished)) {
            channel.send_ch2(UiMessage::ResultsFinished).unwrap();
            break;
        }
    }
}

fn decode_tuple(types: &HashMap<FieldId, Type>, tuple: RowMap) -> HashMap<FieldId, ConcreteType> {
    tuple
        .into_iter()
        .map(|(k, v)| {
            let col = ConcreteType::from_bytes(*types.get(&k).unwrap(), &v.unwrap_or_default());
            (k, col)
        })
        .collect::<HashMap<FieldId, ConcreteType>>()
}

fn tuple_print_format(
    keys: &Vec<&FieldId>,
    mut tuple: HashMap<FieldId, ConcreteType>,
) -> Vec<String> {
    let mut row = vec![];
    for k in keys {
        row.push(tuple.remove(*k).unwrap().to_string());
    }
    row
}
