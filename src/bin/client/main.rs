mod ui_msg;
mod logic;

use std::cell::RefCell;
use aqua::interface::message::{Message, RowMap, Status};
use aqua::query::concrete_types::ConcreteType;
use aqua::schema::types::Type;
use std::collections::HashMap;
use std::io::stdin;
use std::mem;
use std::net::TcpStream;
use std::rc::Rc;
use fltk::*;
use fltk::button::Button;
use fltk::enums::Event;
use fltk::group::{Pack, PackType};
use fltk::input::{Input, MultilineInput};
use fltk::output::Output;
use fltk::prelude::*;
use fltk::text::{SimpleTerminal, TextEditor};
use fltk_table::{SmartTable, TableOpts};
use tabled::settings::Style;
use tabled::Table;
use aqua::FieldId;
use crate::logic::start_backend;
use crate::ui_msg::{DuplexChannel, UiMessage};

fn main() {
    let comms = DuplexChannel::default();
    let (ui_tx,ui_rx) = fltk::app::channel::<UiControl>();
    let backend = start_backend(comms.clone());
    let app = app::App::default().with_scheme(app::Scheme::Gtk);
    let mut entry_window = window::Window::default().with_size(500, 300);
    let mut pack = Pack::new(0,100,300,400,"").center_x(&entry_window);
    let mut server_conn_input = Input::new(0,-50,400,50,"Server").with_label("Server").center_y(&pack);
    let mut server_conn_btn = Button::new(0,-50,400,50,"Connect").with_label("Connect").center_y(&pack);
    pack.set_type(PackType::Vertical);
    pack.end();
    entry_window.end();
    entry_window.show();

    let mut main_window = window::Window::default().with_size(1000, 600);
    let mut main_pack = Pack::new(0,50,300,400,"").center_x(&main_window);
    main_pack.set_spacing(50);
    let mut query_input = Rc::new(RefCell::new(MultilineInput::new(0,-50,600,300,"").center_y(&main_pack)));
    query_input.borrow_mut().set_wrap(true);
    let mut query_btn = Button::new(0,-50,400,50,"Send").with_label("Send").center_y(&main_pack);
    let mut status = Output::new(0,-100,400,50,"").center_y(&main_pack);
    pack.end();
    main_window.end();


    let mut results_window = window::Window::default().with_size(1000, 1500);
    let mut results_pack = Pack::new(0,50,1000,1500,"").center_x(&results_window);
    results_window.make_resizable(true);
    let mut results_table = SmartTable::default()
        .with_size(900, 900)
        .center_of(&results_window);
    let mut close_results_btn = Button::new(0, -50, 400, 50, "Close").with_label("Close").center_y(&results_pack);
    results_pack.end();
    results_window.end();

    let window1_comms = comms.clone();
    let ui_tx1 = ui_tx.clone();
    server_conn_btn.handle(move |_,ev| match ev{
        Event::Push => {
            window1_comms.send_ch1(UiMessage::ServerConnect(server_conn_input.value())).unwrap();
            match window1_comms.recv_ch2().unwrap(){
                UiMessage::ServerConnectedSuccess => {
                    ui_tx1.send(UiControl::StartWinToMainWin)
                },
                UiMessage::ServerConnectedFail(s) => dialog::alert_default(&s),
                _ => todo!()
            }
            true
        },
        _ => false
    });

    let window2_comms = comms.clone();
    let ui_tx2 = ui_tx.clone();
    let query_input2 = query_input.clone();
    query_btn.handle( move |_,ev| match ev {
        Event::Push => {
            let msg_send = UiMessage::UiRequest(query_input2.borrow().value());
            window2_comms.send_ch1(msg_send).unwrap();
            match window2_comms.recv_ch2().unwrap() {
                UiMessage::DatabaseCreated(s) | UiMessage::GenericStatus(s) => ui_tx2.send(UiControl::SetMainStatus(s)) ,
                UiMessage::FieldsNames(fields) => {
                    ui_tx2.send(UiControl::MainWinToResults(fields));

                },
                _ => todo!()
            }
            true
        },
        _ => false
    });

    let ui_tx3 = ui_tx.clone();
    close_results_btn.handle(move |_,ev| match ev{
        Event::Push =>{
            ui_tx3.send(UiControl::ResultsWinToMain);
            true
        },
        _ => false
    });



    while app.wait(){
        if let Some(msg) =  ui_rx.recv(){
            match msg {
                UiControl::StartWinToMainWin => {
                    entry_window.hide();
                    main_window.show();
                }
                UiControl::MainWinToResults(fields) => {
                    main_window.hide();
                    close_results_btn.deactivate();
                    results_window.show();
                    set_results_table(&mut results_table, &fields);
                },
                UiControl::ResultsWinToMain =>{
                    results_table.clear();
                    results_window.hide();
                    query_input.borrow_mut().set_value("");
                    status.set_value("");
                    main_window.show();

                }
                UiControl::SetMainStatus(s) => status.set_value(&s),
            }
        }
        if results_window.shown(){
            if let Ok(msg) = comms.try_recv_ch2(){
                if let UiMessage::ResultSet(results) = msg{
                    append_results_table(&mut results_table,results);
                }
                else if let UiMessage::ResultsFinished = msg {
                    close_results_btn.activate();
                }
            }
        }
    }
    backend.join();



}

fn set_results_table(table:&mut SmartTable,fields:&Vec<String>){
    table.set_opts(
        TableOpts{
            cols : fields.len() as i32,
            rows : 0,
            editable : false,
            ..Default::default()
        }
    );
    for (i,field) in fields.iter().enumerate(){
        table.set_col_header_value(i as i32,field)

    }
}
fn append_results_table(table:&mut SmartTable,tuples:Vec<Vec<String>>){
    for t in tuples{
        let t :Vec<_> = t.iter().map(String::as_str).collect();
        table.append_row(&(table.row_count() + 1).to_string(),&t);
    }
}

enum UiControl{
    StartWinToMainWin,
    MainWinToResults(Vec<String>),
    ResultsWinToMain,
    SetMainStatus(String),
}