mod logic;
mod ui_msg;

use crate::logic::start_backend;
use crate::ui_msg::{DuplexChannel, UiMessage};
use aqua::interface::message::{Message, RowMap, Status};
use aqua::query::concrete_types::ConcreteType;
use aqua::schema::types::Type;
use aqua::FieldId;
use fltk::app::Scheme;
use fltk::button::Button;
use fltk::enums::{Align, Event};
use fltk::frame::Frame;
use fltk::group::{Pack, PackType};
use fltk::input::{Input, MultilineInput};
use fltk::output::{MultilineOutput, Output};
use fltk::prelude::*;
use fltk::text::{SimpleTerminal, TextEditor};
use fltk::*;
use fltk_table::{SmartTable, TableOpts};
use fltk_theme::{
    color_themes, widget_themes, ColorTheme, SchemeType, ThemeType, WidgetScheme, WidgetTheme,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::read_to_string;
use std::io::stdin;
use std::mem;
use std::net::TcpStream;
use std::rc::Rc;
use fltk::valuator::{Counter, ValueOutput};

fn main() {
    let comms = DuplexChannel::default();
    let (ui_tx, ui_rx) = fltk::app::channel::<UiControl>();
    let backend = start_backend(comms.clone());
    let app = app::App::default().with_scheme(Scheme::Oxy);
    // let widget_theme = WidgetTheme::new(ThemeType::Aero);
    // widget_theme.apply();
    // let widget_scheme = WidgetScheme::new(SchemeType::Fluent);
    // widget_scheme.apply();
    // let theme = ColorTheme::new(color_themes::);
    // theme.apply();
    let mut entry_window = window::Window::default().with_size(500, 300);
    let mut pack = Pack::new(0, 100, 300, 400, "").center_x(&entry_window);
    let mut server_conn_input = Input::new(0, -50, 400, 50, "Server")
        .with_label("Server")
        .center_y(&pack);
    let mut server_conn_btn = Button::new(0, -50, 400, 50, "Connect")
        .with_label("Connect")
        .center_y(&pack);
    pack.set_type(PackType::Vertical);
    pack.end();
    entry_window.end();
    entry_window.show();

    let mut main_window = window::Window::default().with_size(1000, 600);
    main_window.make_resizable(true);
    let mut main_pack = Pack::new(0, 50, 700, 400, "").center_x(&main_window);
    main_pack.set_spacing(50);
    let mut query_input = Rc::new(RefCell::new(
        MultilineInput::new(0, -50, 600, 200, "").center_y(&main_pack),
    ));
    let mut use_file_btn = Button::new(20,-50,20,50,"From File")
        .with_label("From File")
        .center_y(&main_pack);
    query_input.borrow_mut().set_wrap(true);
    let mut query_btn = Button::new(0, -50, 200, 50, "Send")
        .with_label("Send")
        .center_y(&main_pack);
    let mut status = MultilineOutput::new(0, -100, 700, 100, "")
        .center_y(&main_pack)
        .with_align(Align::Center);
    status.set_wrap(true);
    let mut counter = Rc::new(RefCell::new(Output::new(0,-75,50,50,"Executed")
        .center_y(&main_pack)));
    let mut  val_counter = Rc::new(RefCell::new(0));
    pack.end();
    main_window.end();

    let mut results_window = window::Window::default().with_size(1000, 1500);
    let mut results_pack = Pack::new(0, 50, 1000, 1500, "").center_x(&results_window);
    results_window.make_resizable(true);
    let mut results_table = SmartTable::default()
        .with_size(900, 900)
        .with_align(Align::Center)
        .center_of(&results_pack);
    let mut close_results_btn = Button::new(0, -50, 400, 50, "Close")
        .with_label("Close")
        .center_y(&results_pack);
    results_pack.end();
    results_window.end();

    let window1_comms = comms.clone();
    let ui_tx1 = ui_tx.clone();
    server_conn_btn.handle(move |_, ev| match ev {
        Event::Push => {
            window1_comms
                .send_ch1(UiMessage::ServerConnect(server_conn_input.value()))
                .unwrap();
            match window1_comms.recv_ch2().unwrap() {
                UiMessage::ServerConnectedSuccess => ui_tx1.send(UiControl::StartWinToMainWin),
                UiMessage::ServerConnectedFail(s) => dialog::alert_default(&s),
                _ => todo!(),
            }
            true
        }
        _ => false,
    });

    let window2_comms = comms.clone();
    let ui_tx2 = ui_tx.clone();
    let query_input2 = query_input.clone();
    let counter1 = counter.clone();
    let val_counter1 = val_counter.clone();
    query_btn.handle(move |_, ev| match ev {
        Event::Push => {
            let value = query_input2.borrow().value();
            if value.is_empty() {
                return true;
            }
            let value = value.lines();
            for v in value {
                let msg_send = UiMessage::UiRequest(v.to_string());
                window2_comms.send_ch1(msg_send).unwrap();
                match window2_comms.recv_ch2().unwrap() {
                    UiMessage::DatabaseCreated(s) | UiMessage::GenericStatus(s) => {
                        let mut count = val_counter1.borrow_mut();
                        *count +=1;
                        counter1.borrow_mut().set_value(&(count).to_string());
                        ui_tx2.send(UiControl::SetMainStatus(s));
                    }
                    UiMessage::FieldsNames(fields) => {
                        let mut count = val_counter1.borrow_mut();
                        *count +=1;
                        counter1.borrow_mut().set_value(&(count).to_string());
                        ui_tx2.send(UiControl::MainWinToResults(fields));
                    }
                    _ => (),
                }
            }
            true
        }
        _ => false,
    });

    let window2_comms2 = comms.clone();
    let counter2 = counter.clone();
    let ui_tx_file = ui_tx.clone();
    use_file_btn.handle(move |_,ev| match ev{
        Event::Push =>{
            let file = dialog::file_chooser("Choose Dump File", "*.txt\t*.sql", ".", false).unwrap_or_default();
            let contents = read_to_string(file).unwrap_or_default();
            for v in contents.lines() {
                let msg_send = UiMessage::UiRequest(v.to_string());
                window2_comms2.send_ch1(msg_send).unwrap();
                match window2_comms2.recv_ch2().unwrap() {
                    UiMessage::DatabaseCreated(s) | UiMessage::GenericStatus(s) => {
                        let mut count = val_counter.borrow_mut();
                        *count +=1;
                        counter2.borrow_mut().set_value(&(count).to_string());
                        ui_tx_file.send(UiControl::SetMainStatus(s));
                    }
                    UiMessage::FieldsNames(fields) => {
                        let mut count = val_counter.borrow_mut();
                        *count +=1;
                        counter2.borrow_mut().set_value(&(count).to_string());
                        ui_tx_file.send(UiControl::MainWinToResults(fields));
                    }
                    _ => (),
                }
            }
            true
        },
        _ => false
    });

    let ui_tx3 = ui_tx.clone();
    close_results_btn.handle(move |_, ev| match ev {
        Event::Push => {
            ui_tx3.send(UiControl::ResultsWinToMain);
            true
        }
        _ => false,
    });

    let comms4 = comms.clone();
    entry_window.set_callback(move |_| {
        if fltk::app::event() == enums::Event::Close {
            comms4.send_ch1(UiMessage::Terminate).unwrap();
            app.quit()
        }
    });

    let ui_tx4 = ui_tx.clone();
    main_window.set_callback(move |_| {
        if fltk::app::event() == enums::Event::Close {
            ui_tx4.send(UiControl::MainWinToStartWin);
        }
    });

    let ui_tx5 = ui_tx.clone();
    results_window.set_callback(move |_| {
        if fltk::app::event() == enums::Event::Close {
            ui_tx5.send(UiControl::ResultsWinToMain);
        }
    });

    while app.wait() {
        if let Some(msg) = ui_rx.recv() {
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
                }
                UiControl::ResultsWinToMain => {
                    results_table.clear();
                    results_window.hide();
                    query_input.borrow_mut().set_value("");
                    status.set_value("");
                    main_window.show();
                }
                UiControl::SetMainStatus(s) => status.set_value(&s),
                // UiControl::CloseUi => {
                //     main_window.hide();
                //     comms.send_ch1(UiMessage::Terminate).unwrap();
                //     app.quit();
                //     break
                // }
                UiControl::MainWinToStartWin => {
                    comms.send_ch1(UiMessage::BackToStart).unwrap();
                    query_input.borrow_mut().set_value("");
                    status.set_value("");
                    main_window.hide();
                    entry_window.show();
                }
            }
        }
        if results_window.shown() {
            if let Ok(msg) = comms.try_recv_ch2() {
                if let UiMessage::ResultSet(results) = msg {
                    append_results_table(&mut results_table, results);
                } else if let UiMessage::ResultsFinished = msg {
                    close_results_btn.activate();
                }
            }
        }
    }
    backend.join();
}

fn set_results_table(table: &mut SmartTable, fields: &Vec<String>) {
    table.set_opts(TableOpts {
        cols: fields.len() as i32,
        rows: 0,
        editable: false,
        ..Default::default()
    });
    for (i, field) in fields.iter().enumerate() {
        table.set_col_header_value(i as i32, field)
    }
}
fn append_results_table(table: &mut SmartTable, tuples: Vec<Vec<String>>) {
    for t in tuples {
        let t: Vec<_> = t.iter().map(String::as_str).collect();
        table.append_row(&(table.row_count() + 1).to_string(), &t);
    }
}

enum UiControl {
    StartWinToMainWin,
    MainWinToResults(Vec<String>),
    ResultsWinToMain,
    SetMainStatus(String),
    MainWinToStartWin,
    // CloseUi
}
