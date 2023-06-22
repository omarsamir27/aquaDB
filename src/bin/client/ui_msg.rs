// use std::sync::mpsc::{Sender, Receiver, ecvError, SendError, channel};
use crossbeam::channel::{bounded, Receiver, RecvError, Sender, SendError, TryRecvError};
pub enum UiMessage{
    ServerConnect(String),
    ServerConnectedSuccess,
    ServerConnectedFail(String),
    UiRequest(String),
    DatabaseCreated(String),
    GenericStatus(String),
    FieldsNames(Vec<String>),
    ResultSet(Vec<Vec<String>>),
    ResultsFinished
}
impl UiMessage{
    pub fn get_server_connect(&self) -> Option<&str>{
        if let Self::ServerConnect(s) = self{
            Some(s.as_str())
        }else { None }
    }
    pub fn get_ui_request(&self) -> Option<&str>{
        if let Self::UiRequest(s) = self{
            Some(s.as_str())
        }else { None }
    }
}

pub struct DuplexChannel<T>{
    ch1 : (Sender<T>,Receiver<T>),
    ch2 : (Sender<T>,Receiver<T>)
}

impl<T> DuplexChannel<T> {
    /// Ui thread sends on ch1 sender and Logic thread receives on it
    pub fn send_ch1(&self, msg:T) -> Result<(), SendError<T>> {
        self.ch1.0.send(msg)
    }
    /// Logic thread sends on ch2 sender and Ui thread receives on it
    pub fn send_ch2(&self, msg:T) -> Result<(), SendError<T>> {
        self.ch2.0.send(msg)
    }

    /// Logic thread receives on ch1 receiver and Ui thread sends on it
    pub fn recv_ch1(&self) -> Result<T, RecvError> {
        self.ch1.1.recv()
    }
    
    pub fn try_recv_ch1(&self) -> Result<T, TryRecvError> {
        self.ch1.1.try_recv()
    }
    pub fn try_recv_ch2(&self) -> Result<T, TryRecvError> {
        self.ch2.1.try_recv()
    }

    /// Ui thread receives on ch2 receiver and Logic thread sends on it
    pub fn recv_ch2(&self) -> Result<T, RecvError> {
        self.ch2.1.recv()
    }
}

impl<T> Default for DuplexChannel<T>{
    fn default() -> Self {
        Self{
            ch1 : bounded(5),
            ch2 : bounded(5)
        }
    }
}

impl<T> Clone for DuplexChannel<T>{
    fn clone(&self) -> Self {
        Self{
            ch1: (self.ch1.0.clone(),self.ch1.1.clone()),
            ch2: (self.ch2.0.clone(),self.ch2.1.clone())
        }
    }
}