use std::fmt::{Debug, Formatter, Pointer};
use positioned_io2::WriteAt;

pub struct Page {
    pub payload: Vec<u8>,
}

impl Page {
    pub fn new(page_size: usize) -> Self {
        Page {
            payload: vec![0; page_size],
        }
    }

    pub fn write_bytes(&mut self, data: &[u8], offset: u64) {
        self.payload.write_at(offset, data).unwrap();
    }

    fn format(&mut self) {}
}

impl Debug for Page {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}