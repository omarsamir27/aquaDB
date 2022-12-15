use positioned_io2::WriteAt;
use std::fmt::{Debug, Formatter, Pointer};

/// A container for disk block to be stored in memory.
///
/// Representation of a disk block inside memory.
pub struct Page {
    pub payload: Vec<u8>,
}

impl Page {
    /// Creates an instance of the page with zeros written as the default value
    ///
    /// We can think of this as allocating a space in memory to store a disk block.
    pub fn new(page_size: usize) -> Self {
        Page {
            payload: vec![0; page_size],
        }
    }

    /// Writes bytes to the page
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
