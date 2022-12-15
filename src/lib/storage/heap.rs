// use std::borrow::BorrowMut;
use crate::common::numerical::ByteMagic;
use crate::schema::null_bitmap::NullBitMap;
use crate::schema::schema::Layout;
use crate::storage::blockid::BlockId;
use crate::storage::buffermgr::FrameRef;
use positioned_io2::{Size, WriteAt};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;
use crate::storage::page::Page;
use crate::storage::tuple::Tuple;

/// An Entity that is owned by a certain Heap Page that encapsulates the byte that indicates the
/// start and the end of the free space inside a Heap Page
#[derive(Debug, PartialEq)]
pub struct PageHeader {
    pub space_start: usize,
    pub space_end: usize,
}

impl PageHeader {
    /// Read the first 4 bytes of a Heap Page 2 by 2 to assign them to the start and end attributes
    fn new(payload: &[u8]) -> Self {
        Self {
            space_start: payload.extract_u16(0) as usize,
            space_end: payload.extract_u16(2) as usize,
        }
    }
}

/// An Entity owned by a Heap Page multiple different times each pointing to the offset
/// of a corresponding tuple inside this Heap Page and the number of bytes needed for that tuple
/// to be stored
#[derive(Clone, Debug)]
struct TuplePointer {
    offset: usize,
    size: u16,
}

impl TuplePointer {
    pub fn new(offset: usize, size: u16) -> Self {
        Self { offset, size }
    }

    /// Reads a tuple pointer pointing to a tuple where the offset of the beginning of that
    /// Tuple Pointer is passed as an argument, then returns this Tuple Pointer
    fn read_pointer(payload: &[u8], offset: usize) -> TuplePointer {
        let tuple_offset = payload.extract_u16(offset);
        let size = payload.extract_u16(offset + 2);
        TuplePointer {
            offset: tuple_offset as usize,
            size,
        }
    }

    /// Converts the Tuple Pointer attributes into bytes and returns a vector of these bytes
    fn to_bytes(self) -> Vec<u8> {
        let mut tuple_pointer = Vec::new();
        let offset_bytes = (self.offset as u16).to_ne_bytes();
        let size_bytes = (self.size).to_ne_bytes();
        tuple_pointer.extend(offset_bytes);
        tuple_pointer.extend(size_bytes);
        tuple_pointer
    }
}

/// HeapPage is
#[derive(Debug)]
pub struct HeapPage {
    //tx_id,layout,schema
    blk: BlockId,
    pub frame: FrameRef,
    pub header: PageHeader,
    tuple_pointers: Vec<TuplePointer>,
    layout: Rc<Layout>,
    vacuuming: bool,
}

impl HeapPage {
    /// Creates a new Heap Page given a reference of a frame in the memory, a block id and the layout
    /// of the tuples inside the block.
    ///
    /// The function reads the page header from the payload of the page, reads tuple pointers and push
    /// them into the tuple pointers vector
    pub fn new(frame: FrameRef, blk: &BlockId, layout: Rc<Layout>) -> Self {
        let blk = blk.clone();
        let heap_frame = frame.clone();
        let mut frame_ref = heap_frame.borrow_mut();
        frame_ref.update_replace_stats();
        let header = PageHeader::new(frame_ref.page.payload.as_slice());
        let mut current_offset = 4_usize;
        let mut tuple_pointers: Vec<TuplePointer> = Vec::new();
        if header.space_start != header.space_end {
            while current_offset != header.space_start {
                let pointer =
                    TuplePointer::read_pointer(frame_ref.page.payload.as_slice(), current_offset);
                tuple_pointers.push(pointer);
                current_offset += 4;
            }
        }
        drop(frame_ref);
        Self {
            blk,
            frame: heap_frame,
            header,
            tuple_pointers,
            layout,
            vacuuming: false,
        }
    }

    /// A helper function called by get_field and get_multiple_fields
    ///
    /// Extracts the bytes of a certain field from the tuple given its name and the null bitmap of
    /// the tuple containing the required field
    #[inline(always)]
    fn extract_field_from_tuple(&self, field_name: &str, tuple: &[u8], mut bitmap: NullBitMap) -> Option<Vec<u8>> {
        let bitmap_len = bitmap.bitmap().len();
        let field_index = *self.layout.index_map().get(field_name).unwrap() as usize;
        // the field is already null from the bitmap
        if bitmap.is_null(field_index) {
            return None;
        }
        let (field_type, mut start_byte) = self.layout.field_data(field_name);
        let name_map = self.layout.name_map();
        // subtracting the null sizes of null fields from the start byte
        for bit in 0..field_index {
            start_byte -= (bitmap.get_bit(bit)
                * (self
                .layout
                .field_data(name_map.get(&(bit as u8)).unwrap())
                .0
                .unit_size()
                .unwrap()) as u8) as u16;
        }
        Some(
            field_type
                .read_from_tuple(&tuple[1 + bitmap_len..], start_byte)
                .to_vec(),
        )
    }

    /// Returns Some bytes of the required field if not Null
    /// Else it returns None
    ///
    /// Uses the function extract_field_from_tuple to do the algorithm of physically getting the
    /// field bytes
    pub fn get_field(&self, field_name: &str, index: u16) -> Option<Vec<u8>> {
        let mut bitmap = NullBitMap::new(self.layout.clone());
        let pointer = &self.tuple_pointers[index as usize];
        let mut frame = self.frame.borrow_mut();
        frame.update_replace_stats();
        let tuple = &frame.page.payload[pointer.offset..(pointer.offset + pointer.size as usize)];
        let bitmap_len = bitmap.bitmap().len();
        bitmap.read_bitmap(&tuple[1..(bitmap_len + 1)]);
        self.extract_field_from_tuple(field_name, tuple, bitmap.clone())
    }

    /// Returns a vector of Some bytes of the required fields if not Null
    ///
    /// Uses the function extract_field_from_tuple to do the algorithm of physically getting the
    /// fields bytes
    pub fn get_multiple_fields(&self, field_names: Vec<String>, index: u16) -> Vec<Option<Vec<u8>>> {
        let mut bitmap = NullBitMap::new(self.layout.clone());
        let pointer = &self.tuple_pointers[index as usize];
        let mut frame = self.frame.borrow_mut();
        frame.update_replace_stats();
        let tuple = &frame.page.payload[pointer.offset..(pointer.offset + pointer.size as usize)];
        let bitmap_len = bitmap.bitmap().len();
        bitmap.read_bitmap(&tuple[1..(bitmap_len + 1)]);
        let mut fields = Vec::new();
        for field_name in field_names {
            fields.push(self.extract_field_from_tuple(field_name.as_str(), tuple.clone(), bitmap.clone()));
        }
        fields
    }

    /// Virtually deleting a tuple in a specific slot inside the page by setting the deleted byte
    pub fn mark_delete(&self, slot_num: usize) {
        let pointer = &self.tuple_pointers[slot_num];
        let mut frame = self.frame.borrow_mut();
        frame.update_replace_stats();
        let offset = pointer.offset;
        frame.page.payload[offset] = 1_u8;
    }

    /// Returns a vector of bytes containing the tuple that exists at a specific slot inside the page
    /// as bytes
    pub fn get_tuple(&self, slot_num: usize) -> Vec<u8> {
        let pointer = &self.tuple_pointers[slot_num];
        let mut frame = self.frame.borrow_mut();
        if !self.vacuuming {
            frame.update_replace_stats();
        }
        let offset = pointer.offset;
        let size = pointer.size;
        frame.page.payload[offset..(offset + size as usize)].to_vec()
    }

    /// A helper function used by new_from_empty to create an empty HeapPage
    fn init_heap(frame: &FrameRef) {
        let mut frame = frame.borrow_mut();
        let header = [
            4_u16.to_ne_bytes(),
            ((frame.page.payload.len()) as u16).to_ne_bytes(),
        ]
        .concat();
        frame.update_replace_stats();
        frame.page.write_bytes(header.as_slice(), 0);
    }

    /// Creates an empty Heap Page and returns it
    pub fn new_from_empty(frame: FrameRef, blk: &BlockId, layout: Rc<Layout>) -> Self {
        HeapPage::init_heap(&frame);
        HeapPage::new(frame, blk, layout)
    }

    /// Checks the state of a specific tuple given its tuple pointer whether it exists or
    /// is vacuumed or deleted
    pub fn pointer_and_tuple_exist(&self, tuple_pointer: usize) -> (bool, bool) {
        match self.tuple_pointers.get(tuple_pointer) {
            None => (false, false),
            Some(tuple) => (true, tuple.size != 0),
        }
    }

    /// Returns the number of tuple pointers inside a Heap Page
    pub fn pointer_count(&self) -> usize {
        self.tuple_pointers.len()
    }

    /// Inserting a given tuple inside a Heap Page
    ///
    /// Ask the free map at which page to put the tuple
    /// Search for an empty tuple pointer and modify it
    /// If there is no empty tuple pointers, add a new tuple pointer
    /// Write the tuple at its correct position
    pub fn insert_tuple(&mut self, tuple: Tuple) {
        let tuple_size = tuple.tuple_size();
        let pointer_pos = self
            .tuple_pointers
            .iter_mut()
            .position(|pointer| pointer.size == 0);
        let (tuple_pointer_bytes, index) = if pointer_pos.is_some() {
            let tuple_pointer = self.tuple_pointers.get_mut(pointer_pos.unwrap()).unwrap();
            tuple_pointer.offset = self.header.space_end - tuple_size as usize;
            tuple_pointer.size = tuple_size;
            self.header.space_end = tuple_pointer.offset;
            (tuple_pointer.clone().to_bytes(), pointer_pos.unwrap())
        } else {
            let mut tuple_pointer =
                TuplePointer::new(self.header.space_end - tuple_size as usize, tuple_size);
            self.header.space_start += 4;
            self.header.space_end = tuple_pointer.offset;
            self.tuple_pointers.push(tuple_pointer.clone());
            (tuple_pointer.to_bytes(), self.tuple_pointers.len())
        };
        let mut borrowed_frame = self.frame.borrow_mut();
        borrowed_frame.update_replace_stats();
        borrowed_frame.write_at(tuple_pointer_bytes.as_slice(), (index * 4) as u64);
        borrowed_frame.write_at(tuple.to_bytes().as_slice(), self.header.space_end as u64);
        borrowed_frame.write_at((self.header.space_start as u16).to_ne_bytes().as_slice(), 0);
        borrowed_frame.write_at((self.header.space_end as u16).to_ne_bytes().as_slice(), 2);
    }

    /// Calculates the number of free bytes inside a Heap Page to be stored in Free Space Map
    pub fn free_space(&self) -> u16{
        (self.header.space_end - self.header.space_start) as u16
    }

    /// Compacts the page by removing the fragmentation and virtually deleted tuples
    ///
    /// Creates a new empty Heap Page
    /// Loops over the tuple pointers of the required Heap Page to get vacuumed
    /// If the tuple pointer points to a deleted tuple, it writes the tuple pointer only to the new
    /// heap page without any tuple data
    /// If the tuple pointer points to an existing tuple, it writes the tuple to the new offset
    /// Calculates the new space end of the Heap Page
    pub fn vacuum(&mut self) {
        self.vacuuming = true;
        let mut new_page = Page::new(4096);
        let mut space_start = 4_u16;
        let mut space_end = 4096_u16;
        for mut tuple_pointer_index in 0..self.tuple_pointers.len() {
            let tuple = self.get_tuple(tuple_pointer_index);
            if tuple[0] == 1 {
                self.tuple_pointers[tuple_pointer_index].offset = 0;
                self.tuple_pointers[tuple_pointer_index].size = 0;
                new_page.write_bytes(
                    self.tuple_pointers[tuple_pointer_index]
                        .clone()
                        .to_bytes()
                        .as_slice(),
                    space_start as u64,
                );
                space_start += 4;
            } else {
                let tuple_len = tuple.len() as u16;
                new_page.write_bytes(tuple.as_slice(), (space_end - tuple_len) as u64);
                self.tuple_pointers[tuple_pointer_index].offset = (space_end - tuple_len) as usize;
                new_page.write_bytes(
                    self.tuple_pointers[tuple_pointer_index]
                        .clone()
                        .to_bytes()
                        .as_slice(),
                    space_start as u64,
                );
                space_start += 4;
                space_end -= tuple_len;
            }
        }
        new_page.write_bytes(space_start.to_ne_bytes().as_slice(), 0);
        new_page.write_bytes(space_end.to_ne_bytes().as_slice(), 2);
        self.frame
            .borrow_mut()
            .page
            .write_bytes(new_page.payload.as_slice(), 0);
        self.header.space_start = space_start as usize;
        self.header.space_end = space_end as usize;
        self.vacuuming = false;
    }

    /// Returns an initialized iterator over the Heap Page of index 0
    pub fn page_iter(&self) -> PageIter {
        PageIter {
            current_slot: 0,
            page: &self,
        }
    }
}

/// An iterator over the Heap Page slots containing the current slot inside a page pointed to
pub struct PageIter<'page> {
    current_slot: u16,
    page: &'page HeapPage,
}

impl<'page> PageIter<'page> {
    /// Returns the next tuple inside a Heap Page if exists
    /// Else returns None
    pub fn next(&mut self) -> Option<Vec<u8>> {
        if self.current_slot == (self.page.tuple_pointers.len() - 1) as u16 {
            return None;
        }
        while self.current_slot != self.page.tuple_pointers.len() as u16 {
            if self.page.tuple_pointers[self.current_slot as usize].size != 0 {
                let tuple = Some(self.page.get_tuple(self.current_slot as usize));
                self.current_slot += 1;
                return tuple;
            }
            self.current_slot += 1;
        }
        None
    }

    /// Checks whether there is a next tuple inside the Heap Page
    pub fn has_next(&self) -> bool {
        self.current_slot != (self.page.tuple_pointers.len() - 1) as u16
    }
}
