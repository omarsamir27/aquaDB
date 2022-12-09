// use std::borrow::BorrowMut;
use crate::common::numerical::ByteMagic;
use crate::schema::schema::Layout;
use crate::storage::blockid::BlockId;
use crate::storage::buffermgr::FrameRef;
use positioned_io2::{Size, WriteAt};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;
// use crate::storage::frame::Frame;
use crate::storage::page::Page;
use crate::storage::tuple::Tuple;

// #[cfg(target_pointer_width = "32")]
// const USIZE_LENGTH :usize = 4 ;
//
// #[cfg(target_pointer_width = "64")]
// const USIZE_LENGTH :usize = 4 ;

struct PageHeader {
    space_start: usize,
    space_end: usize,
}

impl PageHeader {
    fn new(payload: &[u8]) -> Self {
        Self {
            space_start: payload.extract_u16(0) as usize,
            space_end: payload.extract_u16(2) as usize,
        }
    }
}

#[derive(Clone)]
struct TuplePointer {
    offset: usize,
    size: u16,
}

impl TuplePointer {
    pub fn new(offset: usize, size: u16) -> Self {
        Self { offset, size }
    }

    fn read_pointer(payload: &[u8], offset: usize) -> TuplePointer {
        let tuple_offset = payload.extract_u16(offset);
        let size = payload.extract_u16(offset + 2);
        TuplePointer {
            offset: tuple_offset as usize,
            size,
        }
    }

    fn to_bytes(self) -> Vec<u8> {
        let mut tuple_pointer = Vec::new();
        let offset_bytes = (self.offset as u16).to_ne_bytes();
        let size_bytes = (self.size).to_ne_bytes();
        tuple_pointer.extend(offset_bytes);
        tuple_pointer.extend(size_bytes);
        tuple_pointer
    }
}

pub struct HeapPage {
    //tx_id,layout,schema
    blk: BlockId,
    pub frame: FrameRef,
    header: PageHeader,
    tuple_pointers: Vec<TuplePointer>,
    layout: Rc<Layout>,
}

impl HeapPage {
    pub fn new(frame: FrameRef, blk: &BlockId, layout: Rc<Layout>) -> Self {
        let blk = blk.clone();
        let heap_frame = frame.clone();
        let frame_ref = heap_frame.borrow();
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
        }
    }

    // remmeber to add tuple metadata
    pub fn get_field(&self, field_name: &str, index: u16) -> Vec<u8> {
        let (field_type, start_byte) = self.layout.field_data(field_name);
        let pointer = &self.tuple_pointers[index as usize];
        let frame = self.frame.borrow_mut();
        let tuple = &frame.page.payload[pointer.offset..(pointer.offset + pointer.size as usize)];
        field_type.read_from_tuple(tuple, start_byte).to_vec()
    }
    pub fn mark_delete(&self, slot_num: usize) {
        let pointer = &self.tuple_pointers[slot_num];
        let mut frame = self.frame.borrow_mut();
        let offset = pointer.offset;
        frame.page.payload[offset] = 1_u8;
    }
    pub fn get_tuple(&self, slot_num: usize) -> Vec<u8> {
        let pointer = &self.tuple_pointers[slot_num];
        let frame = self.frame.borrow();
        let offset = pointer.offset;
        let size = pointer.size;
        frame.page.payload[offset..(offset + size as usize)].to_vec()
    }
    pub fn init_heap(frame: &FrameRef) {
        let header = [4_u16.to_ne_bytes(), 4_u16.to_ne_bytes()].concat();
        frame.borrow_mut().page.write_bytes(header.as_slice(), 0)
    }
    pub fn new_from_empty(frame: FrameRef, blk: &BlockId, layout: Rc<Layout>) -> Self {
        HeapPage::init_heap(&frame);
        HeapPage::new(frame, blk, layout)
    }

    pub fn pointer_and_tuple_exist(&self,tuple_pointer:usize) -> (bool,bool){
        match self.tuple_pointers.get(tuple_pointer){
            None => (false,false),
            Some(tuple) => (true,tuple.size != 0)
        }

    }

    pub fn pointer_count(&self) -> usize{
        self.tuple_pointers.len()
    }
    pub fn insert_tuple(&mut self, tuple: Tuple) {
        // put metadata
        let tuple_size = tuple.tuple_size();
        // ask free map where to put
        // search for empty tuple pointer and modify it
        // if no empty , add new tuple pointer
        // write tuple at  SPACE_END - tuple.length
        let pointer_pos = self
            .tuple_pointers
            .iter_mut()
            .position(|pointer| pointer.size == 0);
        let (tuple_pointer_bytes, index) = if pointer_pos.is_some() {
            let tuple_pointer = self.tuple_pointers.get_mut(pointer_pos.unwrap()).unwrap();
            tuple_pointer.offset = self.header.space_end - tuple_size as usize;
            tuple_pointer.size = tuple_size;
            (tuple_pointer.clone().to_bytes(), pointer_pos.unwrap())
        } else {
            let mut tuple_pointer =
                TuplePointer::new(self.header.space_end - tuple_size as usize, tuple_size);
            self.header.space_start += 4;
            self.tuple_pointers.push(tuple_pointer.clone());
            (tuple_pointer.to_bytes(), self.tuple_pointers.len())
        };
        let mut borrowed_frame = self.frame.borrow_mut();
        borrowed_frame
            .page
            .write_bytes(tuple_pointer_bytes.as_slice(), (index * 4 + 4) as u64);
        borrowed_frame.page.write_bytes(
            tuple.to_bytes().as_slice(),
            self.header.space_end as u64 - tuple_size as u64,
        )
    }
    pub fn vacuum(&mut self) {
        let mut new_page = Page::new(4096);
        let mut space_start = 4_u16;
        let mut space_end = 4095_u16;
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
    }
    pub fn page_iter(&self) -> PageIter{
        PageIter{
            current_slot:0,
            page:&self
        }
    }
}

pub struct PageIter<'page> {
    current_slot : u16,
    page : &'page HeapPage
}

impl<'page> PageIter<'page> {
    pub fn next(&mut self) -> Option<Vec<u8>>{
        if self.current_slot == (self.page.tuple_pointers.len() - 1) as u16{ return None}
        while self.current_slot != self.page.tuple_pointers.len() as u16  {
            if self.page.tuple_pointers[self.current_slot as usize].size != 0 {
                let tuple =  Some(self.page.get_tuple(self.current_slot as usize));
                self.current_slot += 1;
                return tuple
            }
            self.current_slot += 1;

        }
        None
    }
    pub fn has_next(&self) -> bool{
        self.current_slot != (self.page.tuple_pointers.len() -1 ) as u16
    }

}

// pub struct HeapFile {
//     free_space: FreeMap,
//     pages: Vec<HeapPage>,
//     layout: Rc<Layout>,
//     filename: String,
// }
//
// impl HeapFile {
//     pub fn new(
//         free_space: FreeMap,
//         pages: Vec<HeapPage>,
//         layout: Rc<Layout>,
//         filename: String,
//     ) -> Self {
//         Self {
//             free_space,
//             pages,
//             layout,
//             filename,
//         }
//     }
//     pub fn try_insert_tuple(&mut self, tuple_bytes: Vec<(String, Vec<u8>)>) {
//         let tuple = Tuple::new(tuple_bytes, self.layout.clone());
//         let target_page = self.free_space.get_best_fit_block(tuple.tuple_size());
//         let mut target_page = self
//             .pages
//             .iter_mut()
//             .find(|page| page.blk == *target_page.as_ref().unwrap())
//             .unwrap();
//         target_page.insert_tuple(tuple);
//     }
//
//     fn vacuum(&mut self, heap_page: &mut HeapPage) {
//         let mut new_page = Page::new(4096);
//         let mut space_start = 4_u16;
//         let mut space_end = 4095_u16;
//         for mut tuple_pointer_index in 0..heap_page.tuple_pointers.len() {
//             let tuple = heap_page.get_tuple(tuple_pointer_index);
//             if tuple[0] == 1 {
//                 heap_page.tuple_pointers[tuple_pointer_index].offset = 0;
//                 heap_page.tuple_pointers[tuple_pointer_index].size = 0;
//                 new_page.write_bytes(
//                     heap_page.tuple_pointers[tuple_pointer_index]
//                         .clone()
//                         .to_bytes()
//                         .as_slice(),
//                     space_start as u64,
//                 );
//                 space_start += 4;
//             } else {
//                 let tuple_len = tuple.len() as u16;
//                 new_page.write_bytes(tuple.as_slice(), (space_end - tuple_len) as u64);
//                 heap_page.tuple_pointers[tuple_pointer_index].offset =
//                     (space_end - tuple_len) as usize;
//                 new_page.write_bytes(
//                     heap_page.tuple_pointers[tuple_pointer_index]
//                         .clone()
//                         .to_bytes()
//                         .as_slice(),
//                     space_start as u64,
//                 );
//                 space_start += 4;
//                 space_end -= tuple_len;
//             }
//         }
//         new_page.write_bytes(space_start.to_ne_bytes().as_slice(), 0);
//         new_page.write_bytes(space_end.to_ne_bytes().as_slice(), 2);
//         heap_page
//             .frame
//             .borrow_mut()
//             .page
//             .write_bytes(new_page.payload.as_slice(), 0);
//         heap_page.header.space_start = space_start as usize;
//         heap_page.header.space_end = space_end as usize;
//     }
// }

// pub struct FreeMap {
//     btree: BTreeMultiMap<u16, BlockId>,
// }
//
// impl FreeMap {
//     pub fn new(btree: BTreeMultiMap<u16, BlockId>) -> Self {
//         Self { btree }
//     }
//
//     pub fn get_best_fit_block(&mut self, tuple_size: u16) -> Option<BlockId> {
//         let mut iterator = self.btree.iter_mut();
//         let value = iterator.find(|(&k, _)| k >= tuple_size);
//         match value {
//             Some((_, blk)) => Some(blk.to_owned()),
//             None => None,
//         }
//     }
// }
