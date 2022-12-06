// use core::slice::SlicePattern;
use crate::common::numerical::ByteMagic;
use crate::schema::schema::Layout;
use crate::storage::blockid::BlockId;
use crate::storage::buffermgr::FrameRef;
use positioned_io2::WriteAt;
use std::collections::HashMap;
use std::rc::Rc;
use std::collections::BTreeMap;
use btreemultimap::BTreeMultiMap;
use log::__log_format_args;


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

struct HeapPage {
    //tx_id,layout,schema
    blk: BlockId,
    frame: FrameRef,
    header: PageHeader,
    tuple_pointers: Vec<TuplePointer>,
    layout: Rc<Layout>,
}

impl HeapPage {
    fn new(frame: FrameRef, blk: &BlockId, layout: Rc<Layout>) -> Self {
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
    fn get_field(&self, field_name: &str, index: u16) -> Vec<u8> {
        let (field_type, start_byte) = self.layout.field_data(field_name);
        let pointer = &self.tuple_pointers[index as usize];
        let frame = self.frame.borrow_mut();
        let tuple = &frame.page.payload[pointer.offset..(pointer.offset + pointer.size as usize)];
        field_type.read_from_tuple(tuple, start_byte).to_vec()
    }
    fn mark_delete(&self, slot_num: usize) {
        let pointer = &self.tuple_pointers[slot_num];
        let mut frame = self.frame.borrow_mut();
        let offset = pointer.offset;
        frame.page.payload[offset] = 1_u8;
    }
    fn get_tuple(&self, slot_num: usize) -> Vec<u8> {
        let pointer = &self.tuple_pointers[slot_num];
        let frame = self.frame.borrow();
        let offset = pointer.offset;
        let size = pointer.size;
        frame.page.payload[offset..(offset + size as usize)].to_vec()
    }
    fn init_heap(frame: FrameRef) {
        let header = [4_u16.to_ne_bytes(), 4_u16.to_ne_bytes()].concat();
        frame.borrow_mut().page.write_bytes(header.as_slice(), 0)
    }
    fn insert_tuple(&mut self, tuple: Tuple) {
        // put metadata
        let tuple_size = tuple.tuple_size();
        // ask free map where to put
        // search for empty tuple pointer and modify it
        // if no empty , add new tuple pointer
        // write tuple at  SPACE_END - tuple.length
        let pointer_pos = self.tuple_pointers.iter_mut().position(|pointer| pointer.size == 0);
        let (tuple_pointer_bytes , index) = if pointer_pos.is_some() {
            let tuple_pointer = self.tuple_pointers.get_mut(pointer_pos.unwrap()).unwrap();
            tuple_pointer.offset = self.header.space_end - tuple_size as usize;
            tuple_pointer.size = tuple_size;
            (tuple_pointer.clone().to_bytes(), pointer_pos.unwrap())
        } else {
            let mut tuple_pointer = TuplePointer::new(self.header.space_end -
                                                          tuple_size as usize, tuple_size);
            self.header.space_start += 4;
            self.tuple_pointers.push(tuple_pointer.clone());
            (tuple_pointer.to_bytes(), self.tuple_pointers.len())
        };
        let mut borrowed_frame = self.frame.borrow_mut();
        borrowed_frame.page.write_bytes(tuple_pointer_bytes.as_slice(),
                                        (index*4 + 4) as u64);
        borrowed_frame.page.write_bytes(tuple.to_bytes().as_slice(),
                                        self.header.space_end as u64 - tuple_size as u64)
    }

    fn vacuum() {
        todo!()
    }

    /*
       VACUUM:
           in a page:
               create new page
               for tuple pointers:
                   read tuple pointer -> temp
                   goto associated tuple and read active flag
                   if active
                       add tuple to new page
                       add temp to new page tuple pointers with appropirate modification
                   else deleted
                       add temp to new page tuple pointers in dead form
               modify PAGE HEADER SPACE END to point to last written to location
    */

    //fn search()
}
struct Tuple {
    deleted: u8,
    data: Vec<(String, Vec<u8>)>,
    layout: Rc<Layout>,
}

impl Tuple {
    fn new(data: Vec<(String, Vec<u8>)>, layout: Rc<Layout>) -> Self {
        Self {
            deleted: 0,
            data,
            layout,
        }
    }
    // BEWARE OF NULL IN DATA MAP
    fn tuple_size(&self) -> u16 {
        let mut size = 0;

        for (fieldname, data) in &self.data {
            let (field_type, _) = self.layout.field_data(fieldname);
            if !field_type.needs_pointer() {
                size += field_type.unit_size().unwrap() as u16;
            } else {
                size += (data.len() as u16 + 4);
            }
        }
        size
    }

    fn to_bytes(self) -> Vec<u8> {
        let size = self.tuple_size() + 1;
        let mut tuple = vec![0_u8; size as usize];
        tuple[0] = 0;

        let (constants, varchars): (Vec<(String, Vec<u8>)>, Vec<(String, Vec<u8>)>) = self
            .data
            .into_iter()
            .partition(|(name, _)| self.layout.field_data(name).0.needs_pointer() == false);

        // for (fieldname, data) in self.data {
        //     let (field_type, offset) = self.layout.field_data(fieldname.as_str());
        //     if !field_type.needs_pointer() {
        //         tuple.write_at(offset as u64, data.as_slice());
        //     }
        // }
        // for (fieldname, data) in self.data {
        //     let (field_type, offset) = self.layout.field_data(fieldname.as_str());
        // }
        let mut current_pos = 0_u16;
        for field in constants{
            let (fieldtype,offset) = self.layout.field_data(field.0.as_str());
            tuple.write_at(offset as u64,field.1.as_slice());
            current_pos += fieldtype.unit_size().unwrap() as u16;
        }
        let varchars_ptrs = (varchars.len() * 4) as u16;
        let mut curr_string_start = current_pos + varchars_ptrs ;
        for field in varchars{
            let (fieldtype,offset) = self.layout.field_data(field.0.as_str());
            tuple.write_at(offset as u64,curr_string_start.to_ne_bytes().as_slice());
            tuple.write_at((offset + 2) as u64,field.1.len().to_ne_bytes().as_slice());
            tuple.write_at(curr_string_start as u64,field.1.as_slice());
            curr_string_start += field.1.len() as u16;
        }
        tuple
    }
}

struct HeapFile {
    free_space: FreeMap,
    pages: Vec<HeapPage>,
    layout: Rc<Layout>,
}

impl HeapFile {
    pub fn new(free_space: FreeMap, pages: Vec<HeapPage>, layout: Rc<Layout>) -> Self {
        Self { free_space, pages, layout }
    }
    pub fn try_insert_tuple(&mut self, tuple_bytes: Vec<(String, Vec<u8>)>){
        let tuple = Tuple::new(tuple_bytes, self.layout.clone());
        let target_page = self.free_space.get_best_fit_block(tuple.tuple_size());
        let mut target_page = self.pages.iter_mut().find(|page| page.blk == *target_page.as_ref().unwrap()).unwrap();
        target_page.insert_tuple(tuple);
    }
}

struct FreeMap {
    btree: BTreeMultiMap<u16, BlockId>,
}

impl FreeMap {
    pub fn new(btree: BTreeMultiMap<u16, BlockId>) -> Self {
        Self { btree }
    }

    pub fn get_best_fit_block(&mut self, tuple_size: u16) -> Option<BlockId> {
        let mut iterator = self.btree.iter_mut();
        let value = iterator.find(|(&k, _)| k >= tuple_size);
        match value {
            Some((_, blk)) => Some(blk.to_owned()),
            None => None
        }
    }
}