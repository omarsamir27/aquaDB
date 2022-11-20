use crate::common::numerical::ByteMagic;
use crate::storage::blockid::BlockId;
use crate::storage::buffermgr::FrameRef;

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
    fn new(payload:&[u8]) -> Self{
        Self{
            space_start : payload.extract_u16(0) as usize,
            space_end : payload.extract_u16(2) as usize
        }
    }
}

struct TuplePointer {
    offset: usize,
    size: u16,
}

impl TuplePointer {
    fn read_pointer(payload: &[u8], offset: usize) -> TuplePointer {
        let tuple_offset = payload.extract_u16(offset);
        let size = payload.extract_u16(offset + 2);
        TuplePointer { offset: tuple_offset as usize, size }
    }
}

struct HeapPage {
    //tx_id,layout,schema
    blk: BlockId,
    frame: FrameRef,
    header: PageHeader,
    tuple_pointers: Vec<TuplePointer>,
}

impl HeapPage {
    fn new(frame: FrameRef, blk: &BlockId) -> Self {
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
                current_offset += 4 ;
            }
        }
        drop(frame_ref);
        Self {
            blk,
            frame: heap_frame,
            header,
            tuple_pointers
        }
    }
    fn get_field() {
        todo!()
    }
    fn mark_delete(&self,slot_num:usize) {
        let pointer = &self.tuple_pointers[slot_num];
        let mut frame = self.frame.borrow_mut();
        let offset = pointer.offset;
        frame.page.payload[offset] = 0_u8;
    }
    fn get_tuple(&self,slot_num:usize) -> Vec<u8> {
        let pointer = &self.tuple_pointers[slot_num];
        let frame = self.frame.borrow();
        let offset = pointer.offset;
        let size = pointer.size ;
        frame.page.payload[offset..(offset+size as usize)].to_vec()
    }
    fn init_heap(frame:FrameRef) {
        let header = [4_u16.to_ne_bytes(),4_u16.to_ne_bytes()].concat();
        frame.borrow_mut().page.write_bytes(header.as_slice(),0)
    }
    fn insert_tuple() {
        todo!()
    }
    fn vacuum() {
        todo!()
    }
    //fn search()
}
