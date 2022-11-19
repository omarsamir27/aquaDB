use crate::common::numerical::ByteMagic;
use crate::storage::blockid::BlockId;
use crate::storage::buffermgr::FrameRef;
use std::intrinsics::size_of;
use std::io::Bytes;

struct PageHeader {
    space_start: usize,
    space_end: usize,
}

struct TuplePointer {
    offset: usize,
    size: u16,
}

impl TuplePointer {
    fn read_pointer(payload: &[u8], offset: usize) -> TuplePointer {
        let offset = payload.extract_usize(offset);
        let size = payload.extract_u16(offset + size_of::<usize>());
        TuplePointer { offset, size }
    }
    #[inline(always)]
    fn size() -> usize {
        size_of::<usize>() + size_of::<u16>() as usize
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
        let usize_size = size_of::<usize>();
        let header = PageHeader {
            space_start: frame_ref.page.payload.as_slice().extract_usize(0),
            space_end: frame_ref.page.payload.as_slice().extract_usize(usize_size),
        };
        let mut current_offset = 0_usize;
        let mut tuple_pointers: Vec<TuplePointer> = Vec::new();
        if header.space_start != header.space_end {
            current_offset = 2 * usize_size;
            while current_offset != header.space_start {
                let pointer =
                    TuplePointer::read_pointer(frame_ref.page.payload.as_slice(), current_offset);
                tuple_pointers.push(pointer);
                current_offset += TuplePointer::size();
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
    fn mark_delete() {
        todo!()
    }
    fn get_tuple() {
        todo!()
    }
    fn init_heap(frame:FrameRef) {
        let size = 2*size_of::<usize>();
        let header = [size.to_ne_bytes(),size.to_ne_bytes()];
        let header = header.flatten();
        frame.borrow_mut().page.write_bytes(header,0)
    }
    fn insert_tuple() {
        todo!()
    }
    fn vacuum() {
        todo!()
    }
    //fn search()
}
