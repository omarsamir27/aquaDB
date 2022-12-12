use std::rc::Rc;
use crate::schema::schema::Layout;

pub struct NullBitMap {
    bitmap: Vec<u8>,
}

impl NullBitMap {
    pub fn new(layout: Rc<Layout>) -> Self {
        let field_count = layout.fields_count();
        if field_count%8 == 0 {
            Self{bitmap: vec![0_u8; field_count/8]}
        } else {
            Self{bitmap: vec![0_u8; field_count/8 + 1]}
        }
    }
    pub fn set_null_field(&mut self, fld_index: usize) {
        self.bitmap[fld_index/8] |= 1 << fld_index;
    }

    pub fn bitmap(&mut self) -> &mut Vec<u8> {
        &mut self.bitmap
    }

    pub fn get_null_indexes(&self) -> Vec<u8> {
        let mut index = 0_u8;
        let mut indexes = Vec::new();
        for byte in self.bitmap {
            for bit in byte {
                if ((byte >> bit) & 1) == 1 {
                    indexes.push(index);
                }
                index += 1;
            }
        }
        indexes
    }
}