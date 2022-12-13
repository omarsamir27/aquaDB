use std::rc::Rc;
use crate::schema::schema::Layout;

#[derive(Clone)]
pub struct NullBitMap {
    bitmap: Vec<u8>,
}

impl NullBitMap {
    pub fn new(layout: Rc<Layout>) -> Self {
        let field_count = layout.fields_count();
        Self{bitmap: vec![0_u8; f32::ceil(field_count as f32/8.0) as usize]}
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
        for byte in &self.bitmap {
            for bit in 0..8_u8 {
                if ((byte >> bit) & 1) == 1 {
                    indexes.push(index);
                }
                index += 1;
            }
        }
        indexes
    }

    pub fn read_bitmap(&mut self, bitmap: &[u8]) {
        let mut new_bitmap = bitmap.to_vec();
        self.bitmap.swap_with_slice(new_bitmap.as_mut());
    }

    pub fn is_null(&self, index: usize) -> bool {
        self.get_bit(index) == 1
    }

    pub fn get_bit(&self, index: usize) -> u8 {
        let byte = index/8;
        self.bitmap[byte] >> index % 8 & 1
    }
}