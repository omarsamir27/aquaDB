use crate::schema::schema::Layout;
use std::rc::Rc;

/// Custom vector wrapper that stores the null fields in a tuple bit by bit ( 1 -> Null )
#[derive(Clone, Debug)]
pub struct NullBitMap {
    bitmap: Vec<u8>,
}

impl NullBitMap {
    pub fn new(layout: Rc<Layout>) -> Self {
        let field_count = layout.fields_count();
        Self {
            bitmap: vec![0_u8; f32::ceil(field_count as f32 / 8.0) as usize],
        }
    }

    /// Sets the bit of a certain Null field by it's index
    pub fn set_null_field(&mut self, fld_index: usize) {
        self.bitmap[fld_index / 8] |= 1 << fld_index;
    }

    pub fn bitmap(&mut self) -> &mut Vec<u8> {
        &mut self.bitmap
    }

    /// Returns a vector with the indexes of the Null fields in a tuple by reading the 1s in the
    /// bitmap of a tuple
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

    /// Fills an empty bitmap by reading it from the bitmap bytes at the beginning of a tuple
    pub fn read_bitmap(&mut self, bitmap: &[u8]) {
        let mut new_bitmap = bitmap.to_vec();
        self.bitmap.swap_with_slice(new_bitmap.as_mut());
    }

    /// Checks if a certain field inside the bitmap is 1 (Null)
    pub fn is_null(&self, index: usize) -> bool {
        self.get_bit(index) == 1
    }

    /// Returns the bit of a certain field by it's index whether 1 or 0 in a whole byte
    pub fn get_bit(&self, index: usize) -> u8 {
        let byte = index / 8;
        self.bitmap[byte] >> index % 8 & 1
    }
}
