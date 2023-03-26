use crate::schema::null_bitmap::NullBitMap;
use crate::schema::schema::Layout;
use positioned_io2::WriteAt;
use std::ops::Index;
use std::rc::Rc;

#[derive(Debug)]
/// A helper Struct that acts as an converter between the raw form of a tuple supplied by the result
/// of parsing a query statement and the Storage Engine
///
/// It is responsible for placing the bytes according to the layout generated from the table schema
/// and constructing the associated Null BitMap
pub struct Tuple {
    deleted: u8,
    bitmap: NullBitMap,
    data: Vec<(String, Option<Vec<u8>>)>,
    layout: Rc<Layout>,
}

impl Tuple {
    ///Creates a Tuple instance from a (Fieldname , Field Data Bytes) Vector and a layout supplied from
    /// the table schema
    pub fn new(data: Vec<(String, Option<Vec<u8>>)>, layout: Rc<Layout>) -> Self {
        let bitmap = NullBitMap::new(layout.clone());
        Self {
            deleted: 0,
            bitmap,
            data,
            layout,
        }
    }
    /// Returns the length of the tuple when it has been reordered and with the necessary metadata and
    /// internal field pointers added , used by the heap file interface for tuple insertion and
    /// navigation
    pub fn tuple_size(&self) -> u16 {
        let mut size = 0;
        let mut bitmap = self.bitmap.clone();
        for (fieldname, data) in &self.data {
            if data.is_none() {
                continue;
            }
            let (field_type, _) = self.layout.field_data(fieldname);
            if !field_type.needs_pointer() {
                size += field_type.unit_size().unwrap() as u16;
            } else {
                size += (data.clone().unwrap().len() as u16 + 4);
            }
        }
        size + 1 + (bitmap.bitmap().len() as u16)
    }

    /// Consumes the Tuple instance to create the record form to be placed in a heap file.
    ///
    /// The algorithm to do this is as follows:
    ///
    /// We start out by separating numeric and character fields , the numeric fields are to be
    /// considered first because their size is deterministic so they do not need pointers.
    ///
    /// For correctness , the supplied Tuple data vector must be reordered in strict
    /// ascending order produced by processing the table layout , before doing any work
    ///
    /// For each constant size field:
    ///
    /// if the data is not null , subtract the size that would have been taken by all the previous numeric
    /// fields that were NULLS from the precalculated OFFSET , and place the data bytes at that offset
    /// else SET the corresponding field in the NullBitMap and increment the NULL_SIZE accumulator
    ///
    /// For variable size field:
    ///
    /// The field data itself will start after all the constant size fields and the variable fields
    /// pointers, those pointers will only exist if their data is not NULL , so we count them first,
    /// a variable field pointer is a u16 offset + a u16 length so 1 pointer is 4 bytes wide
    ///
    /// For each variable size fields:
    /// do as the numeric fields , except that the data itself is written after all pointers
    /// and the start of the second var field data is directly after the end of the preceding one
    pub fn to_bytes(mut self) -> Vec<u8> {
        let size = self.tuple_size();
        let mut tuple = vec![0; (size - 1 - (self.bitmap.bitmap().len() as u16)) as usize];
        let index_map = self.layout.index_map();
        let mut ordered_tuple = vec![("".to_string(), None); self.data.len()];
        for field in self.data {
            let index = index_map.get(field.0.as_str()).unwrap().clone();
            ordered_tuple[index as usize] = field.clone();
        }
        self.data = ordered_tuple;
        let (constants, varchars): (
            Vec<(String, Option<Vec<u8>>)>,
            Vec<(String, Option<Vec<u8>>)>,
        ) = self
            .data
            .into_iter()
            .partition(|(name, _)| self.layout.field_data(name).0.needs_pointer() == false);

        let mut current_pos = 0_u16;
        let mut field_pos = 0_usize;
        let mut null_size = 0_u16;
        for field in constants {
            if field.1.is_some() {
                let (fieldtype, mut offset) = self.layout.field_data(field.0.as_str());
                offset -= null_size;
                tuple.write_at((offset) as u64, field.1.unwrap().as_slice());
                current_pos += fieldtype.unit_size().unwrap() as u16;
            } else {
                let (fieldtype, _) = self.layout.field_data(field.0.as_str());
                null_size += fieldtype.unit_size().unwrap() as u16;
                self.bitmap.set_null_field(field_pos);
            }
            field_pos += 1;
        }
        let varchars_ptrs = (varchars.iter().fold(0, |acc, (_, data)| {
            acc + match data {
                None => 0,
                Some(_) => 1,
            }
        })) * 4 as u16;
        let mut curr_string_start = current_pos + varchars_ptrs;
        for field in varchars {
            if field.1.is_some() {
                let (fieldtype, mut offset) = self.layout.field_data(field.0.as_str());
                offset -= null_size;
                tuple.write_at(offset as u64, curr_string_start.to_ne_bytes().as_slice());
                let field_bytes = field.1.unwrap();
                tuple.write_at(
                    (offset + 2) as u64,
                    (field_bytes.len() as u16).to_ne_bytes().as_slice(),
                );
                tuple.write_at(curr_string_start as u64, field_bytes.as_slice());
                curr_string_start += field_bytes.len() as u16;
            } else {
                null_size += 4;
                self.bitmap.set_null_field(field_pos);
            }
            field_pos += 1;
        }
        let mut tuple_all = vec![0_u8; 1];
        tuple_all.append(self.bitmap.bitmap());
        tuple_all.append(&mut tuple);
        tuple_all
    }
}
