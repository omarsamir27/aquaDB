use std::ops::Index;
use crate::schema::schema::Layout;
use positioned_io2::WriteAt;
use std::rc::Rc;
use crate::schema::null_bitmap::NullBitMap;

pub struct Tuple {
    deleted: u8,
    bitmap: NullBitMap,
    data: Vec<(String, Option<Vec<u8>>)>,
    layout: Rc<Layout>,
}

impl Tuple {
    pub fn new(data: Vec<(String, Option<Vec<u8>>)>, layout: Rc<Layout>) -> Self {
        let bitmap = NullBitMap::new(layout.clone());
        Self {
            deleted: 0,
            bitmap,
            data,
            layout,
        }
    }
    // BEWARE OF NULL IN DATA MAP
    pub fn tuple_size(&self) -> u16 {
        let mut size = 0;
        let mut bitmap = self.bitmap.clone();
        // let mut bitmap = NullBitMap::new(self.layout.clone());
        for (fieldname, data) in &self.data {
            if data.is_none(){
                continue
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

    pub fn to_bytes(mut self) -> Vec<u8> {
        let size = self.tuple_size();
        // let mut bitmap = NullBitMap::new(self.layout.clone());
        let mut tuple = vec![0;(size - 1 - (self.bitmap.bitmap().len() as u16)) as usize];
        let index_map = self.layout.index_map();
        let mut ordered_tuple = vec![("".to_string(), None); self.data.len()];
        for field in self.data {
            let index = index_map.get(field.0.as_str()).unwrap().clone();
            ordered_tuple[index as usize] = field.clone();
        }
        self.data = ordered_tuple;
        let (constants, varchars): (Vec<(String, Option<Vec<u8>>)>, Vec<(String, Option<Vec<u8>>)>) = self
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
        let varchars_ptrs = (varchars.iter().fold(0, |acc, (_,data)| acc +
            match data {
                None => 0,
                Some(_) => 1
            })) * 4 as u16;
        let mut curr_string_start = current_pos + varchars_ptrs;
        for field in varchars {
            if field.1.is_some() {
                let (fieldtype, mut offset) = self.layout.field_data(field.0.as_str());
                offset -= null_size;
                tuple.write_at(offset as u64, curr_string_start.to_ne_bytes().as_slice());
                let field_bytes = field.1.unwrap();
                tuple.write_at((offset + 2) as u64, (field_bytes.len() as u16).to_ne_bytes().as_slice());
                tuple.write_at(curr_string_start as u64, field_bytes.as_slice());
                curr_string_start += field_bytes.len() as u16;
            } else {
                // let (fieldtype, mut offset) = self.layout.field_data(field.0.as_str());
                // offset -= null_size;
                // tuple.write_at(offset as u64, curr_string_start.to_ne_bytes().as_slice());
                // tuple.write_at((offset + 2) as u64, (0_u16).to_ne_bytes().as_slice());
                null_size += 4;
                self.bitmap.set_null_field(field_pos);
            }
            field_pos += 1;
        };
        let mut tuple_all = vec![0_u8; 1];
        tuple_all.append(self.bitmap.bitmap());
        tuple_all.append(&mut tuple);
        tuple_all
    }
}
