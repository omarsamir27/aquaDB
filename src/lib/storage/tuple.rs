use crate::schema::schema::Layout;
use positioned_io2::WriteAt;
use std::rc::Rc;

pub struct Tuple {
    deleted: u8,
    data: Vec<(String, Vec<u8>)>,
    layout: Rc<Layout>,
}

impl Tuple {
    pub fn new(data: Vec<(String, Vec<u8>)>, layout: Rc<Layout>) -> Self {
        Self {
            deleted: 0,
            data,
            layout,
        }
    }
    // BEWARE OF NULL IN DATA MAP
    pub fn tuple_size(&self) -> u16 {
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

    pub(crate) fn to_bytes(self) -> Vec<u8> {
        let size = self.tuple_size() + 1;
        let mut tuple = vec![0_u8; size as usize];
        tuple[0] = 0;

        let (constants, varchars): (Vec<(String, Vec<u8>)>, Vec<(String, Vec<u8>)>) = self
            .data
            .into_iter()
            .partition(|(name, _)| self.layout.field_data(name).0.needs_pointer() == false);

        let mut current_pos = 0_u16;
        for field in constants {
            let (fieldtype, offset) = self.layout.field_data(field.0.as_str());
            tuple.write_at(offset as u64, field.1.as_slice());
            current_pos += fieldtype.unit_size().unwrap() as u16;
        }
        let varchars_ptrs = (varchars.len() * 4) as u16;
        let mut curr_string_start = current_pos + varchars_ptrs;
        for field in varchars {
            let (fieldtype, offset) = self.layout.field_data(field.0.as_str());
            tuple.write_at(offset as u64, curr_string_start.to_ne_bytes().as_slice());
            tuple.write_at((offset + 2) as u64, field.1.len().to_ne_bytes().as_slice());
            tuple.write_at(curr_string_start as u64, field.1.as_slice());
            curr_string_start += field.1.len() as u16;
        }
        tuple
    }
}
