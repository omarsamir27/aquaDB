use std::collections::HashMap;
use super::types::Type;

pub struct Schema{
    fields: Vec<Field>
}

struct Field{
    name : String,
    field_type : Type,
    nullable : bool,
    char_limit : Option<u32>,
}

pub struct Layout{
    map : HashMap<String,(Type,u16)>
}
impl Layout {
    fn new(schema: &Schema) -> Self {
        let mut map = HashMap::new();
        let mut offset = 0_u16;
        for field in &schema.fields{
            if !field.field_type.needs_pointer(){
                map.insert(field.name.clone(),(field.field_type,offset));
                offset += field.field_type.unit_size().unwrap() as u16;
            }
        }
        for field in &schema.fields{
            if field.field_type.needs_pointer(){
                map.insert(field.name.clone(),(field.field_type,offset));
                offset += 4;
            }
        }
        Self{map}
    }
    pub fn field_data(&self, field_name:&str) -> (Type, u16) {
        self.map.get(field_name).unwrap().clone()
    }
}
/*
    layout hashmap creation

    let mut pos = 0
    for field in fields
        if ! field.field_type.needs_pointer

            
 */