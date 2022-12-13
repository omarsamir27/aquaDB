use super::types::Type;
use std::collections::HashMap;

pub struct Schema {
    fields: Vec<Field>,
}
impl Schema {
    pub fn new() -> Self {
        Self { fields: vec![] }
    }
    pub fn add_field(
        &mut self,
        name: &str,
        field_type: Type,
        nullable: bool,
        char_limit: Option<u32>,
    ) {
        self.fields.push(Field::new(
            name,
            field_type,
            nullable,
            char_limit,
        ))
    }
    pub fn to_layout(&self) -> Layout{
        Layout::new(self)
    }

    pub fn fields(&self) -> Vec<Field> {
        self.fields.to_vec()
    }
}

#[derive(Clone)]
pub struct Field {
    name: String,
    field_type: Type,
    nullable: bool,
    char_limit: Option<u32>,
}

impl Field {
    pub fn new(name: &str, field_type: Type, nullable: bool, char_limit: Option<u32>) -> Self {
        Self {
            name:name.to_string(),
            field_type,
            nullable,
            char_limit,
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn field_type(&self) -> Type {
        self.field_type
    }
    pub fn nullable(&self) -> bool {
        self.nullable
    }
    pub fn char_limit(&self) -> Option<u32> {
        self.char_limit
    }
}
#[derive(Debug)]
pub struct Layout {
    map: HashMap<String, (Type, u16)>,
    index_map: HashMap<String, u8>,
}
impl Layout {
    fn new(schema: &Schema) -> Self {
        let mut map = HashMap::new();
        let mut index = 0_u8;
        let mut index_map = HashMap::new();
        let mut offset = 0_u16;
        for field in &schema.fields {
            if !field.field_type.needs_pointer() {
                map.insert(field.name.clone(), (field.field_type, offset));
                offset += field.field_type.unit_size().unwrap() as u16;
                index_map.insert(field.name.clone(), index);
                index += 1;
            }
        }
        for field in &schema.fields {
            if field.field_type.needs_pointer() {
                map.insert(field.name.clone(), (field.field_type, offset));
                offset += 4;
                index_map.insert(field.name.clone(), index);
                index += 1;
            }
        }

        Self { map, index_map }
    }
    pub fn field_data(&self, field_name: &str) -> (Type, u16) {
        self.map.get(field_name).unwrap().clone()
    }
    pub fn fields_count(&self) -> usize {
        self.map.keys().len()
    }

    pub fn map(&self) -> &HashMap<String, (Type, u16)> {
        &self.map
    }
    pub fn index_map(&self) -> &HashMap<String, u8> {
        &self.index_map
    }
    pub fn name_map(&self) -> HashMap<u8, String> {
        self.index_map.iter().map(|(k, v)| (v.clone(), k.clone())).collect()
    }
}
/*
   layout hashmap creation

   let mut pos = 0
   for field in fields
       if ! field.field_type.needs_pointer


*/
