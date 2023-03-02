use super::types::Type;
use std::collections::HashMap;

/// Vector of fields that are in a table (tuple)
pub struct Schema {
    name : String,
    primary_key : Vec<String>,
    fields: Vec<Field>,
}
impl Schema {
    pub fn new() -> Self {
        Self { name : "".to_string() , primary_key : vec![],fields: vec![] }
    }
    pub fn add_field(
        &mut self,
        name: &str,
        field_type: Type,
        nullable: bool,
        unique : bool,
        foreign_reference: Option<(String, String)>,
        char_limit: Option<u32>,
    ) {
        self.fields
            .push(Field::new(name, field_type, nullable, unique,foreign_reference, char_limit))
    }
     pub fn add_field_default_constraints(&mut self,name:&str,field_type:Type,char_limit:Option<u32>){
         self.add_field(name,field_type,true,false,None,char_limit);
     }

    /// Convert the schema to a layout
    pub fn to_layout(&self) -> Layout {
        Layout::new(self)
    }

    pub fn fields(&self) -> Vec<Field> {
        self.fields.to_vec()
    }
}

/// Entity containing a certain field's info such as:
///
/// Name, Type, Whether it can be Null, Limit of characters if it is a Varchar
#[derive(Clone)]
pub struct Field {
    name: String,
    field_type: Type,
    nullable: bool,
    unique: bool,
    foreign_reference : Option<(String,String)>,
    char_limit: Option<u32>,
}

impl Field {
    pub fn new(name: &str, field_type: Type, nullable: bool, unique:bool , foreign_reference : Option<(String,String)> , char_limit: Option<u32>) -> Self {
        Self {
            name: name.to_string(),
            field_type,
            nullable,
            unique,
            foreign_reference,
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

/// Entity used to order fields inside a tuple
///
/// Used when reading and writing tuples
#[derive(Debug)]
pub struct Layout {
    map: HashMap<String, (Type, u16)>,
    index_map: HashMap<String, u8>,
}
impl Layout {
    /// Loops over the fields by their order in the schema and reorders them by putting
    /// the constants first then the Varchars
    fn new(schema: &Schema) -> Self {
        let mut map = HashMap::new();
        let mut index = 0_u8;
        let mut index_map = HashMap::new();
        let mut offset = 0_u16;
        // getting constants first and putting them in the hashmap ordered by their precedence
        // in the schema
        for field in &schema.fields {
            if !field.field_type.needs_pointer() {
                map.insert(field.name.clone(), (field.field_type, offset));
                offset += field.field_type.unit_size().unwrap() as u16;
                index_map.insert(field.name.clone(), index);
                index += 1;
            }
        }
        // getting variables then and putting them in the hashmap ordered by their precedence
        // in the schema
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

    /// Return the type, offset of a field in a tuple by its name
    pub fn field_data(&self, field_name: &str) -> (Type, u16) {
        self.map.get(field_name).unwrap().clone()
    }

    /// Number of fields in a table
    pub fn fields_count(&self) -> usize {
        self.map.keys().len()
    }

    pub fn map(&self) -> &HashMap<String, (Type, u16)> {
        &self.map
    }
    pub fn index_map(&self) -> &HashMap<String, u8> {
        &self.index_map
    }

    /// Inverse hashmap of a index_map with
    ///
    /// k -> field order in a layout
    /// v -> field name
    pub fn name_map(&self) -> HashMap<u8, String> {
        self.index_map
            .iter()
            .map(|(k, v)| (v.clone(), k.clone()))
            .collect()
    }

    pub fn type_map(&self) -> HashMap<String, Type> {
        self.map
            .iter()
            .map(|(k, (t, _))| (k.to_string(), *t))
            .collect()
    }
}
