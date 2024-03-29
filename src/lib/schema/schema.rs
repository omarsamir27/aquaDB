use super::types::Type;
use crate::index::IndexInfo;
use crate::sql::create_table::IndexType;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Debug)]
/// Vector of fields that are in a table (tuple)
pub struct Schema {
    name: String,
    primary_key: Vec<String>,
    fields: Vec<Field>,
    indexes: Vec<FieldIndex>,
}
impl Schema {
    pub fn new() -> Self {
        Self {
            name: "".to_string(),
            primary_key: vec![],
            fields: vec![],
            indexes: vec![],
        }
    }
    pub fn field_types(&self) -> HashMap<&str, Type> {
        self.fields
            .iter()
            .map(|f| (f.name(), f.field_type))
            .collect::<HashMap<&str, Type>>()
    }
    pub fn set_primary_keys(&mut self, mut keys: Vec<String>) {
        keys.sort_unstable();
        self.primary_key.append(keys.as_mut())
    }
    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
    pub fn add_field(
        &mut self,
        name: &str,
        field_type: Type,
        nullable: bool,
        unique: bool,
        foreign_reference: Option<(String, String)>,
        char_limit: Option<u32>,
    ) {
        self.fields.push(Field::new(
            name,
            field_type,
            nullable,
            unique,
            foreign_reference,
            char_limit,
        ))
    }
    pub fn add_field_default_constraints(
        &mut self,
        name: &str,
        field_type: Type,
        char_limit: Option<u32>,
    ) {
        self.add_field(name, field_type, true, false, None, char_limit);
    }
    pub fn add_index(&mut self, index_name: &str, fieldname: &str, index_type: IndexType) {
        let key_type = self
            .fields
            .iter()
            .find(|f| f.name == fieldname)
            .unwrap()
            .field_type;
        self.indexes
            .push(FieldIndex::new(index_name, fieldname, index_type, key_type))
    }

    pub fn serialize(
        &self,
    ) -> (
        Vec<Vec<(String, Option<Vec<u8>>)>>,
        Vec<Vec<(String, Option<Vec<u8>>)>>,
    ) {
        let mut ret = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            let mut row = [
                "tablename".to_string(),
                "fieldname".to_string(),
                "fieldtype".to_string(),
                "pkey_piece".to_string(),
                "nullable".to_string(),
                "unique".to_string(),
                "foreign_table".to_string(),
                "foreign_field".to_string(),
            ];
            let row = row
                .into_iter()
                .zip([
                    Some(self.name.clone().into_bytes()),
                    Some(field.name.clone().into_bytes()),
                    Some(field.field_type.to_string().into_bytes()),
                    Some(if self.primary_key.contains(&field.name) {
                        1_u8.to_ne_bytes().to_vec()
                    } else {
                        0_u8.to_ne_bytes().to_vec()
                    }),
                    Some(if field.nullable {
                        1_u8.to_ne_bytes().to_vec()
                    } else {
                        0_u8.to_ne_bytes().to_vec()
                    }),
                    Some(if field.unique {
                        1_u8.to_ne_bytes().to_vec()
                    } else {
                        0_u8.to_ne_bytes().to_vec()
                    }),
                    field
                        .foreign_reference
                        .as_ref()
                        .map(|(f_table, _)| f_table.clone().into_bytes()),
                    field
                        .foreign_reference
                        .as_ref()
                        .map(|(_, f_col)| f_col.clone().into_bytes()),
                ])
                .collect();
            // dbg!(&row);
            ret.push(row);
        }
        (ret, self.serialize_indexes())
    }

    pub fn deserialize(
        row_bytes: Vec<HashMap<String, Option<Vec<u8>>>>,
        indexes: Vec<HashMap<String, Option<Vec<u8>>>>,
    ) -> Self {
        let mut schema = Self::new();
        schema.set_name(
            String::from_utf8(
                row_bytes[0]
                    .get("tablename")
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .clone(),
            )
            .unwrap()
            .as_str(),
        );
        for mut field in row_bytes {
            let name = String::from_utf8(field.remove("fieldname").unwrap().unwrap()).unwrap();
            let datatype = Type::from_str(
                String::from_utf8(field.remove("fieldtype").unwrap().unwrap())
                    .unwrap()
                    .as_str(),
            )
            .unwrap();
            let pkey_piece = field.remove("pkey_piece").unwrap().unwrap()[0] == 1;
            let nullable = field.remove("nullable").unwrap().unwrap()[0] == 1;
            let unique = field.remove("unique").unwrap().unwrap()[0] == 1;
            let foreign_ref = match (
                field.remove("foreign_table").unwrap(),
                field.remove("foreign_field").unwrap(),
            ) {
                (None, _) => None,
                (Some(table), Some(field)) => Some((
                    String::from_utf8(table).unwrap(),
                    String::from_utf8(field).unwrap(),
                )),
                _ => unreachable!(),
            };
            schema.add_field(name.as_str(), datatype, nullable, unique, foreign_ref, None);
            if pkey_piece {
                schema.primary_key.push(name);
            }
        }
        let type_map = schema
            .field_types()
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect::<HashMap<_, _>>();
        schema.indexes.extend(indexes.into_iter().map(|idx| {
            let key_type = *type_map
                .get(&*String::from_utf8_lossy(
                    idx.get("fieldname").unwrap().as_ref().unwrap(),
                ))
                .unwrap();
            FieldIndex::deserialize(idx, key_type)
        }));
        schema
    }
    fn serialize_indexes(&self) -> Vec<Vec<(String, Option<Vec<u8>>)>> {
        self.indexes
            .iter()
            .map(|idx| idx.serialize(self.name()))
            .collect()
    }
    /// Convert the schema to a layout
    pub fn to_layout(&self) -> Layout {
        Layout::new(self)
    }

    pub fn fields(&self) -> Vec<Field> {
        self.fields.to_vec()
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn primary_key(&self) -> &Vec<String> {
        &self.primary_key
    }
    pub fn indexes(&self) -> &Vec<FieldIndex> {
        &self.indexes
    }
    pub fn fields_info(&self) -> HashMap<&str, &Field> {
        self.fields.iter().map(|f| (f.name.as_str(), f)).collect()
    }
}

/// Entity containing a certain field's info such as:
///
/// Name, Type, Whether it can be Null, Limit of characters if it is a Varchar
#[derive(Clone, Debug)]
pub struct Field {
    name: String,
    field_type: Type,
    nullable: bool,
    unique: bool,
    foreign_reference: Option<(String, String)>,
    char_limit: Option<u32>,
}

impl Field {
    pub fn new(
        name: &str,
        field_type: Type,
        nullable: bool,
        unique: bool,
        foreign_reference: Option<(String, String)>,
        char_limit: Option<u32>,
    ) -> Self {
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
    pub fn unique(&self) -> bool {
        self.unique
    }
    pub fn foreign_reference(&self) -> &Option<(String, String)> {
        &self.foreign_reference
    }
}

/// Entity used to order fields inside a tuple
///
/// Used when reading and writing tuples
#[derive(Debug, Default, Clone)]
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
        *self.map.get(field_name).unwrap()
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
    pub fn get_type(&self, field: &str) -> Type {
        self.map.get(field).as_ref().unwrap().0
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

#[derive(Clone, Debug)]
pub struct FieldIndex {
    name: String,
    fieldname: String,
    index_type: IndexType,
    key_type: Type,
}

impl FieldIndex {
    pub fn new(name: &str, fieldname: &str, index_type: IndexType, key_type: Type) -> Self {
        Self {
            name: name.to_string(),
            fieldname: fieldname.to_string(),
            index_type,
            key_type,
        }
    }

    pub fn serialize(&self, tablename: &str) -> Vec<(String, Option<Vec<u8>>)> {
        vec![
            ("tablename".to_string(), Some(tablename.as_bytes().to_vec())),
            (
                "index_name".to_string(),
                Some(self.name.as_bytes().to_vec()),
            ),
            (
                "fieldname".to_string(),
                Some(self.fieldname.as_bytes().to_vec()),
            ),
            (
                "index_type".to_string(),
                Some(self.index_type.to_string().as_bytes().to_vec()),
            ),
            (
                "directory_file".to_string(),
                Some(format!("{}_idx_directory", &self.name).into_bytes()),
            ),
            (
                "index_file".to_string(),
                Some(format!("{}_idx_file", &self.name).into_bytes()),
            ),
        ]
    }
    pub fn deserialize(mut row: HashMap<String, Option<Vec<u8>>>, key_type: Type) -> Self {
        let name = String::from_utf8(row.remove("index_name").unwrap().unwrap()).unwrap();
        let fieldname = String::from_utf8(row.remove("fieldname").unwrap().unwrap()).unwrap();
        let index_type = IndexType::from_str(
            &String::from_utf8(row.remove("index_type").unwrap().unwrap()).unwrap(),
        )
        .unwrap();
        Self {
            name,
            fieldname,
            index_type,
            key_type,
        }
    }
    pub fn to_index_info(&self, db_name: &str) -> IndexInfo {
        IndexInfo::new(
            db_name,
            self.name.clone(),
            self.index_type,
            self.fieldname.clone(),
            PathBuf::from(format!("{}_idx_file", &self.name)),
            PathBuf::from(format!("{}_idx_directory", &self.name)),
            self.key_type,
        )
    }
}
