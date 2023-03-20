use crate::schema::schema::{Layout, Schema};
use crate::schema::types::CharType::VarChar;
use crate::schema::types::{CharType, NumericType, Type};
use crate::storage::blkmgr::BlockManager;
use crate::storage::storagemgr::StorageManager;
use crate::table::tablemgr::TableManager;
use crate::{RcRefCell, AQUADIR};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::{cell::RefCell, rc::Rc};

// type TEXT = Type::Character(CharType::VarChar);

pub struct CatalogManager {
    storage_mgr: Rc<RefCell<StorageManager>>,
    databases_tbl: TableManager,
    db_tbl_schema_catalogs: HashMap<String, TableManager>,
}

impl CatalogManager {
    fn new(
        storage_mgr: Rc<RefCell<StorageManager>>,
        databases_tbl: TableManager,
        db_tbl_schema_catalogs: HashMap<String, TableManager>,
    ) -> Self {
        Self {
            storage_mgr,
            databases_tbl,
            db_tbl_schema_catalogs,
        }
    }
    pub fn startup(storagemgr: Rc<RefCell<StorageManager>>) -> Self {
        let databases_tbl = Self::load_dbs_table(&storagemgr);
        let mut dbs_iter = databases_tbl.heapscan_iter();
        let mut db_names = vec![];
        while let Some(row) = dbs_iter.next() {
            db_names.push(
                String::from_utf8(row.get("database_name").unwrap().as_ref().unwrap().clone())
                    .unwrap(),
            )
        }
        let db_tbl_schema_catalogs = db_names
            .into_iter()
            .map(|db| {
                let tbl = Self::load_db_schema_table(&storagemgr, db.as_str());
                (db, tbl)
            })
            .collect();
        Self::new(storagemgr, databases_tbl, db_tbl_schema_catalogs)
    }
    pub fn init_catalogs() {
        let mut storagemgr = RcRefCell!(StorageManager::new(AQUADIR().as_str(), 4096, 10));
        let database_tbl = Self::init_dbs_table(storagemgr);
    }
    pub fn get_schema(&self,db_name:&str,table_name:&str) -> Option<Schema> {
        let db_catalog = self.db_tbl_schema_catalogs.get(db_name)?;
        let db_catalog = db_catalog.heapscan_iter();
        let schema_vec = db_catalog.filter(
            |row| String::from_utf8(row.get("tablename").unwrap()
                .as_ref().unwrap().clone()).unwrap() == table_name
        ).collect::<Vec<_>>();
        Some(Schema::deserialize(schema_vec))
    }
    pub fn add_schema(&mut self, db_name: &str, schema: &Schema) -> Result<(), String> {
        let mut db_catalog = self
            .db_tbl_schema_catalogs
            .get_mut(db_name)
            .ok_or("Database does not exist")?;
        // IF THIS IS INDEXABLE THEN BETTER
        let mut catalog_iter = db_catalog.heapscan_iter();
        let tables: HashSet<String> = catalog_iter
            .map(|row| {
                row.get("tablename")
                    .unwrap()
                    .as_ref()
                    .map(|t| String::from_utf8(t.clone()).unwrap())
                    .unwrap()
            })
            .collect();
        if tables.contains(schema.name()) {
            return Err(format!("Table '{}' already exists ", schema.name()));
        }
        let mut db_schema_catalog = self.db_tbl_schema_catalogs.get_mut(db_name).unwrap();
        let serialized = schema.serialize();
        for field in serialized{
            db_schema_catalog.try_insert_tuple(field);
        }
        //
        db_schema_catalog.flush_all();
        Ok(())
    }
    fn load_dbs_table(storage: &Rc<RefCell<StorageManager>>) -> TableManager {
        let database_tbl_file = Path::new(AQUADIR().as_str())
            .join("global")
            .join("aqua_database");
        TableManager::from_file(storage.clone(), database_tbl_file, Self::dbs_table_layout())
    }
    fn load_db_schema_table(storage: &Rc<RefCell<StorageManager>>, db_name: &str) -> TableManager {
        let schema_name = format!("{}_{}", db_name, "schemas");
        let db_schema_file = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(schema_name.as_str());
        TableManager::from_file(
            storage.clone(),
            db_schema_file,
            Self::db_schema_layout(schema_name.as_str()),
        )
    }
    fn init_dbs_table(storage: Rc<RefCell<StorageManager>>) -> TableManager {
        let layout = Self::dbs_table_layout();
        let path = Path::new(AQUADIR().as_str())
            .join("global")
            .join("aqua_database");
        let blks = storage
            .borrow_mut()
            .empty_heap_pages(path.to_str().unwrap(), 1);
        TableManager::new(blks, storage, None, layout)
    }
    pub fn create_db_schema_table(&mut self, db_name: &str) -> Result<(), String> {
        if self.db_tbl_schema_catalogs.contains_key(db_name) {
            return Err(format!("Database {} already exists", db_name));
        }
        let layout = Self::db_schema_layout(db_name);
        let schema_name = format!("{}_{}", db_name, "schemas");
        let path = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(schema_name);
        let blks = self
            .storage_mgr
            .borrow_mut()
            .empty_heap_pages(path.to_str().unwrap(), 1);
        self.databases_tbl.try_insert_tuple(vec![(
            "database_name".to_string(),
            Some(db_name.as_bytes().to_vec()),
        )]);
        self.databases_tbl.flush_all();
        self.db_tbl_schema_catalogs.insert(
            db_name.to_string(),
            TableManager::new(blks, self.storage_mgr.clone(), None, layout),
        );
        Ok(())
    }
    fn dbs_table_layout() -> Rc<Layout> {
        let mut schema = Schema::new();
        schema.add_field_default_constraints(
            "database_name",
            Type::Character(CharType::VarChar),
            None,
        );
        Rc::new(schema.to_layout())
    }
    fn db_schema_layout(table_name: &str) -> Rc<Layout> {
        let mut schema = Schema::new();
        schema.add_field(
            "tablename",
            Type::Character(CharType::VarChar),
            false,
            false,
            None,
            None,
        );
        schema.add_field(
            "fieldname",
            Type::Character(CharType::VarChar),
            false,
            false,
            None,
            None,
        );
        schema.add_field(
            "fieldtype",
            Type::Character(CharType::VarChar),
            false,
            false,
            None,
            None,
        );
        schema.add_field("pkey_piece", Type::Boolean, false, false, None, None);
        schema.add_field("nullable", Type::Boolean, false, false, None, None);
        schema.add_field("unique", Type::Boolean, false, false, None, None);
        schema.add_field(
            "foreign_table",
            Type::Boolean,
            true,
            false,
            Some((table_name.to_string(), "tablename".to_string())),
            None,
        );
        schema.add_field(
            "foreign_field",
            Type::Boolean,
            true,
            false,
            Some((table_name.to_string(), "fieldname".to_string())),
            None,
        );
        schema.set_name(table_name);
        schema.set_primary_keys(vec!["fieldname".to_string(), "tablename".to_string()]);
        Rc::new(schema.to_layout())
    }
}
