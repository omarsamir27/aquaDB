use crate::schema::schema::{Layout, Schema};
use crate::schema::types::CharType::VarChar;
use crate::schema::types::{CharType, NumericType, Type};
use crate::storage::blkmgr::BlockManager;
use crate::storage::storagemgr::StorageManager;
use crate::table::tablemgr::TableManager;
use crate::{RcRefCell, AQUADIR};
use std::collections::{HashMap, HashSet};
use std::fs::create_dir;
use std::path::{Path, PathBuf};
use std::{cell::RefCell, rc::Rc};

struct InstanceCatalog {
    schemas: TableManager,
    tables_filepaths: TableManager,
    indexes: TableManager,
}

impl InstanceCatalog {
    fn get_db_tables(&self, storage: &Rc<RefCell<StorageManager>>) -> HashMap<String,TableManager>{
        let tables = self.tables_filepaths.heapscan_iter().map(| r |
            (String::from_utf8(r.get("tablename").unwrap().as_ref().unwrap().clone()).unwrap(),
            String::from_utf8(r.get("filepath").unwrap().as_ref().unwrap().clone()).unwrap()))
            .collect::<HashMap<String,String>>();
        tables.into_iter()
            .map(|(name,path)| {
                let schema = self.get_schema(&name) ;
                let table = TableManager::from_file(storage.clone(),PathBuf::from(path),Rc::new(schema.to_layout()));
                (name,table)
            } ).collect()
    }
    fn get_schema(&self,schema_name:&str) -> Schema{
        let schemas_catalog = self.schemas.heapscan_iter();
        let schema_vec = schemas_catalog
            .filter(|row| {
                String::from_utf8(row.get("tablename").unwrap().as_ref().unwrap().clone()).unwrap()
                    == schema_name
            })
            .collect::<Vec<_>>();
        Schema::deserialize(schema_vec)
    }
}
pub struct CatalogManager {
    storage_mgr: Rc<RefCell<StorageManager>>,
    databases_tbl: TableManager,
    databases_catalogs: HashMap<String, InstanceCatalog>,
}

impl CatalogManager {
    fn new(
        storage_mgr: Rc<RefCell<StorageManager>>,
        databases_tbl: TableManager,
        databases_catalogs: HashMap<String, InstanceCatalog>,
    ) -> Self {
        Self {
            storage_mgr,
            databases_tbl,
            databases_catalogs,
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
                let schemas = Self::load_db_schema_table(&storagemgr, db.as_str());
                let indexes = Self::load_db_indexes_table(&storagemgr, db.as_str());
                let tables_filepaths = Self::load_db_tables_files_table(&storagemgr, db.as_str());
                let instance = InstanceCatalog {
                    schemas,
                    indexes,
                    tables_filepaths,
                };
                (db, instance)
            })
            .collect();
        Self::new(storagemgr, databases_tbl, db_tbl_schema_catalogs)
    }
    pub fn get_db_tables(&self,db_name:&str) -> HashMap<String,TableManager>{
        self.databases_catalogs.get(db_name).unwrap().get_db_tables(&self.storage_mgr)
    }
    pub fn has_db(&self, db_name: &str) -> bool {
        // self.databases_tbl
        //     .heapscan_iter()
        //     .any(|r| r.get("database_name").unwrap().as_ref().unwrap() == db_name.as_bytes())
        self.databases_catalogs.contains_key(db_name)
    }
    pub fn init_catalogs() {
        let mut storagemgr = RcRefCell!(StorageManager::new(AQUADIR().as_str(), 4096, 10));
        let database_tbl = Self::init_dbs_table(storagemgr);
    }
    pub fn get_schema(&self, db_name: &str, table_name: &str) -> Option<Schema> {
        let db_catalog = self.databases_catalogs.get(db_name)?;
        Some(db_catalog.get_schema(table_name))
    }
    pub fn add_schema(&mut self, db_name: &str, schema: &Schema) -> Result<(), String> {
        let mut db_catalog = self
            .databases_catalogs
            .get_mut(db_name)
            .ok_or("Database does not exist")?;
        // IF THIS IS INDEXABLE THEN BETTER
        let mut catalog_iter = db_catalog.tables_filepaths.heapscan_iter();
        // let tables: HashSet<String> = catalog_iter.any()
        //     .map(|row| {
        //         row.get("tablename")
        //             .unwrap()
        //             .as_ref()
        //             .map(|t| String::from_utf8(t.clone()).unwrap())
        //             .unwrap()
        //     })
        //     .collect();
        if catalog_iter.any(|row| {
            row.get("tablename")
                .unwrap()
                .as_ref()
                .map(|t| String::from_utf8(t.clone()).unwrap())
                .unwrap()
                == *schema.name()
        }) {
            return Err(format!("Table '{}' already exists ", schema.name()));
        }
        let mut db_schema_catalog = &mut db_catalog.schemas;
        let serialized = schema.serialize();
        for field in serialized {
            db_schema_catalog.try_insert_tuple(field);
        }
        //
        let mut db_tablesfiles_catalog = &mut db_catalog.tables_filepaths;
        db_tablesfiles_catalog.try_insert_tuple(vec![
            (
                "tablename".to_string(),
                Some(schema.name().as_bytes().to_vec()),
            ),
            (
                "filepath".to_string(),
                Some(format!("{}_heap0", schema.name()).into_bytes()),
            ),
        ]);
        db_tablesfiles_catalog.flush_all();
        db_schema_catalog.flush_all();
        Ok(())
    }
    fn load_dbs_table(storage: &Rc<RefCell<StorageManager>>) -> TableManager {
        let database_tbl_file = Path::new(AQUADIR().as_str())
            .join("global")
            .join("aqua_database");
        TableManager::from_file(storage.clone(), database_tbl_file, Self::dbs_table_layout())
    }
    fn load_db_tables_files_table(
        storage: &Rc<RefCell<StorageManager>>,
        db_name: &str,
    ) -> TableManager {
        let schema_name = format!("{}_{}", db_name, "tables_files");
        let db_schema_file = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(schema_name.as_str());
        TableManager::from_file(
            storage.clone(),
            db_schema_file,
            Self::db_tables_file_layout(schema_name.as_str()),
        )
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
    fn load_db_indexes_table(storage: &Rc<RefCell<StorageManager>>, db_name: &str) -> TableManager {
        let schema_name = format!("{}_{}", db_name, "indexes");
        let db_schema_file = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(schema_name.as_str());
        TableManager::from_file(
            storage.clone(),
            db_schema_file,
            Self::db_indexes_layout(schema_name.as_str()),
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
    pub fn create_database(&mut self, db_name: &str) -> Result<(), String> {
        if self.databases_catalogs.contains_key(db_name) {
            return Err(format!("Database {} already exists", db_name));
        }
        let path = Path::new(AQUADIR().as_str()).join("base").join("samir");
        create_dir(path).expect("could not create database dir ");
        self.databases_tbl.try_insert_tuple(vec![(
            "database_name".to_string(),
            Some(db_name.as_bytes().to_vec()),
        )]);
        self.databases_tbl.flush_all();
        let schemas = self.create_db_schema_table(db_name);
        let indexes = self.create_db_indexes_table(db_name);
        let tables_filepaths = self.create_db_tables_files(db_name);
        self.databases_catalogs.insert(
            db_name.to_string(),
            InstanceCatalog {
                schemas,
                indexes,
                tables_filepaths,
            },
        );
        Ok(())
    }
    fn create_db_indexes_table(&mut self, db_name: &str) -> TableManager {
        let layout = Self::db_indexes_layout(db_name);
        let schema_name = format!("{}_{}", db_name, "indexes");
        let path = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(schema_name);
        let blks = self
            .storage_mgr
            .borrow_mut()
            .empty_heap_pages(path.to_str().unwrap(), 1);
        TableManager::new(blks, self.storage_mgr.clone(), None, layout)
    }
    fn create_db_schema_table(&mut self, db_name: &str) -> TableManager {
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
        TableManager::new(blks, self.storage_mgr.clone(), None, layout)
    }
    fn create_db_tables_files(&mut self, db_name: &str) -> TableManager {
        let layout = Self::db_tables_file_layout(db_name);
        let schema_name = format!("{}_{}", db_name, "tables_files");
        let path = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(schema_name);
        let blks = self
            .storage_mgr
            .borrow_mut()
            .empty_heap_pages(path.to_str().unwrap(), 1);
        TableManager::new(blks, self.storage_mgr.clone(), None, layout)
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
    fn db_indexes_layout(tablename: &str) -> Rc<Layout> {
        let mut schema = Schema::new();
        schema.add_field(
            "tablename",
            Type::Character(VarChar),
            false,
            false,
            None,
            None,
        );
        schema.add_field(
            "index_name",
            Type::Character(VarChar),
            false,
            false,
            None,
            None,
        );
        schema.add_field(
            "fieldname",
            Type::Character(VarChar),
            false,
            false,
            None,
            None,
        );
        schema.add_field(
            "directory_file",
            Type::Character(VarChar),
            false,
            false,
            None,
            None,
        );
        schema.add_field(
            "index_file",
            Type::Character(VarChar),
            false,
            false,
            None,
            None,
        );
        schema.add_field(
            "index_type",
            Type::Character(VarChar),
            false,
            false,
            None,
            None
        );
        schema.set_name(tablename);
        schema.set_primary_keys(vec![
            "tablename".to_string(),
            "index_name".to_string(),
            "fieldname".to_string(),
        ]);
        Rc::new(schema.to_layout())
    }
    fn db_tables_file_layout(tablename: &str) -> Rc<Layout> {
        let mut schema = Schema::new();
        schema.add_field(
            "tablename",
            Type::Character(VarChar),
            false,
            true,
            None,
            None,
        );
        schema.add_field(
            "filepath",
            Type::Character(VarChar),
            false,
            true,
            None,
            None,
        );
        Rc::new(schema.to_layout())
    }
}
