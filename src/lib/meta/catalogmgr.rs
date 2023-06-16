use crate::common::btree_multimap::BTreeMultimap;
use crate::index::Index;
use crate::schema::schema::{Layout, Schema};
use crate::schema::types::CharType::VarChar;
use crate::schema::types::{CharType, NumericType, Type};
use crate::storage::blkmgr::BlockManager;
use crate::storage::blockid::BlockId;
use crate::storage::free_space::FreeMap;
use crate::storage::heap::HeapPage;
use crate::storage::storagemgr::StorageManager;
use crate::table::tablemgr::TableManager;
use crate::{RcRefCell, AQUADIR};
use std::collections::{HashMap, HashSet};
use std::fs::create_dir;
use std::path::{Path, PathBuf};
use std::{cell::RefCell, rc::Rc};

struct InstanceCatalog {
    db_name: String,
    schemas: TableManager,
    tables_filepaths: TableManager,
    indexes: TableManager,
}

impl InstanceCatalog {
    fn get_db_tables(
        &self,
        storage: &Rc<RefCell<StorageManager>>,
    ) -> HashMap<String, TableManager> {
        let tables = self
            .tables_filepaths
            .heapscan_iter()
            .map(|mut r| {
                (
                    String::from_utf8(r.remove("tablename").unwrap().unwrap()).unwrap(),
                    (
                        String::from_utf8(r.remove("filepath").unwrap().unwrap()).unwrap(),
                        String::from_utf8(r.remove("freemap").unwrap().unwrap()).unwrap(),
                    ),
                )
            })
            .collect::<HashMap<String, (String, String)>>();
        let db_path = Path::new(AQUADIR().as_str())
            .join("base")
            .join(&self.db_name);
        tables
            .into_iter()
            .map(|(name, (heap_path, freemap_file))| {
                let schema = self.get_schema(&name);
                let indexes = schema
                    .indexes()
                    .iter()
                    .map(|idx| idx.to_index_info(self.db_name.as_str()))
                    .collect();
                let table = TableManager::from_file(
                    storage.clone(),
                    db_path.join(heap_path),
                    Rc::new(schema.to_layout()),
                    indexes,
                    db_path.join(freemap_file),
                );
                (name, table)
            })
            .collect()
    }
    fn get_schema_indexes(&self, schema_name: &str) -> Vec<HashMap<String, Option<Vec<u8>>>> {
        self.indexes
            .heapscan_iter()
            .filter(|row| row.get("tablename").unwrap().as_ref().unwrap() == schema_name.as_bytes())
            .collect()
    }
    fn get_schema(&self, schema_name: &str) -> Schema {
        let schemas_catalog = self.schemas.heapscan_iter();
        let schema_vec = schemas_catalog
            .filter(|row| row.get("tablename").unwrap().as_ref().unwrap() == schema_name.as_bytes())
            .collect::<Vec<_>>();
        Schema::deserialize(schema_vec, self.get_schema_indexes(schema_name))
    }
    fn add_schema(
        &mut self,
        schema: &Schema,
        storage: Rc<RefCell<StorageManager>>,
    ) -> Result<TableManager, String> {
        let mut catalog_iter = self.tables_filepaths.heapscan_iter();
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
        let indexes = schema
            .indexes()
            .iter()
            .map(|idx| idx.to_index_info(self.db_name.as_str()));
        for idx in indexes {
            Index::init_index(idx, storage.clone());
        }
        let mut schema_catalog = &mut self.schemas;
        let (serde_schema, mut serde_indexes) = schema.serialize();
        for field in serde_schema {
            schema_catalog.try_insert_tuple(field);
        }
        //
        let mut tablesfiles_catalog = &mut self.tables_filepaths;
        tablesfiles_catalog.try_insert_tuple(vec![
            (
                "tablename".to_string(),
                Some(schema.name().as_bytes().to_vec()),
            ),
            (
                "filepath".to_string(),
                Some(format!("{}_heap0", &schema.name()).into_bytes()),
            ),
            (
                "freemap".to_string(),
                Some(format!("{}_freemap", &schema.name()).into_bytes()),
            ),
        ]);

        let mut indexes_catalog = &mut self.indexes;
        for idx in serde_indexes {
            indexes_catalog.try_insert_tuple(idx);
        }
        tablesfiles_catalog.flush_all();
        schema_catalog.flush_all();
        indexes_catalog.flush_all();
        let table_path = Path::new(AQUADIR().as_str())
            .join("base")
            .join(&self.db_name)
            .join(format!("{}_heap0", &schema.name()));
        let indexes = schema
            .indexes()
            .iter()
            .map(|idx| idx.to_index_info(self.db_name.as_str()))
            .collect();
        let freemap_file = Path::new(AQUADIR().as_str())
            .join("base")
            .join(&self.db_name)
            .join(format!("{}_freemap", &schema.name()));
        FreeMap::init(freemap_file.clone(), 0, &BlockId::new("", 0));

        let table = TableManager::from_file(
            storage,
            table_path,
            Rc::new(schema.to_layout()),
            indexes,
            freemap_file,
        );
        Ok(table)
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
                    db_name: db.clone(),
                    schemas,
                    indexes,
                    tables_filepaths,
                };
                (db, instance)
            })
            .collect();
        Self::new(storagemgr, databases_tbl, db_tbl_schema_catalogs)
    }
    pub fn get_db_tables(&self, db_name: &str) -> HashMap<String, TableManager> {
        self.databases_catalogs
            .get(db_name)
            .unwrap()
            .get_db_tables(&self.storage_mgr)
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
    pub fn add_schema(&mut self, db_name: &str, schema: &Schema) -> Result<TableManager, String> {
        let mut db_catalog = self
            .databases_catalogs
            .get_mut(db_name)
            .ok_or("Database does not exist")?;
        // IF THIS IS INDEXABLE THEN BETTER
        db_catalog.add_schema(schema, self.storage_mgr.clone())
    }
    fn load_dbs_table(storage: &Rc<RefCell<StorageManager>>) -> TableManager {
        let database_tbl_file = Path::new(AQUADIR().as_str())
            .join("global")
            .join("aqua_database");
        let freemap_file = Path::new(AQUADIR().as_str())
            .join("global")
            .join("aqua_freemap");
        TableManager::from_file(
            storage.clone(),
            database_tbl_file,
            Self::dbs_table_layout(),
            vec![],
            freemap_file,
        )
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
        let freemap_file = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(format!("{}_freemap", schema_name));
        TableManager::from_file(
            storage.clone(),
            db_schema_file,
            Self::db_tables_file_layout(schema_name.as_str()),
            vec![],
            freemap_file,
        )
    }
    fn load_db_schema_table(storage: &Rc<RefCell<StorageManager>>, db_name: &str) -> TableManager {
        let schema_name = format!("{}_{}", db_name, "schemas");
        let db_schema_file = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(schema_name.as_str());
        let freemap_file = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(format!("{}_freemap", schema_name));
        TableManager::from_file(
            storage.clone(),
            db_schema_file,
            Self::db_schema_layout(schema_name.as_str()),
            vec![],
            freemap_file,
        )
    }
    fn load_db_indexes_table(storage: &Rc<RefCell<StorageManager>>, db_name: &str) -> TableManager {
        let schema_name = format!("{}_{}", db_name, "indexes");
        let db_schema_file = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(schema_name.as_str());
        let freemap_file = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(format!("{}_freemap", schema_name));
        TableManager::from_file(
            storage.clone(),
            db_schema_file,
            Self::db_indexes_layout(schema_name.as_str()),
            vec![],
            freemap_file,
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
        let freemap_file = Path::new(AQUADIR().as_str())
            .join("global")
            .join("aqua_freemap");
        let mut freemap = FreeMap::init(
            freemap_file,
            HeapPage::default_free_space(storage.borrow().blk_size()) as u16,
            &blks[0],
        );
        TableManager::new(blks, storage, freemap, layout, vec![])
        // TableManager::new(blks, storage, None, layout, vec![])
    }
    pub fn create_database(&mut self, db_name: &str) -> Result<(), String> {
        if self.databases_catalogs.contains_key(db_name) {
            return Err(format!("Database {} already exists", db_name));
        }
        let path = Path::new(AQUADIR().as_str()).join("base").join(db_name);
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
                db_name: db_name.to_string(),
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
            .join(schema_name.clone());
        let blks = self
            .storage_mgr
            .borrow_mut()
            .empty_heap_pages(path.to_str().unwrap(), 1);
        let freemap_file = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(format!("{}_freemap", schema_name));
        let mut freemap = FreeMap::init(
            freemap_file,
            HeapPage::default_free_space(self.storage_mgr.borrow().blk_size()) as u16,
            &blks[0],
        );
        // freemap.add_blockspace(HeapPage::default_free_space(self.storage_mgr.borrow().blk_size()) as u16,&blks[0]);
        TableManager::new(blks, self.storage_mgr.clone(), freemap, layout, vec![])
        // TableManager::new(blks, self.storage_mgr.clone(), None, layout, vec![])
    }
    fn create_db_schema_table(&mut self, db_name: &str) -> TableManager {
        let layout = Self::db_schema_layout(db_name);
        let schema_name = format!("{}_{}", db_name, "schemas");
        let path = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(schema_name.clone());
        let blks = self
            .storage_mgr
            .borrow_mut()
            .empty_heap_pages(path.to_str().unwrap(), 1);
        let freemap_file = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(format!("{}_freemap", schema_name));
        let mut freemap = FreeMap::init(
            freemap_file,
            HeapPage::default_free_space(self.storage_mgr.borrow().blk_size()) as u16,
            &blks[0],
        );
        // freemap.add_blockspace(HeapPage::default_free_space(self.storage_mgr.borrow().blk_size()) as u16,&blks[0]);
        TableManager::new(blks, self.storage_mgr.clone(), freemap, layout, vec![])
    }
    fn create_db_tables_files(&mut self, db_name: &str) -> TableManager {
        let layout = Self::db_tables_file_layout(db_name);
        let schema_name = format!("{}_{}", db_name, "tables_files");
        let path = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(schema_name.clone());
        let blks = self
            .storage_mgr
            .borrow_mut()
            .empty_heap_pages(path.to_str().unwrap(), 1);
        let freemap_file = Path::new(AQUADIR().as_str())
            .join("base")
            .join(db_name)
            .join(format!("{}_freemap", schema_name));

        let mut freemap = FreeMap::init(
            freemap_file,
            HeapPage::default_free_space(self.storage_mgr.borrow().blk_size()) as u16,
            &blks[0],
        );
        // freemap.add_blockspace(HeapPage::default_free_space(self.storage_mgr.borrow().blk_size()) as u16,&blks[0]);
        TableManager::new(blks, self.storage_mgr.clone(), freemap, layout, vec![])
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
            None,
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
        schema.add_field("freemap", Type::Character(VarChar), false, true, None, None);
        Rc::new(schema.to_layout())
    }
}
