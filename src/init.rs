use aqua::meta::catalogmgr::CatalogManager;
use aqua::schema::schema::Schema;
use aqua::schema::types::CharType::VarChar;
use aqua::schema::types::{NumericType, Type};
use aqua::storage::blkmgr::BlockManager;
use aqua::storage::storagemgr::StorageManager;
use aqua::{RcRefCell, AQUADIR, AQUA_HOME_VAR};
use lazy_static::lazy_static;
use std::cell::Cell;
use std::env;
use std::fs::{create_dir, create_dir_all};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::rc::Rc;

const AQUA_HOME_DIR: &str = "AQUA";
lazy_static! {
    static ref AQUA_BASE: &'static Path = Path::new("base/tmp");
    static ref AQUA_GLOBAL: &'static Path = Path::new("global");
}

pub fn init_aqua() {
    init_homedir();
    CatalogManager::init_catalogs();
    let storage = RcRefCell!(StorageManager::new(&AQUADIR(), 4096, 100));
    let mut catalogmgr = CatalogManager::startup(storage.clone());
    let path = Path::new(AQUADIR().as_str()).join("base").join("samir");
    create_dir(path).expect("could not create database dir ");
    catalogmgr.create_db_schema_table("samir");
    let mut schema = Schema::new();
    schema.set_name("omar");
    schema.add_field_default_constraints("id", Type::Numeric(NumericType::Integer), None);
    schema.add_field_default_constraints("name", Type::Character(VarChar), None);
    schema.set_primary_keys(vec!["id".to_string()]);
    catalogmgr.add_schema("samir", &schema);
    let omar = catalogmgr.get_schema("samir", "omar");
    dbg!(omar);
}

fn init_homedir() {
    let AQUA_HOME_PATH = init_AQUADATA();
    let path = Path::new(AQUA_HOME_PATH.as_str());
    create_dir(path).expect("Could not create Aqua Directory");
    env::set_current_dir(path).unwrap();
    create_dir_all(AQUA_BASE.clone()).unwrap();
    create_dir_all("global").expect("Could not create Aqua Data Directory");
}

fn get_user_homedir() -> Option<String> {
    match env::var("HOME") {
        Ok(home) if Path::is_dir(home.as_ref()) => Some(home),
        _ => None,
    }
}

fn init_AQUADATA() -> String {
    if let Ok(aquadata) = env::var(AQUA_HOME_VAR) {
        aquadata
    } else {
        match get_user_homedir() {
            Some(userhome) => {
                let val = PathBuf::from(userhome).join(AQUA_HOME_DIR);
                env::set_var(AQUA_HOME_VAR, val.to_str().unwrap());
                val.to_str().unwrap().to_string()
            }
            None => {
                eprintln!("User directory is not set");
                exit(2)
            }
        }
    }
}

// fn init_global(){
//     let mut storagemgr = StorageManager::new(AQUADIR().as_str(),4096,10);
//     // let mut blkmgr = BlockManager::new(,4096);
//     // blkmgr.extend_file_many("global/aqua_database",10);
//     storagemgr.extend_file_many("global/aqua_database",20);
// }
