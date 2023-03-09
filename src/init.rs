use std::env;
use std::fs::{create_dir_all};
use std::path::{Path, PathBuf};
use std::process::exit;
use lazy_static::lazy_static;

const AQUA_HOME_VAR : &str = "AQUADATA";
const AQUA_HOME_DIR : &str = "AQUA";
lazy_static!{
    static ref AQUA_BASE : &'static Path = Path::new("base/tmp");
}

pub fn init_aqua(){
    init_homedir();

}

fn init_homedir(){
    // set_env::set(AQUA_HOME_VAR,home).expect("Could not set Aqua Home Directory");
    // dbg!(set_env::get(AQUA_HOME_VAR));
    let AQUA_HOME_PATH = init_AQUADATA();
    // dbg!(env::var(AQUA_HOME_VAR));
    let path = Path::new(&AQUA_HOME_PATH).join(AQUA_BASE.clone());
    create_dir_all(path).expect("Could not create Aqua Data Directory");

}

fn get_user_homedir() -> Option<String>{
    match env::var("HOME"){
        Ok(home) if Path::is_dir(home.as_ref()) => Some(home),
        _ => None
    }
}

fn init_AQUADATA() -> String{
    if let Ok(aquadata) = env::var(AQUA_HOME_VAR){
        aquadata
    }else {
        match get_user_homedir() {
            Some(userhome) => {
                let val = PathBuf::from(userhome).join(AQUA_HOME_DIR);
                env::set_var(AQUA_HOME_VAR,val.to_str().unwrap());
                val.to_str().unwrap().to_string()
            },
            None => {eprintln!("User directory is not set"); exit(2)}
        }
    }
}