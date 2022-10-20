
pub struct Page{
    pub payload : Vec<u8>,

}

impl Page {
    fn new() -> Self{
        Page{payload:Vec::new()}
    }

}