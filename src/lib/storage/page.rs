pub struct Page {
    pub payload: Vec<u8>,
}

impl Page {
    pub fn new(page_size: usize) -> Self {
        Page {
            payload: Vec::with_capacity(page_size),
        }
    }
}

// impl Default for Page{
//     fn default() -> Self {
//         Page{
//             payload : vec![1;]
//         }
//     }
// }
