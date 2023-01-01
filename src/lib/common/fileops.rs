use std::fs::File;
use std::io::Read;

pub fn read_file(file: &mut File) -> Vec<u8> {
    let mut buffer = vec![];
    file.read_to_end(&mut buffer).unwrap();
    buffer
}
