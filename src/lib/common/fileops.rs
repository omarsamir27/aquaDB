use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

pub fn read_file(file: &mut File) -> Vec<u8> {
    let mut buffer = vec![];
    file.read_to_end(&mut buffer).unwrap();
    buffer
}

#[cfg(target_os = "linux")]
pub fn file_size(filepath: &PathBuf) -> u64 {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::metadata(filepath).unwrap();
    meta.size()
}

pub fn write_file(filepath: &Path, data: Vec<u8>) -> std::io::Result<()> {
    fs::write(filepath, data)
}

#[cfg(target_os = "windows")]
pub fn file_size(filepath: PathBuf) -> u64 {
    use std::os::windows::fs::MetadataExt;
    let meta = fs::metadata(filepath).unwrap();
    meta.file_size()
}
