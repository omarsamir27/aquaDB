use nix::sys::statvfs;
fn main() {
    let stats = statvfs::statvfs("/dev/sdc2");
    println!("{:?}",stats);
}