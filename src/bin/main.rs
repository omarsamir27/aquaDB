use btreemultimap::BTreeMultiMap;

fn main() {
    let mut btree = BTreeMultiMap::new();
    btree.insert(50_u16, 1);
    btree.insert(100_u16, 1);
    btree.insert(100_u16, 2);
    btree.insert(105, 5);
    for (key, value) in btree.iter() {
        println!("key: {:?}, val: {:?}", key, value);
    }

    let mut iterator = btree.iter_mut();
    let value = iterator.find(|(&k, _)| k >= 100_u16);
    println!("{:?}", value);
    for (key, value) in btree.iter() {
        println!("key: {:?}, val: {:?}", key, value);
    }
}