#![allow(non_snake_case)]


use std::collections::BTreeMap;

fn main() {
    let mut tree : BTreeMap<i32,Vec<i32>> = BTreeMap::new();
    tree.insert(100,vec![200,300,400]);
    tree.insert(105,vec![100,500,140]);

    let x = tree.range(70..104);
        print!("{:?}",x);
}