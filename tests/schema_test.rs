mod common;

use aqua::schema::schema::Schema;
use aqua::schema::types::CharType::VarChar;
use aqua::schema::types::NumericType::{Integer, SmallInt};
use aqua::schema::types::Type;
use common::random;

// #[test]
// fn tuple_creation(){
//     let schema = vec![
//         ("id",Type::Numeric(SmallInt)),
//         ("name",Type::Character(VarChar)),
//         ("salary",Type::Numeric(Integer))
//     ];
//     let tups = random::generate_random_tuples(&schema,100);
//     for t in tups{
//         println!("{:?}",&t);
//     }
// }

#[test]
fn layout_test() {
    let mut schema = Schema::new();
    let schema_vec = vec![
        ("id", Type::Numeric(SmallInt), false, None),
        ("name", Type::Character(VarChar), false, None),
        ("salary", Type::Numeric(Integer), false, None),
        ("job", Type::Character(VarChar), false, None),
    ];
    for attr in schema_vec {
        schema.add_field(attr.0, attr.1, attr.2, attr.3);
    }
    let layout = schema.to_layout();
    println!("{:?}", layout)
}
