use std::ffi::c_int;
use aqua::schema::types::{CharType, NumericType, Type};
use aqua::schema::schema::Schema;
use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;

trait RandomTypeBytes {
    fn random(self) -> Vec<u8>;
}

impl RandomTypeBytes for NumericType {
    fn random(self) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        match self {
            NumericType::SmallInt => rng.gen_range(i16::MIN..i16::MAX).to_ne_bytes().to_vec(),
            NumericType::Integer | NumericType::Serial => {
                rng.gen_range(i32::MIN..i32::MAX).to_ne_bytes().to_vec()
            }
            NumericType::BigInt => rng.gen_range(i64::MIN..i64::MAX).to_ne_bytes().to_vec(),
            NumericType::Single => rng.gen_range(f32::MIN..f32::MAX).to_ne_bytes().to_vec(),
            NumericType::Double => rng.gen_range(f64::MIN..f64::MAX).to_ne_bytes().to_vec()
        }
    }
}

impl RandomTypeBytes for CharType{
    fn random(self) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let len = rng.gen_range(1..300_usize);
        let string : String =
            thread_rng().sample_iter(&Alphanumeric).take(len).map(char::from).collect();
        string.as_bytes().to_vec()
    }
}

impl RandomTypeBytes for Type{
    fn random(self) -> Vec<u8> {
        match self {
            Type::Numeric(n) => n.random(),
            Type::Character(c) => c.random()
        }
    }
}

fn generate_random_tuple(schema:&Vec<(&str,Type)>) -> Vec<(String,Vec<u8>)>{
    schema.iter().map(|(name,fldtype)| (name.to_string(),fldtype.random())).collect()
}

pub fn generate_random_tuples(schema:&Vec<(&str,Type)>,count:u32) -> Vec<Vec<(String,Vec<u8>)>>{
    vec![ generate_random_tuple(schema);count as usize ]
}