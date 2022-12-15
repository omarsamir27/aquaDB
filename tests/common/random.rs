use std::sync::RwLock;
use lazy_static::lazy_static;
use aqua::schema::schema::Schema;
use aqua::schema::types::{CharType, NumericType, Type};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use rand::rngs::ThreadRng;
use names::{NOUNS,ADJECTIVES};



trait RandomTypeBytes {
    fn random(self,rng:&mut ThreadRng) -> Vec<u8>;
}

impl RandomTypeBytes for NumericType {
    fn random(self,rng:&mut ThreadRng) -> Vec<u8> {
        match self {
            NumericType::SmallInt => rng.gen_range(i16::MIN..i16::MAX).to_ne_bytes().to_vec(),
            NumericType::Integer | NumericType::Serial => {
                rng.gen_range(i32::MIN..i32::MAX).to_ne_bytes().to_vec()
            }
            NumericType::BigInt => rng.gen_range(i64::MIN..i64::MAX).to_ne_bytes().to_vec(),
            NumericType::Single => rng.gen_range(f32::MIN..f32::MAX).to_ne_bytes().to_vec(),
            NumericType::Double => rng.gen_range(f64::MIN..f64::MAX).to_ne_bytes().to_vec(),
        }
    }
}

impl RandomTypeBytes for CharType {
    fn random(self,rng:&mut ThreadRng) -> Vec<u8> {
        let noun_idx = rng.gen_range(0..NOUNS.len());
        let adj_idx = rng.gen_range(0..ADJECTIVES.len());
        let mut string = String::new();
        string.push_str(NOUNS[noun_idx]);
        string.push('-');
        string.push_str(ADJECTIVES[adj_idx]);
        string.into_bytes()

    }
}

impl RandomTypeBytes for Type {
    fn random(self,rng:&mut ThreadRng ) -> Vec<u8> {
        match self {
            Type::Numeric(n) => n.random(rng),
            Type::Character(c) => c.random(rng),
        }
    }
}

fn generate_random_tuple(schema: &Vec<(String, Type)>,rng:&mut ThreadRng) -> Vec<(String, Option<Vec<u8>>)> {
    schema
        .iter()
        .map(|(name, fldtype)| (name.to_string(), Some(fldtype.random(rng))))
        .collect()
}

pub fn generate_random_tuples(
    schema: &Vec<(String, Type)>,
    count: u32,
) -> Vec<Vec<(String, Option<Vec<u8>>)>> {
    let mut rng = thread_rng();
    vec![generate_random_tuple(schema,&mut rng); count as usize]
}

pub fn distill_schema(schema: Schema) -> Vec<(String, Type)> {
    let fields = schema.fields();
    fields
        .into_iter()
        .map(|field| (field.name().to_string(), field.field_type()))
        .collect()
}
