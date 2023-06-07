mod realize;

use crate::common::numerical::ByteMagic;
use crate::FieldId;
use crate::schema::types::{NumericType, Type};
use super::tuple_table::TupleTable;
use super::MergedRow;
use evalexpr::Value::Float;
use evalexpr::{
    ContextWithMutableVariables, FloatType, HashMapContext, IntType, IterateVariablesContext, Value,
};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::path::Iter;
use crate::query::tuple_table::TupleTableIter;
use crate::table::hash_iter::HashIter;
use crate::table::heap_iter::TableIter;

type IndexMap = HashMap<FieldId, Type>;

impl Debug for AccessPath{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self{
            AccessPath::HeapIter(_) => write!(f,"HeapIter"),
            AccessPath::HashIter(_) => write!(f,"HashIter")

        }
    }
}

impl Iterator for AccessPath{
    type Item = HashMap<String,Option<Vec<u8>>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self{
            AccessPath::HeapIter(t) => t.next(),
            AccessPath::HashIter(h) => h.next()
        }
    }
}

pub enum AccessPath{
    HeapIter(TableIter),
    HashIter(HashIter),
    // BTREE ITER
}
impl AccessPath{
    fn load_key(&mut self,key:&[u8]){
        match self{
            AccessPath::HashIter(h) => h.load_key(key),
            AccessPath::HeapIter(_) => unreachable!("Heap Does not Support Loading Keys")
        }
    }
}



#[derive(Debug)]
pub enum PhysicalNode {
    Project(Project),
    Select(Select),
    MergeJoin(MergeJoin),
    IndexedLoopJoin(IndexedJoin),
    AccessPath(Box<AccessPath>),
    RemoveDuplicates(DeDup),
    Sort(Sort)
}

impl Iterator for PhysicalNode {
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

// impl Default for PhysicalNode {
//     fn default() -> Self {
//         Self::Empty
//     }
// }

#[derive(Debug)]
pub struct Project {
    fields: HashSet<FieldId>,
    child: Box<PhysicalNode>,
}

impl Project {}
impl Iterator for Project {
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(mut next) = self.child.next() {
            next.retain(|field, _| self.fields.contains(field));
            Some(next)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct Select {
    condition: evalexpr::Node,
    context: HashMapContext,
    child: Box<PhysicalNode>,
}

impl Select {

}

impl Iterator for Select {
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        for next in self.child.by_ref() {
            fill_ctx_map(&mut self.context,&next);
            if self
                .condition
                .eval_boolean_with_context(&self.context)
                .unwrap()
            {
                return Some(next);
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct MergeJoin {
    left : Box<PhysicalNode>,
    right : Box<PhysicalNode>,
    left_table : Option<TupleTable>,
    right_table : Option<TupleTable>,
    left_iter : Option<TupleTableIter>,
    right_iter : Option<TupleTableIter>,
    eq_fields : (FieldId,FieldId),
    loaded : bool,
    current_left_row : Option<MergedRow>
}

impl MergeJoin {
    fn load(&mut self){
        for next in self.left.by_ref(){
            let next = next.into_iter().map(|(k,v)| (k.field,v)).collect();
            self.left_table.as_mut().unwrap().add_row_map(next);
        }
        for next in self.right.by_ref(){
            let next = next.into_iter().map(|(k,v)| (k.field,v)).collect();
            self.right_table.as_mut().unwrap().add_row_map(next);
        }
        self.left_table.as_mut().unwrap().sort(self.eq_fields.0.field.as_str());
        self.right_table.as_mut().unwrap().sort(self.eq_fields.1.field.as_str());
        self.left_iter = Some(self.left_table.take().unwrap().into_iter());
        self.right_iter = Some(self.right_table.take().unwrap().into_iter());
        self.loaded = true;
    }
}
impl Iterator for MergeJoin{
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.loaded{
            self.load();
        }
        let mut left_iter = self.left_iter.as_mut().unwrap();
        if self.current_left_row.is_none(){
            let left = left_iter.next();
            if left.is_none(){
                None
            }
            else {
                self.current_left_row = left;
                self.next()
            }
        }
        else {
            if let Some(right) = self.right_iter.as_mut().unwrap().next(){
                let left = self.current_left_row.as_ref().unwrap();
                if left.get(&self.eq_fields.0) == right.get(&self.eq_fields.1){
                    return Some(merge(left,right))
                }
                else if let Some(new_left) = left_iter.next(){
                    self.current_left_row.replace(new_left.clone());
                    if new_left.get(&self.eq_fields.0) == right.get(&self.eq_fields.1){
                        return Some(merge(&new_left,right))
                    }
                }
                else {
                    return None
                }
            }
            None
        }
        // if let Some(left) = self.left_iter.as_mut().unwrap().next(){
        //     while let Some(right) = self.right_iter.as_mut().unwrap().next(){
        //         if left.get(&self.eq_fields.0) == right.get(&self.eq_fields.1){
        //             return Some(merge(&left, right));
        //         }else {
        //             break
        //         }
        //     }
        //     return None
        // }
        // None
    }
}

#[derive(Debug)]
pub struct IndexedJoin{
    eq_fields: (FieldId,FieldId),
    current_left_row : Option<MergedRow>,
    left : Box<PhysicalNode>,
    right : AccessPath
}
impl Iterator for IndexedJoin{
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        let right_table_name = &self.eq_fields.1.table;
        if let Some(left) = &self.current_left_row{
            if let Some(right) = self.right.next(){
                let right = row_to_merged_row(right_table_name,right);
                return Some(merge(left, right))
            }
            else if let Some(left) = &self.left.next(){
                self.current_left_row.replace(left.clone());
                let key = left.get(&self.eq_fields.0).unwrap().as_ref().unwrap();
                self.right.load_key(key);
                if let Some(right) = self.right.next(){
                    let right = row_to_merged_row(right_table_name,right);
                    return Some(merge(left, right))
                }
            }
        }
        else {
                let left = self.left.next();
                if left.is_some(){
                    self.current_left_row = left;
                    return self.next()
                }
        }
        None
    }

}

#[derive(Debug)]
pub struct DeDup{
    child : Box<PhysicalNode>,
    current_row : Option<MergedRow>,
    loaded:bool,
    sort_on : String
}
impl DeDup{

}
impl Iterator for DeDup{
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {

        if self.current_row.is_none(){
            let next = self.child.next();
            self.current_row = next.clone();
            return next
        }
        for next in self.child.by_ref() {
            if next != *self.current_row.as_ref().unwrap(){
                self.current_row.replace(next.clone());
                return Some(next)
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct Sort{
    child : Box<PhysicalNode>,
    table : Option<TupleTable>,
    table_iter : Option<TupleTableIter>,
    field : String,
    loaded : bool
}

impl Sort{
    fn load(&mut self){
        for next in self.child.by_ref(){
            let next = next.into_iter().map(|(k,v)| (k.field,v)).collect();
            self.table.as_mut().unwrap().add_row_map(next);
        }
        self.table.as_mut().unwrap().sort(&self.field);
        self.table_iter.replace(self.table.take().unwrap().into_iter());
    }
}

impl Iterator for Sort{
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.loaded{self.load();}
        self.table_iter.as_mut().unwrap().next()
    }
}



fn fill_ctx_map(ctx:&mut HashMapContext, row: &MergedRow) {
    let idx: IndexMap = HashMap::new();
    for table_var in ctx.iter_variable_names().collect::<Vec<_>>() {
        let field_id = table_var.parse::<FieldId>().unwrap();
        let val = row.get(&field_id).unwrap();
        let val = data_to_value(val.as_ref(), *idx.get(&field_id).unwrap());
        ctx.set_value(table_var, val);
    }
}
fn data_to_value(data: Option<&Vec<u8>>, schema_type: Type) -> Value {
    if let Some(data) = data {
        match schema_type {
            Type::Numeric(n) => match n {
                NumericType::SmallInt => Value::Int(IntType::from(data.to_i16())),
                NumericType::Integer => Value::Int(IntType::from(data.to_i32())),
                NumericType::BigInt => Value::Int(IntType::from(data.to_i64())),
                NumericType::Single => Value::Float(data.to_f32() as FloatType),
                NumericType::Double => Value::Float(data.to_f32() as FloatType),
                NumericType::Serial => Value::Int(IntType::from(data.to_i32())),
            },
            Type::Character(c) => Value::String(String::from_utf8(data.to_vec()).unwrap()),
            Type::Boolean => Value::Boolean(data[0] == 1),
        }
    } else {
        Value::Empty
    }
}

fn row_to_merged_row(table:&str,row:HashMap<String,Option<Vec<u8>>>)->MergedRow{
    row.into_iter().map(|(k,v)| (FieldId::new(table,&k),v) ).collect()
}

fn merge(left:&MergedRow,right:MergedRow) -> MergedRow{
    let mut left = left.clone();
    left.extend(right);
    left
}
