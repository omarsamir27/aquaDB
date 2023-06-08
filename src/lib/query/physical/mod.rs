pub mod realize;
mod utils;

use utils::*;

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
use crate::table::btree_iter::BtreeIter;
use crate::table::hash_iter::HashIter;
use crate::table::heap_iter::TableIter;

type TypeMap = HashMap<FieldId, Type>;

const MAX_WORKING_MEM : usize = 4e3 as usize;

impl Debug for AccessMethod{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self{
            AccessMethod::HeapIter(_,_) => write!(f,"HeapIter"),
            AccessMethod::HashIter(_,_) => write!(f,"HashIter"),
            AccessMethod::BtreeIter(_,_) => write!(f,"BtreeIter"),
            _ => unreachable!()
        }
    }
}

impl Iterator for AccessMethod{
    type Item = HashMap<FieldId,Option<Vec<u8>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let (s,Some(next)) = match self{
            AccessMethod::HeapIter(s, t) => (s,t.next()),
            AccessMethod::HashIter(s, h) => (s,h.next()),
            AccessMethod::BtreeIter(s, b) => (s,b.next()),
            _ => unreachable!()
        }
        {
                Some(row_to_merged_row(s, next))
        }
        else {
            None
        }
    }
}

pub enum AccessMethod{
    HeapIter(String,TableIter),
    HashIter(String,HashIter),
    BtreeIter(String,BtreeIter)
}
impl AccessMethod{
    fn load_key(&mut self,key:&[u8]){
        match self{
            AccessMethod::HashIter(s,h) => h.load_key(key),
            AccessMethod::BtreeIter(s,b) => b.load_key(key),
            AccessMethod::HeapIter(_, _) => unreachable!("Heap Does not Support Loading Keys")
        }
    }
}



#[derive(Debug)]
pub enum PhysicalNode {
    Project(Project),
    Select(Select),
    MergeJoin(MergeJoin),
    IndexedLoopJoin(IndexedJoin),
    AccessPath(Box<AccessMethod>),
    RemoveDuplicates(DeDup),
    Sort(Sort)
}

impl PhysicalNode{
    fn load_key(&mut self,key:&[u8]){
        match self{
            PhysicalNode::AccessPath(a) => a.load_key(key),
            _ => unreachable!()
        }
    }
    pub fn get_type_map(&self) -> TypeMap{
        match self{
            PhysicalNode::Project(a) => a.fields_map.clone(),
            PhysicalNode::Select(a) => a.fields_map.clone(),
            PhysicalNode::MergeJoin(a) => a.fields_map.clone(),
            PhysicalNode::IndexedLoopJoin(a) => a.fields_map.clone(),
            PhysicalNode::AccessPath(_) => unreachable!(),
            PhysicalNode::RemoveDuplicates(a) => a.fields_map.clone(),
            PhysicalNode::Sort(a) => a.fields_map.clone(),
        }
    }
}

impl Iterator for PhysicalNode {
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        match self{
            PhysicalNode::Project(a) => a.next(),
            PhysicalNode::Select(a) => a.next(),
            PhysicalNode::MergeJoin(a) => a.next(),
            PhysicalNode::IndexedLoopJoin(a) => a.next(),
            PhysicalNode::AccessPath(a) => a.next(),
            PhysicalNode::RemoveDuplicates(a) => a.next(),
            PhysicalNode::Sort(a) => a.next(),
        }
    }
}

// impl Default for PhysicalNode {
//     fn default() -> Self {
//         Self::Empty
//     }
// }

#[derive(Debug)]
pub struct Project {
    fields_map : TypeMap,
    child: Box<PhysicalNode>,
}

impl Project {}
impl Iterator for Project {
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(mut next) = self.child.next() {
            next.retain(|field, _| self.fields_map.contains_key(field));
            Some(next)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct Select {
    fields_map : TypeMap,
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
            fill_ctx_map(&mut self.context,&next,&self.fields_map);
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
    fields_map : TypeMap,
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
            // let next = next.into_iter().map(|(k,v)| (k.field,v)).collect();
            self.left_table.as_mut().unwrap().add_row_map(next);
        }
        for next in self.right.by_ref(){
            // let next = next.into_iter().map(|(k,v)| (k.field,v)).collect();
            self.right_table.as_mut().unwrap().add_row_map(next);
        }
        self.left_table.as_mut().unwrap().sort(&self.eq_fields.0);
        self.right_table.as_mut().unwrap().sort(&self.eq_fields.1);
        self.left_iter = Some(self.left_table.take().unwrap().into_iter());
        self.right_iter = Some(self.right_table.take().unwrap().into_iter());
        self.loaded = true;
    }
    pub fn new(fields_map:TypeMap,left: Box<PhysicalNode>, right: Box<PhysicalNode>, eq_fields: (FieldId, FieldId),left_headers:TypeMap,right_headers:TypeMap) -> Self {
        let left_table = Some(TupleTable::new(&eq_fields.0.table, left_headers, MAX_WORKING_MEM));
        let right_table = Some(TupleTable::new(&eq_fields.1.table, right_headers, MAX_WORKING_MEM));
        Self { fields_map ,left, right, left_table, right_table, left_iter: None, right_iter: None, eq_fields, loaded: false, current_left_row: None }
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
    fields_map : TypeMap,
    eq_fields: (FieldId,FieldId),
    current_left_row : Option<MergedRow>,
    left : Box<PhysicalNode>,
    right : Box<PhysicalNode>
}

impl IndexedJoin {
    pub fn new(fields_map:TypeMap,eq_fields: (FieldId, FieldId), left: Box<PhysicalNode>, right: Box<PhysicalNode>) -> Self {
        Self { fields_map,eq_fields, current_left_row: None, left, right }
    }
}

impl Iterator for IndexedJoin{
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        let right_table_name = &self.eq_fields.1.table;
        if let Some(left) = &self.current_left_row{
            if let Some(right) = self.right.next(){
                return Some(merge(left, right))
            }
            else if let Some(left) = &self.left.next(){
                self.current_left_row.replace(left.clone());
                let key = left.get(&self.eq_fields.0).unwrap().as_ref().unwrap();
                self.right.load_key(key);
                if let Some(right) = self.right.next(){
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
    fields_map : TypeMap,
    child : Box<PhysicalNode>,
    current_row : Option<MergedRow>,
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
    fields_map : TypeMap,
    child : Box<PhysicalNode>,
    table : Option<TupleTable>,
    table_iter : Option<TupleTableIter>,
    field : FieldId,
    loaded : bool
}

impl Sort{
    fn load(&mut self){
        for next in self.child.by_ref(){
            // let next = next.into_iter().map(|(k,v)| (k.field,v)).collect();
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



