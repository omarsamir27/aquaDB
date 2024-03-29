pub mod realize;
mod utils;

use utils::*;

use super::tuple_table::TupleTable;
use super::MergedRow;
use crate::common::numerical::ByteMagic;
use crate::query::algebra::GroupBy;
use crate::query::concrete_types::ConcreteType;
use crate::query::tuple_table::TupleTableIter;
use crate::schema::schema::Field;
use crate::schema::types::{NumericType, Type};
use crate::sql::query::select::AggregateFunc;
use crate::table::btree_iter::BtreeIter;
use crate::table::hash_iter::HashIter;
use crate::table::heap_iter::TableIter;
use crate::{AggregateField, FieldId};
use chrono::format::Item;
use evalexpr::Value::Float;
use evalexpr::{
    ContextWithMutableVariables, FloatType, HashMapContext, IntType, IterateVariablesContext, Value,
};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::fs::write;
use std::process::exit;
// use genawaiter::

type TypeMap = HashMap<FieldId, Type>;

const MAX_WORKING_MEM: usize = 16e3 as usize;

impl Debug for AccessMethod {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AccessMethod::HeapIter(_, _) => write!(f, "HeapIter"),
            AccessMethod::HashIter(_, _) => write!(f, "HashIter"),
            AccessMethod::BtreeIter(_, _) => write!(f, "BtreeIter"),
            _ => unreachable!(),
        }
    }
}

impl Iterator for AccessMethod {
    type Item = HashMap<FieldId, Option<Vec<u8>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let (s, Some(next)) = match self {
            AccessMethod::HeapIter(s, t) => (s, t.next()),
            AccessMethod::HashIter(s, h) => (s, h.next()),
            AccessMethod::BtreeIter(s, b) => (s, b.next()),
            _ => unreachable!(),
        } {
            Some(row_to_merged_row(s, next))
        } else {
            None
        }
    }
}

pub enum AccessMethod {
    HeapIter(String, TableIter),
    HashIter(String, HashIter),
    BtreeIter(String, BtreeIter),
}
impl AccessMethod {
    fn load_key(&mut self, key: &[u8]) {
        match self {
            AccessMethod::HashIter(s, h) => h.load_key(key),
            AccessMethod::BtreeIter(s, b) => b.load_key(key),
            AccessMethod::HeapIter(_, _) => unreachable!("Heap Does not Support Loading Keys"),
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
    Sort(Sort),
    GroupBy(Grouper),
}

impl PhysicalNode {
    fn load_key(&mut self, key: &[u8]) {
        match self {
            PhysicalNode::AccessPath(a) => a.load_key(key),
            _ => unreachable!(),
        }
    }
    pub fn get_type_map(&self) -> TypeMap {
        match self {
            PhysicalNode::Project(a) => a.fields_map.clone(),
            PhysicalNode::Select(a) => a.fields_map.clone(),
            PhysicalNode::MergeJoin(a) => a.fields_map.clone(),
            PhysicalNode::IndexedLoopJoin(a) => a.fields_map.clone(),
            PhysicalNode::AccessPath(_) => unreachable!(),
            PhysicalNode::RemoveDuplicates(a) => a.fields_map.clone(),
            PhysicalNode::Sort(a) => a.fields_map.clone(),
            PhysicalNode::GroupBy(a) => a.fields_map.clone(),
        }
    }
}

impl Iterator for PhysicalNode {
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            PhysicalNode::Project(a) => a.next(),
            PhysicalNode::Select(a) => a.next(),
            PhysicalNode::MergeJoin(a) => a.next(),
            PhysicalNode::IndexedLoopJoin(a) => a.next(),
            PhysicalNode::AccessPath(a) => a.next(),
            PhysicalNode::RemoveDuplicates(a) => a.next(),
            PhysicalNode::Sort(a) => a.next(),
            PhysicalNode::GroupBy(a) => a.next(),
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
    fields_map: TypeMap,
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
    fields_map: TypeMap,
    condition: evalexpr::Node,
    context: HashMapContext,
    child: Box<PhysicalNode>,
    pub bridged: (bool, Option<Vec<u8>>), // (LOADED,KEY)
}

impl Select {
    fn next_normal(&mut self) -> Option<MergedRow> {
        for next in self.child.by_ref() {
            fill_ctx_map(&mut self.context, &next, &self.fields_map);
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
    fn next_bridged(&mut self) -> Option<MergedRow> {
        self.child.next()
    }
}

impl Iterator for Select {
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bridged.1.is_none() {
            self.next_normal()
        } else if self.bridged.0 {
            self.next_bridged()
        } else {
            self.child.load_key(self.bridged.1.as_ref().unwrap());
            self.bridged.0 = true;
            self.next_bridged()
        }
    }
}

#[derive(Debug)]
pub struct MergeJoin {
    fields_map: TypeMap,
    left: Box<PhysicalNode>,
    right: Box<PhysicalNode>,
    left_table: Option<TupleTable>,
    right_table: Option<TupleTable>,
    left_iter: Option<TupleTableIter>,
    right_iter: Option<TupleTableIter>,
    eq_fields: (FieldId, FieldId),
    loaded: bool,
    current_left_row: Option<MergedRow>,
    current_right_row: Option<MergedRow>,
    out1: bool,
    just_returned: bool,
}

impl MergeJoin {
    fn load(&mut self) {
        for next in self.left.by_ref() {
            // let next = next.into_iter().map(|(k,v)| (k.field,v)).collect();
            self.left_table.as_mut().unwrap().add_row_map(next);
        }
        for next in self.right.by_ref() {
            // let next = next.into_iter().map(|(k,v)| (k.field,v)).collect();
            self.right_table.as_mut().unwrap().add_row_map(next);
        }
        self.left_table
            .as_mut()
            .unwrap()
            .sort_single(&self.eq_fields.0, false);
        self.right_table
            .as_mut()
            .unwrap()
            .sort_single(&self.eq_fields.1, false);
        self.left_iter = Some(self.left_table.take().unwrap().into_iter());
        self.right_iter = Some(self.right_table.take().unwrap().into_iter());
        self.current_left_row = self.left_iter.as_mut().unwrap().next();
        self.current_right_row = self.right_iter.as_mut().unwrap().next();
        self.loaded = true;
    }
    pub fn new(
        fields_map: TypeMap,
        left: Box<PhysicalNode>,
        right: Box<PhysicalNode>,
        eq_fields: (FieldId, FieldId),
        left_headers: TypeMap,
        right_headers: TypeMap,
    ) -> Self {
        let left_table = Some(TupleTable::new(
            &eq_fields.0.table,
            left_headers,
            MAX_WORKING_MEM,
        ));
        let right_table = Some(TupleTable::new(
            &eq_fields.1.table,
            right_headers,
            MAX_WORKING_MEM,
        ));
        Self {
            fields_map,
            left,
            right,
            left_table,
            right_table,
            left_iter: None,
            right_iter: None,
            eq_fields,
            loaded: false,
            current_left_row: None,
            current_right_row: None,
            out1: false,
            just_returned: false,
        }
    }
    fn merged_row_to_val(&self, row: &MergedRow, field: &FieldId) -> ConcreteType {
        let bytes = row
            .get(field)
            .unwrap()
            .as_ref()
            .map_or([].as_slice(), |d| d);
        let datatype = *self.fields_map.get(field).unwrap();
        ConcreteType::from_bytes(datatype, bytes)
    }
}

impl Iterator for MergeJoin {
    type Item = MergedRow;

    #[allow(clippy::collapsible_else_if)]
    fn next(&mut self) -> Option<Self::Item> {
        if !self.loaded {
            self.load();
        }

        // setting the nexts
        let mut left =
            self.merged_row_to_val(self.current_left_row.as_ref().unwrap(), &self.eq_fields.0);
        let mut right =
            self.merged_row_to_val(self.current_right_row.as_ref().unwrap(), &self.eq_fields.1);

        // loop until 2 rows match
        loop {
            while !self.out1 && left != right {
                // if right value of field is greater, shift left till it becomes equal or greater than right.
                if left < right {
                    if let Some(next_left) = self.left_iter.as_mut().unwrap().next() {
                        self.current_left_row.replace(next_left);
                        left = self.merged_row_to_val(
                            self.current_left_row.as_ref().unwrap(),
                            &self.eq_fields.0,
                        );
                    }
                    // no more lefts so return no matched records to upper node
                    else {
                        return None;
                    }
                }
                // if left value of field is greater, shift right till it becomes equal or greater than left.
                else {
                    if let Some(next_right) = self.right_iter.as_mut().unwrap().next() {
                        self.current_right_row.replace(next_right);
                        right = self.merged_row_to_val(
                            self.current_right_row.as_ref().unwrap(),
                            &self.eq_fields.1,
                        );
                    }
                    // no more rights so return no matched records to upper node
                    else {
                        return None;
                    }
                }
            }

            // loop 1 broke due to matching records
            // hold the current left record in a temp variable
            // set the flag not to enter the upper loop again when calling next again
            self.out1 = true;
            let marked_left = self.current_left_row.as_ref().unwrap().clone();
            // internal flag to seek left
            let mut no_more_left = false;

            // a loop getting all right records matching the held left record
            loop {
                // internal loop entered at least once (i.e; the upper loop broke in this condition already)
                while left == right {
                    // check if we returned this merge before
                    if self.just_returned == false {
                        let result = merge(
                            self.current_left_row.as_ref().unwrap(),
                            self.current_right_row.as_ref().unwrap(),
                        );
                        // set the just returned flag to force the next iteration to skip merging the records again
                        self.just_returned = true;
                        return Some(result);
                    }
                    // reset the flag after skipping merge for the next time
                    self.just_returned = false;
                    // seek left.next
                    if let Some(next_left) = self.left_iter.as_mut().unwrap().next() {
                        self.current_left_row.replace(next_left);
                        left = self.merged_row_to_val(
                            self.current_left_row.as_ref().unwrap(),
                            &self.eq_fields.0,
                        );
                    } else {
                        no_more_left = true;
                        break;
                    }
                }

                // get the next right record to compare it to the held value of left (i.e; not the just seeked one)
                if let Some(next_right) = self.right_iter.as_mut().unwrap().next() {
                    self.current_right_row.replace(next_right);
                    right = self.merged_row_to_val(
                        self.current_right_row.as_ref().unwrap(),
                        &self.eq_fields.1,
                    );
                }
                // no more right records so return None
                else {
                    return None;
                }

                // reconstruct the value of the held left to compare it to current right after fetching next right
                let marked_left_val =
                    self.merged_row_to_val(&marked_left.clone(), &self.eq_fields.0);
                // the already held left matches the next right
                // step back the call to next left and keep the held left as the iter position
                if marked_left_val == right {
                    self.left_iter.as_mut().unwrap().step_back();
                    self.current_left_row.replace(marked_left.clone());
                    left = self.merged_row_to_val(
                        self.current_left_row.as_ref().unwrap(),
                        &self.eq_fields.0,
                    );
                } else {
                    if no_more_left {
                        return None;
                    } else {
                        self.out1 = false;
                        break;
                    }
                }
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct IndexedJoin {
    fields_map: TypeMap,
    eq_fields: (FieldId, FieldId),
    current_left_row: Option<MergedRow>,
    left: Box<PhysicalNode>,
    right: Box<PhysicalNode>,
}

impl IndexedJoin {
    pub fn new(
        fields_map: TypeMap,
        eq_fields: (FieldId, FieldId),
        left: Box<PhysicalNode>,
        right: Box<PhysicalNode>,
    ) -> Self {
        Self {
            fields_map,
            eq_fields,
            current_left_row: None,
            left,
            right,
        }
    }
}

impl Iterator for IndexedJoin {
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(left) = &self.current_left_row {
            if let Some(right) = self.right.next() {
                return Some(merge(left, &right));
            } else if let Some(left) = &self.left.next() {
                self.current_left_row.replace(left.clone());
                let key = left.get(&self.eq_fields.0).unwrap().as_ref().unwrap();
                self.right.load_key(key);
                self.next()
            } else {
                None
            }
        } else if let Some(left) = self.left.next() {
            self.current_left_row.replace(left.clone());
            let key = left.get(&self.eq_fields.0).unwrap().as_ref().unwrap();
            self.right.load_key(key);
            self.next()
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct DeDup {
    fields_map: TypeMap,
    child: Box<PhysicalNode>,
    current_row: Option<MergedRow>,
}
impl DeDup {}
impl Iterator for DeDup {
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_row.is_none() {
            let next = self.child.next();
            self.current_row = next.clone();
            return next;
        }
        for next in self.child.by_ref() {
            if next != *self.current_row.as_ref().unwrap() {
                self.current_row.replace(next.clone());
                return Some(next);
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct Sort {
    fields_map: TypeMap,
    child: Box<PhysicalNode>,
    table: Option<TupleTable>,
    table_iter: Option<TupleTableIter>,
    fields: Vec<FieldId>,
    loaded: bool,
    desc: Vec<bool>,
}

impl Sort {
    fn new(
        headers: TypeMap,
        child: Box<PhysicalNode>,
        fields: Vec<FieldId>,
        desc: Vec<bool>,
    ) -> Self {
        let table = Some(TupleTable::new(
            &fields[0].to_string(),
            headers.clone(),
            MAX_WORKING_MEM,
        ));
        Self {
            fields_map: headers,
            child,
            table,
            table_iter: None,
            fields,
            loaded: false,
            desc,
        }
    }

    fn load(&mut self) {
        for next in self.child.by_ref() {
            self.table.as_mut().unwrap().add_row_map(next);
        }
        self.table.as_mut().unwrap().sort(&self.fields, &self.desc);
        self.table_iter
            .replace(self.table.take().unwrap().into_iter());
        self.loaded = true;
    }
}

impl Iterator for Sort {
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.loaded {
            self.load();
        }
        self.table_iter.as_mut().unwrap().next()
    }
}

#[derive(Debug)]
pub struct Grouper {
    group_on: Vec<FieldId>,
    agg_ops: HashMap<FieldId, AggregateField>,
    child: Box<PhysicalNode>,
    fields_map: TypeMap,
    results: Vec<MergedRow>,
    loaded: bool,
}

impl Grouper {
    // fn row_grouping_set(&self,row:&MergedRow) -> MergedRow{
    //
    // }
    fn load(&mut self) {
        let child_map = self.child.get_type_map();
        // dbg!(&child_map);
        let mut table = TupleTable::new("grouping", child_map.clone(), MAX_WORKING_MEM);
        for mut row in self.child.by_ref() {
            table.add_row_map(row);
        }
        let desc = vec![false; self.group_on.len()];
        table.sort(&self.group_on, &desc);
        // table.print_all();
        let mut iter = table.into_iter();
        let mut agg_fns = self
            .agg_ops
            .iter()
            .map(|(k, v)| {
                Box::<dyn AggregateFunction>::from((v.clone(), *child_map.get(k).unwrap()))
            })
            .collect::<Vec<_>>();
        let current_row = iter.next().unwrap();
        agg_fns.iter_mut().for_each(|func| func.apply(&current_row));
        let grouping_set: HashSet<FieldId> = HashSet::from_iter(self.group_on.iter().cloned());
        let mut current_group = current_row
            .iter()
            .filter(|(field, _)| grouping_set.contains(field) )
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<HashMap<_, _>>();
        for row in iter {
            if current_group.iter().all(|(k, v)| row.get(k).unwrap() == v) {
                // same group , apply on aggregators
                agg_fns.iter_mut().for_each(|func| func.apply(&row));
            } else {
                let mut group_result = current_group.clone();
                group_result.retain(|f,_| self.fields_map.contains_key(f));
                group_result.extend(agg_fns.iter_mut().flat_map(|func| func.finalize()));
                self.results.push(group_result);
                current_group
                    .iter_mut()
                    .for_each(|(k, v)| *v = row.get(k).unwrap().clone());
                agg_fns.iter_mut().for_each(|func| func.apply(&row));
            }
        }
        self.results.reverse();
        self.loaded = true
    }
    pub fn new(
        group_on: Vec<FieldId>,
        agg_ops: HashMap<FieldId, AggregateField>,
        child: Box<PhysicalNode>,
        fields_map: TypeMap,
    ) -> Self {
        Self {
            group_on,
            agg_ops,
            child,
            fields_map,
            results: vec![],
            loaded: false,
        }
    }
}
impl Iterator for Grouper {
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.loaded {
            self.load();
            self.results.pop()
        } else {
            self.results.pop()
        }
    }
}

impl From<(AggregateField, Type)> for Box<dyn AggregateFunction> {
    fn from(value: (AggregateField, Type)) -> Box<dyn AggregateFunction> {
        match value.0.op {
            AggregateFunc::Min => {
                Box::new(Min::new((value.0.field, value.1))) as Box<dyn AggregateFunction>
            }
            AggregateFunc::Max => {
                Box::new(Max::new((value.0.field, value.1))) as Box<dyn AggregateFunction>
            }
            AggregateFunc::Count => {
                Box::new(Count::new((value.0.field, value.1))) as Box<dyn AggregateFunction>
            }
            AggregateFunc::Avg => {
                Box::new(Avg::new((value.0.field, value.1))) as Box<dyn AggregateFunction>
            }
            AggregateFunc::Sum => {
                Box::new(Sum::new((value.0.field, value.1))) as Box<dyn AggregateFunction>
            }
        }
    }
}

trait AggregateFunction {
    fn apply(&mut self, row: &MergedRow);
    fn finalize(&mut self) -> Vec<(FieldId, Option<Vec<u8>>)>;
    // fn reset(&mut self);
}

struct Count {
    field: (FieldId, Type),
    count: u64,
}
impl Count {
    fn new(field: (FieldId, Type)) -> Self {
        Self { field, count: 0 }
    }
}
impl AggregateFunction for Count {
    fn apply(&mut self, row: &MergedRow) {
        self.count += 1;
    }

    fn finalize(&mut self) -> Vec<(FieldId, Option<Vec<u8>>)> {
        let count = self.count;
        self.count = 0;
        let agg = AggregateField::new(AggregateFunc::Count, self.field.0.clone());
        vec![
            (
                FieldId::from(agg),
                ConcreteType::BigInt(count as i64).to_bytes(),
            )
        ]
    }
}
struct Min {
    field: (FieldId, Type),
    current_min: Option<MergedRow>,
}

impl Min {
    fn new(field: (FieldId, Type)) -> Self {
        Self {
            field,
            current_min: None,
        }
    }
}
impl AggregateFunction for Min {
    fn apply(&mut self, row: &MergedRow) {
        let value = row.get(&self.field.0).unwrap().as_ref().unwrap();
        let value = ConcreteType::from_bytes(self.field.1, value);
        if self.current_min.is_some(){
            if let Some(current_min_val) = self.current_min.as_ref().unwrap().get(&self.field.0).unwrap() {
                let current_min_val = ConcreteType::from_bytes(self.field.1, current_min_val);
                if value > current_min_val {
                    self.current_min.replace(row.clone());
                }
            } else {
                self.current_min = Some(row.clone())
            }
        }
        else {
            self.current_min = Some(row.clone())
        }
    }

    fn finalize(&mut self) -> Vec<(FieldId, Option<Vec<u8>>)> {
        let agg = AggregateField::new(AggregateFunc::Min, self.field.0.clone());
        let field =FieldId::from(agg);
        let mut min_row = self.current_min.take().unwrap();
        let min_val = min_row.remove(&self.field.0).unwrap();
        min_row.insert(field,min_val);
        min_row.into_iter().collect::<Vec<_>>()
    }
}
struct Max {
    field: (FieldId, Type),
    current_max: Option<MergedRow>,
}

impl Max {
    fn new(field: (FieldId, Type)) -> Self {
        Self {
            field,
            current_max: None,
        }
    }
}
impl AggregateFunction for Max {
    fn apply(&mut self, row: &MergedRow) {
        let value = row.get(&self.field.0).unwrap().as_ref().unwrap();
        let value = ConcreteType::from_bytes(self.field.1, value);
        if self.current_max.is_some(){
            if let Some(current_max_val) = self.current_max.as_ref().unwrap().get(&self.field.0).unwrap() {
                let current_max_val = ConcreteType::from_bytes(self.field.1, current_max_val);
                if value > current_max_val {
                    self.current_max.replace(row.clone());
                }
            } else {
                self.current_max = Some(row.clone())
            }
        }
        else {
            self.current_max = Some(row.clone())
        }
    }

    fn finalize(&mut self) -> Vec<(FieldId, Option<Vec<u8>>)> {
        let agg = AggregateField::new(AggregateFunc::Max, self.field.0.clone());
        let field =FieldId::from(agg);
        let mut max_row = self.current_max.take().unwrap();
        let max_val = max_row.remove(&self.field.0).unwrap();
        max_row.insert(field,max_val);
        max_row.into_iter().collect::<Vec<_>>()
    }
}

struct Sum {
    field: (FieldId, Type),
    sum: Option<ConcreteType>,
}

impl Sum {
    fn new(field: (FieldId, Type)) -> Self {
        Self { field, sum: None }
    }
}
impl AggregateFunction for Sum {
    fn apply(&mut self, row: &MergedRow) {
        let value = row.get(&self.field.0).unwrap().as_ref().unwrap();
        let value = ConcreteType::from_bytes(self.field.1, value);
        if let Some(val) = &mut self.sum {
            *val += value;
        } else {
            self.sum = Some(value);
        }
    }

    fn finalize(&mut self) -> Vec<(FieldId, Option<Vec<u8>>)> {
        let agg = AggregateField::new(AggregateFunc::Sum, self.field.0.clone());
        vec![(FieldId::from(agg), self.sum.take().unwrap().to_bytes())]
    }
}

struct Avg {
    field: (FieldId, Type),
    sum: Option<ConcreteType>,
    count: u64,
}

impl Avg {
    fn new(field: (FieldId, Type)) -> Self {
        Self {
            field,
            sum: None,
            count: 0,
        }
    }
}
impl AggregateFunction for Avg {
    fn apply(&mut self, row: &MergedRow) {
        self.count += 1;
        let value = row.get(&self.field.0).unwrap().as_ref().unwrap();
        let value = ConcreteType::from_bytes(self.field.1, value);
        if let Some(val) = &mut self.sum {
            *val += value;
        } else {
            self.sum = Some(value);
        }
    }

    fn finalize(&mut self) -> Vec<(FieldId, Option<Vec<u8>>)> {
        let count = ConcreteType::BigInt(self.count as i64);
        self.count = 0;
        let avg = self.sum.take().unwrap() / count;
        let agg = AggregateField::new(AggregateFunc::Avg, self.field.0.clone());
        vec![(FieldId::from(agg), avg.to_bytes())]
    }
}
