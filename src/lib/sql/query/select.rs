use crate::sql::query::select::ProjectionTarget::AllFields;
use bincode::{Decode, Encode};
use std::mem;
use std::str::FromStr;

#[derive(Debug)]
pub struct SqlSelect {
    pub distinct: bool,
    pub targets: Vec<ProjectionTarget>,
    pub from: FromClause,
    pub where_clause: Option<String>,
    pub group_by: Option<Grouping>,
    pub order_by: Option<Ordering>,
}

impl SqlSelect {
    pub fn new(
        distinct: bool,
        targets: Vec<ProjectionTarget>,
        from: FromClause,
        where_clause: Option<String>,
        group_by: Option<Grouping>,
        order_by: Option<Ordering>,
    ) -> Self {
        Self {
            distinct,
            targets,
            from,
            where_clause,
            group_by,
            order_by,
        }
    }

    pub fn replace_aggregates_with_fields(&mut self) -> Vec<AggregateItem> {
        let mut ret = vec![];
        for target in &mut self.targets {
            let flag = target.is_aggregate();
            if flag {
                let var = target.replace_self_with_attribute().get_aggregate();
                ret.push(var);
            }
        }
        ret
    }
}

#[derive(Encode, Decode, Debug, Clone, Hash, Eq, PartialEq)]
pub enum AggregateFunc {
    Min,
    Max,
    Count,
    Avg,
    Sum,
}
impl ToString for AggregateFunc {
    fn to_string(&self) -> String {
        let str = match self {
            AggregateFunc::Min => "MIN",
            AggregateFunc::Max => "MAX",
            AggregateFunc::Count => "COUNT",
            AggregateFunc::Avg => "AVG",
            AggregateFunc::Sum => "SUM",
        };
        String::from(str)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AggregateItem {
    pub op: AggregateFunc,
    pub field: Attribute,
}
impl AggregateItem {
    pub fn new(op: AggregateFunc, field: Attribute) -> Self {
        Self { op, field }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Attribute {
    FullyQualified(String, String),
    Shorthand(String),
}
impl FromStr for Attribute {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut target = s.split('.');
        if let Some(first) = target.next() {
            return if let Some(second) = target.next() {
                Ok(Self::FullyQualified(first.to_string(), second.to_string()))
            } else {
                Ok(Self::Shorthand(first.to_string()))
            };
        }
        Err(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProjectionTarget {
    AllFields,
    Attribute(Attribute),
    AggregateItem(AggregateItem),
}

impl ProjectionTarget {
    pub fn get_attribute(&self) -> Result<Attribute, ()> {
        if let Self::Attribute(attr) = self {
            Ok(attr.clone())
        } else {
            Err(())
        }
    }
    pub fn is_aggregate(&self) -> bool {
        matches!(self, Self::AggregateItem(_))
    }
    pub fn get_aggregate(&self) -> AggregateItem {
        match self {
            ProjectionTarget::AggregateItem(a) => a.clone(),
            _ => unreachable!(), // Only use this when checked beforehand it's an aggregate variant
        }
    }
    pub fn replace_self_with_attribute(&mut self) -> Self {
        let replace = self.get_aggregate().field;
        std::mem::replace(self, ProjectionTarget::Attribute(replace))
    }
}

// impl FromStr for ProjectionTarget {
//     type Err = ();
//
//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         if s.eq("*") {
//             Ok(Self::AllFields)
//         } else {
//             let mut target = s.split('.');
//             if let Some(first) = target.next() {
//                 return if let Some(second) = target.next() {
//                     Ok(Self::FullyQualified(first.to_string(), second.to_string()))
//                 } else {
//                     Ok(Self::Shorthand(first.to_string()))
//                 };
//             }
//             Err(())
//         }
//     }
// }

#[derive(Debug)]
pub struct Grouping {
    pub criteria: Vec<Attribute>,
    having_condition: Option<String>,
}

impl Grouping {
    pub fn new(criteria: Vec<Attribute>, having_condition: Option<String>) -> Self {
        Self {
            criteria,
            having_condition,
        }
    }
}

#[derive(Debug)]
pub struct Ordering {
    pub criteria: Vec<ProjectionTarget>,
    pub descending: Vec<bool>,
}

impl Ordering {
    pub fn new(criteria: Vec<ProjectionTarget>, descending: Vec<bool>) -> Self {
        Self {
            criteria,
            descending,
        }
    }
}

#[derive(Debug, Clone)]
pub enum FromClause {
    Table(String),
    JoinClause(JoinClause),
}

impl FromClause {
    pub fn get_table(&self) -> Option<String> {
        match self {
            FromClause::Table(t) => Some(t.to_string()),
            FromClause::JoinClause(_) => None,
        }
    }
    pub fn get_join(&self) -> Option<JoinClause> {
        match self {
            FromClause::Table(_) => None,
            FromClause::JoinClause(j) => Some(j.clone()),
        }
    }
}
#[derive(Debug, Clone)]
pub struct Join {
    pub table: String,
    pub join_type: JoinType,
    pub join_condition: Option<String>,
}

impl Join {
    pub fn new(table: String, join_type: JoinType, join_condition: Option<String>) -> Self {
        Self {
            table,
            join_type,
            join_condition,
        }
    }
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub first: String,
    // right: String,
    // join_type: JoinType,
    // join_condition: String,
    pub joins: Vec<Join>,
}

impl JoinClause {
    // pub fn new(left: String, right: String, join_type: JoinType, join_condition: String) -> Self {
    //     Self {
    //         left,
    //         right,
    //         join_type,
    //         join_condition,
    //     }
    pub fn new(first: String, joins: Vec<Join>) -> Self {
        Self { first, joins }
    }
    pub fn get_tables(&self) -> Vec<String> {
        let mut ret = self
            .joins
            .iter()
            .map(|j| j.table.clone())
            .collect::<Vec<_>>();
        ret.push(self.first.clone());
        ret
    }
    // pub fn get_tables(&self) -> (String,String){
    //     (self.left.clone(),self.right.clone())
    // }
    // pub fn join_condition(&self) -> &str {
    //     &self.join_condition
    // }
}

#[derive(Debug, Clone, Copy)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}
