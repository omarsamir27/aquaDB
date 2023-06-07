use std::str::FromStr;
use crate::sql::query::select::ProjectionTarget::AllFields;

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
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProjectionTarget {
    AllFields,
    FullyQualified(String, String),
    Shorthand(String),
}

impl FromStr for ProjectionTarget {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq("*"){
            Ok(Self::AllFields)
        }
        else {
            let mut target = s.split('.');
            if let Some(first) = target.next(){
                return if let Some(second) = target.next() {
                    Ok(Self::FullyQualified(first.to_string(), second.to_string()))
                } else {
                    Ok(Self::Shorthand(first.to_string()))
                }
            }
            Err(())
        }
    }
}

#[derive(Debug)]
pub struct Grouping {
    criteria: Vec<ProjectionTarget>,
    having_condition: Option<String>,
}

impl Grouping {
    pub fn new(criteria: Vec<ProjectionTarget>, having_condition: Option<String>) -> Self {
        Self {
            criteria,
            having_condition,
        }
    }
}

#[derive(Debug)]
pub struct Ordering {
    pub criteria: Vec<ProjectionTarget>,
    pub descending: bool,
}

impl Ordering {
    pub fn new(criteria: Vec<ProjectionTarget>, descending: bool) -> Self {
        Self {
            criteria,
            descending,
        }
    }
}

#[derive(Debug,Clone)]
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
    pub fn get_join(&self) -> Option<JoinClause>{
        match self {
            FromClause::Table(_) => None,
            FromClause::JoinClause(j) => Some(j.clone())
        }
    }
}
#[derive(Debug,Clone)]
pub struct Join{
    pub table : String,
    pub join_type : JoinType,
    pub join_condition : Option<String>
}

impl Join {
    pub fn new(table:String,join_type:JoinType,join_condition:Option<String>) -> Self{
        Self{table,join_type,join_condition}
    }
}


#[derive(Debug,Clone)]
pub struct JoinClause {
    pub first: String,
    // right: String,
    // join_type: JoinType,
    // join_condition: String,
    pub joins : Vec<Join>
}

impl JoinClause {
    // pub fn new(left: String, right: String, join_type: JoinType, join_condition: String) -> Self {
    //     Self {
    //         left,
    //         right,
    //         join_type,
    //         join_condition,
    //     }
    pub fn new(first:String,joins:Vec<Join>) -> Self{
        Self{first,joins}
    }
    pub fn get_tables(&self) -> Vec<String>{
        let mut ret = self.joins.iter().map(|j|j.table.clone()).collect::<Vec<_>>();
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

#[derive(Debug,Clone,Copy)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}
