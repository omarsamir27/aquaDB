use crate::schema::schema::Schema;
use crate::sql::Sql;

pub enum QueryPlan{
    CreateTable(Schema)
}

pub fn create_plan(query_tree:&Sql)-> QueryPlan{
    match query_tree {
        Sql::Query(_) => todo!(),
        Sql::CreateTable(ct) => QueryPlan::CreateTable(ct.to_schema())
    }
}

