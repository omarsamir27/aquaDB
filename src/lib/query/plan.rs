// use crate::schema::schema::Schema;
// use crate::sql::query::query::SqlQuery;
// use crate::sql::Sql;
//
//
//
// pub fn create_plan(query_tree:&Sql)-> QueryPlan{
//     match query_tree {
//         Sql::CreateTable(ct) => QueryPlan::CreateTable(ct.to_schema()),
//         Sql::Query(query) => match query {
//             SqlQuery::SELECT(_) => todo!(),
//             SqlQuery::INSERT() => todo!(),
//             SqlQuery::DELETE(_) => todo!(),
//             SqlQuery::UPDATE(_) => todo!()
//         }
//     }
// }
//
