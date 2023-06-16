#![allow(non_snake_case)]
mod init;

use aqua::common::boolean::{set_node_true, simplify};
use aqua::database::server::DatabaseServer;
use aqua::sql::parser::{parse_query, Rule, SqlParser};
use evalexpr::build_operator_tree;
use pest::Parser;
use pest_ascii_tree::print_ascii_tree;
use std::env;
use std::process::exit;
// use ptree::print_tree;
use aqua::query::algebra::LogicalNode;
use aqua::query::concrete_types::ConcreteType;
use aqua::schema::types::NumericType::Integer;
use aqua::schema::types::Type::Numeric;
use aqua::sql::query::query::SqlQuery;
use aqua::sql::Sql;
use aqua::storage::storagemgr::StorageManager;

// fn btree_write_test() {
//     let mut tree = BTreeMultimap::new();
//     tree.insert_vec(10, &[1, 2, 3, 4, 5, 6, 7]);
//     tree.insert_vec(5, &[8, 9, 10]);
//     let bytes = tree.to_bytes();
//     std::fs::write("btree", bytes).unwrap()
// }
//
// fn btree_read_test() {
//     let bytes = std::fs::read("btree").unwrap();
//     let tree: BTreeMultimap<i32, i32> = BTreeMultimap::from_bytes(bytes.as_slice());
//     tree.print_all()
// }

fn main() {
    // let query = "select * from uni where name == \"omar\" ";
    // let query = "insert into students(id,name) values(1,\"omar\")";
    // let select = <SqlParser as pest_consume::Parser>::parse(Rule::Sql, query).unwrap();
    // // dbg!(&select);
    // let x = select.single().unwrap();
    // dbg!(SqlParser::Sql(x));
    // return;

    // let query = "create table omar( samir int primary key,koko smallint references oo(bad) , create index hash mazen on (id))";
    // let query = parse_query(query);
    // dbg!(query);

    // let query = "(x > 3 and (y < 8 or z <1)) or v >2";
    // let q = SqlParser::parse(Rule::conditional_expression,query);
    // print_ascii_tree(q);
    // let var = ConcreteType::Integer(816);
    // let var2 = ConcreteType::Integer(404);
    // dbg!(var.cmp(&var2));

    // let x = " 3.0 == 3.0";
    // let mut tree = build_operator_tree(x).unwrap();
    // dbg!(&tree.eval_boolean());
    // return;
    // let mut v = aqua::common::boolean::get_all_binary_clauses(&tree);
    //
    // let z = v.pop().unwrap();
    // set_node_true(&mut tree,&z);
    // simplify(&mut tree);
    // dbg!(&tree);
    // let query = parse_query(query).unwrap();
    // // dbg!(query);
    // let res = match query {
    //     Sql::Query(s) => match s {
    //         SqlQuery::SELECT(z) => LogicalNode::translate_sql(z),
    //       _ => unreachable!()
    //     }
    //     _ => unreachable!()
    // };
    // print_tree(&res.unwrap());
    let opts = env::args().collect::<Vec<_>>();
    if let Some(init) = opts.get(1) {
        if init == "init" {
            init::init_aqua();
        }
    } else {
        init::init_homedir();
    }
    let server = DatabaseServer::new("hi", vec!["127.0.0.1:2710".to_string()]);
    server.run()
}
