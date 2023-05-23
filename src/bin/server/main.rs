#![allow(non_snake_case)]
mod init;

use aqua::database::server::DatabaseServer;
use aqua::sql::parser::parse_query;
use std::env;
use std::process::exit;
use ptree::print_tree;
use aqua::query::algebra::LogicalNode;
use aqua::sql::query::query::SqlQuery;
use aqua::sql::Sql;

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
    // let query = "create table omar( samir int primary key,koko smallint references oo(bad) , create index hash mazen on (id))";
    // let query = parse_query(query);
    // dbg!(query);

    // let query = "select * from omar join samir on id = id join mazen on bye=hi join osama on why=no";
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
