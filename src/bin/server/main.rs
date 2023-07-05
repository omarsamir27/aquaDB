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



fn main() {
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
