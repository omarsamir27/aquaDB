use crate::database::db::DatabaseInstance;
use crate::meta::catalogmgr::CatalogManager;
use crate::query;
// use crate::query::physical::project::ProjectNode;
use crate::schema::schema::Schema;
use crate::sql::parser::Node;
use crate::sql::query::select::{ProjectionTarget, SqlSelect};
use crate::table::tablemgr::TableManager;
use std::cell::Ref;
use std::collections::{HashMap, HashSet};
use std::process::exit;
use crate::schema::types::Type;
// use crate::query::algebra::LogicalNode;
use crate::sql::parser::Rule::sql_value;

type Row = HashMap<String, Option<Vec<u8>>>;
type TreeNode = Box<dyn Iterator<Item = Row>>;

impl DatabaseInstance {
    pub fn plan_query(&self, query: SqlSelect) -> Result<TreeNode, String> {
        let logical_plan =
            query::algebra::LogicalNode::translate_sql(query, self.catalog().borrow(), self.name())
                .map_err(|_| "Broken Query".to_string())?;
        todo!()
    }

    fn planner_info(&self)-> PlannerInfo{
        let info = self.tables().iter().map(|(name,table)| (name.clone(),table.planning_info())).collect();
        PlannerInfo{table_data:info}
    }
}


struct PlannerInfo{
    table_data : HashMap<String,TableInfo>
}


pub struct TableInfo{
    fields_desc : HashMap<String,Type>,
    btree_idx : HashSet<String>,
    hash_idx : HashSet<String>
}

impl TableInfo {
    pub fn new(fields_desc: HashMap<String, Type>, btree_idx: HashSet<String>, hash_idx: HashSet<String>) -> Self {
        Self { fields_desc, btree_idx, hash_idx }
    }
}