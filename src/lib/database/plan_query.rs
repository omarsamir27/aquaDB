use crate::database::db::DatabaseInstance;
use crate::meta::catalogmgr::CatalogManager;
use crate::{FieldId, query};
// use crate::query::physical::project::ProjectNode;
use crate::schema::schema::Schema;
use crate::sql::parser::Node;
use crate::sql::query::select::{ProjectionTarget, SqlSelect};
use crate::table::tablemgr::TableManager;
use std::cell::Ref;
use std::collections::{HashMap, HashSet};
use std::process::exit;
use crate::query::physical::{PhysicalNode,realize::FromLogicalNode};
use crate::schema::types::Type;
// use crate::query::algebra::LogicalNode;
use crate::sql::parser::Rule::sql_value;

type Row = HashMap<String, Option<Vec<u8>>>;
type TreeNode = Box<dyn Iterator<Item = Row>>;

impl DatabaseInstance {
    pub fn plan_query(&self, query: SqlSelect) -> Result<PhysicalNode, String> {
        let planner_info = self.planner_info();
        let logical_plan =
            query::algebra::LogicalNode::translate_sql(query, &planner_info, self.name())
                .map_err(|_| "Broken Query".to_string())?;
        let mut planner_info = self.planner_info();
        Ok(PhysicalNode::from_logic(logical_plan, &mut planner_info, self.tables()))
    }

    fn planner_info(&self)-> PlannerInfo{
        let info = self.tables().iter().map(|(name,table)| (name.clone(),table.planning_info())).collect();
        PlannerInfo{table_info:info}
    }
}


pub struct PlannerInfo{
    pub table_info : HashMap<String,TableInfo>
}
impl PlannerInfo{
    pub fn get_fields_map(&self,table:&str) -> Option<&HashMap<String,Type>> {
        self.table_info.get(table).map(|ti| &ti.fields_desc)
    }
    pub fn get_fields_map_qualified(&self,table:&str) -> HashMap<FieldId,Type>{
        Self::qualify_table_map(table,self.get_fields_map(table).unwrap())
    }
    pub fn qualify_table_map(table:&str,map:&HashMap<String,Type>) -> HashMap<FieldId,Type>{
        map.into_iter().map(|(k,v)| (FieldId::new(table,k),*v) ).collect()
    }
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
    pub fn has_index_for(&self,field:&str) -> bool{
    self.btree_idx.contains(field) || self.hash_idx.contains(field)
}
}