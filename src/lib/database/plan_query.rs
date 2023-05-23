use crate::database::db::DatabaseInstance;
use crate::meta::catalogmgr::CatalogManager;
use crate::query::tree::project::ProjectNode;
use crate::schema::schema::Schema;
use crate::sql::parser::Node;
use crate::sql::query::select::{ProjectionTarget, SqlSelect};
use crate::table::tablemgr::TableManager;
use std::cell::Ref;
use std::collections::{HashMap, HashSet};
use std::process::exit;
// use crate::query::algebra::LogicalNode;
use crate::sql::parser::Rule::sql_value;

type Row = HashMap<String, Option<Vec<u8>>>;
type TreeNode = Box<dyn Iterator<Item = Row>>;

impl DatabaseInstance {
    pub fn plan_query(&self, query: SqlSelect) -> Result<TreeNode, String> {

        // let q = LogicalNode::translate_sql(query,self.catalog().borrow(),self.name());
        // dbg!(q);
        // exit(99);


        let tbl = query.from.get_table().unwrap();
        let schema = self
            .catalog()
            .borrow()
            .get_schema(self.name(), &tbl)
            .unwrap();
        let project_on = query.targets;
        let mut targets = HashSet::new();
        if project_on[0] == ProjectionTarget::AllFields {
            targets.extend(schema.fields_info().keys().map(|s| s.to_string()));
        } else {
            for field in project_on {
                match field {
                    ProjectionTarget::Shorthand(f) => {
                        targets.insert(f);
                    }
                    _ => unreachable!(),
                }
            }
        }
        let target_iter = self.tables().get(schema.name()).unwrap().heapscan_iter();
        Ok(Box::new(ProjectNode::new(targets, Box::new(target_iter))))
    }
}
