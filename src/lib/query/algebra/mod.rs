mod pretty;
use crate::meta::catalogmgr::CatalogManager;
use crate::schema::schema::Schema;
use crate::{FieldId, sql};
use crate::sql::query::select::{FromClause, JoinClause, JoinType, ProjectionTarget, SqlSelect};
use std::borrow::Cow;
use std::cell::Ref;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::mem;
use std::str::FromStr;
use evalexpr::{build_operator_tree, HashMapContext};
use crate::common::boolean;

// impl ToString for FieldId {
//     fn to_string(&self) -> String {
//         format!("{}.{}",self.table,self.field)
//     }
// }

#[derive(Debug, Clone)]
pub enum LogicalNode {
    Project(Project),
    Select(Select),
    Cross(Cross),
    Join(Join),
    Relation(String),
    Sort(Sorting),
    DeDup(DeDuplicate),
    Empty,
}

impl LogicalNode {
    fn chain(&mut self, queue: &mut Vec<Self>) {
        match self {
            LogicalNode::Project(p) => p.chain(queue),
            LogicalNode::Select(s) => s.chain(queue),
            LogicalNode::Cross(c) => c.chain(queue),
            LogicalNode::Join(j) => j.chain(queue),
            LogicalNode::Sort(s) => s.chain(queue),
            LogicalNode::DeDup(d) => d.chain(queue),
            _ => (),
        }
    }
    fn is_leaf(&self) -> bool {
        matches!(self, Self::Empty | Self::Relation(_))
    }
}

impl Default for LogicalNode {
    fn default() -> Self {
        Self::Empty
    }
}

#[derive(Default, Debug, Clone)]
pub struct Project {
    pub fields: Vec<FieldId>,
    pub child: Box<LogicalNode>,
}

impl Project {
    fn with_fields(fields: Vec<FieldId>) -> Self {
        Self {
            fields,
            child: Box::default(),
        }
    }
    fn chain(&mut self, queue: &mut Vec<LogicalNode>) {
        let node = queue.pop().unwrap();
        mem::replace(self.child.as_mut(), node);
    }
}

#[derive(Debug, Clone)]
pub struct Select {
    pub condition: evalexpr::Node,
    pub context_vars : Vec<FieldId>,
    pub child: Box<LogicalNode>,
}

impl Select {
    fn with_condition(condition: evalexpr::Node,context_vars:Vec<FieldId>) -> Self {

        Self {
            condition,
            context_vars,
            child: Box::default(),
        }
    }
    fn chain(&mut self, queue: &mut Vec<LogicalNode>) {
        let node = queue.pop().unwrap();
        mem::replace(self.child.as_mut(), node);
    }
}

#[derive(Debug, Clone)]
pub struct Cross {
    left: Box<LogicalNode>,
    right: Box<LogicalNode>,
}

impl Cross {
    fn chain(&mut self, queue: &mut Vec<LogicalNode>) {
        let node_r = queue.pop().unwrap();
        let node_l = queue.pop().unwrap();
        mem::replace(self.left.as_mut(), node_l);
        mem::replace(self.right.as_mut(), node_r);
    }
}

#[derive(Debug, Clone)]
pub struct Join {
    condition: Option<evalexpr::Node>,
    join_type: JoinType,
    left: Box<LogicalNode>,
    right: Box<LogicalNode>,
}
impl Join {
    fn with_condition(join_type: JoinType, condition: Option<evalexpr::Node>) -> Self {
        Self {
            condition,
            join_type,
            left: Box::default(),
            right: Box::default(),
        }
    }
    fn chain(&mut self, queue: &mut Vec<LogicalNode>) {
        let node_l = queue.pop().unwrap();
        let node_r = queue.pop().unwrap();
        mem::replace(self.left.as_mut(), node_l);
        mem::replace(self.right.as_mut(), node_r);
    }
    // fn correct_condition(&mut self,joined:&Option<HashMap<String, HashSet<String>>>){
    //     LogicalNode::qualify_attributes(self.condition.as_mut().unwrap(),&None, joined);
    // }
}

#[derive(Debug, Clone)]
pub struct Sorting {
    sort_on: Vec<FieldId>,
    descending: bool,
    child: Box<LogicalNode>,
}

impl Sorting {
    fn with_sort_cols(sort_on: Vec<FieldId>, descending: bool) -> Self {
        Self {
            sort_on,
            descending,
            child: Default::default(),
        }
    }
    fn chain(&mut self, queue: &mut Vec<LogicalNode>) {
        let node = queue.pop().unwrap();
        mem::replace(self.child.as_mut(), node);
    }
}

#[derive(Default, Debug, Clone)]
pub struct DeDuplicate {
    pub child: Box<LogicalNode>,
}

impl DeDuplicate {
    fn chain(&mut self, queue: &mut Vec<LogicalNode>) {
        let node = queue.pop().unwrap();
        mem::replace(self.child.as_mut(), node);
    }
}

impl LogicalNode {
    pub fn translate_sql(
        sql: SqlSelect,
        catalog: Ref<CatalogManager>,
        db: &str,
    ) -> Result<Self, ()> {
        let mut queue = Vec::new();
        let (single, joined) = Self::get_schemas(&catalog, sql.from.clone(), db);
        if sql.distinct {
            queue.push(LogicalNode::DeDup(DeDuplicate::default()));
            if let Some(order) = sql.order_by {
                let order_on = Self::target_list(order.criteria, &single, &joined)?;
                queue.push(LogicalNode::Sort(Sorting::with_sort_cols(
                    order_on,
                    order.descending,
                )));
            } else if let Some(schema) = single.as_ref() {
                let fields = schema.fields_info();
                let field_name = fields.keys().next().unwrap();
                let field = FieldId {
                    table: schema.name().to_string(),
                    field: field_name.to_string(),
                };
                queue.push(LogicalNode::Sort(Sorting::with_sort_cols(
                    vec![field],
                    false,
                )));
            } else if let Some(schema) = joined.as_ref() {
                let (table, field) = schema
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.iter().next().unwrap().to_string()))
                    .take(1)
                    .next()
                    .unwrap();
                let field = FieldId { table, field };
                queue.push(LogicalNode::Sort(Sorting::with_sort_cols(
                    vec![field],
                    false,
                )));
            }
        }
        let project = Project::with_fields(Self::target_list(sql.targets, &single, &joined)?);
        queue.push(LogicalNode::Project(project));
        if let Some(pred) = sql.where_clause {
            let mut pred_tree = build_operator_tree(pred.as_str()).map_err(|_| ())?;
            let ctx_vars = Self::qualify_attributes(&mut pred_tree,&single,&joined)?;
            let select = Select::with_condition(pred_tree,ctx_vars);
            queue.push(LogicalNode::Select(select));
        }
        match sql.from {
            FromClause::Table(t) => queue.push(LogicalNode::Relation(t)),
            FromClause::JoinClause(j) => {
                let mut j = Self::preprocess_joins(j,&joined);
                queue.append(&mut j);
            }
        }
        // dbg!(&queue);
        Ok(Self::link_nodes(&mut queue))
    }
    fn preprocess_joins(mut joins: JoinClause, joined: &Option<HashMap<String, HashSet<String>>>) -> Vec<LogicalNode> {
        let mut result = vec![];
        let first =
            sql::query::select::Join::new(joins.first.clone(), joins.joins[0].join_type, None);
        joins.joins.insert(0, first);
        for i in 0..joins.joins.len() - 1 {
            joins.joins[i].join_condition = joins.joins[i + 1].join_condition.take();
            joins.joins[i].join_type = joins.joins[i + 1].join_type;
        }
        let last_join = joins.joins.pop().unwrap();
        'pushing_order: for j in joins.joins {
            let mut condition = j.join_condition.map(|jc| build_operator_tree(jc.as_str()).unwrap());
            if let Some(condition) = condition.as_mut(){
                Self::qualify_attributes(condition,&None,joined);
            }
            let node = Join::with_condition(j.join_type, condition);
            result.push(LogicalNode::Join(node));
            result.push(LogicalNode::Relation(j.table));
        }
        let last = LogicalNode::Relation(last_join.table);
        result.push(last);
        result
    }

    fn link_nodes(queue: &mut Vec<LogicalNode>) -> LogicalNode {
        let mut idx = queue.len() - 1;
        while queue.len() != 1 {
            // dbg!(&queue);
            if queue[idx].is_leaf() {
                idx -= 1;
                continue;
            } else {
                let mut current = queue.remove(idx);
                current.chain(queue);
                queue.push(current);
                if idx == 0 {
                    break;
                }
                idx -= 1;
            }
        }
        queue.pop().unwrap()
    }

    fn target_list(
        targets: Vec<ProjectionTarget>,
        single_schema: &Option<Schema>,
        joined_schemas: &Option<HashMap<String, HashSet<String>>>,
    ) -> Result<Vec<FieldId>, ()> {
        if let Some(schema) = single_schema {
            let name = schema.name().to_string();
            let fields_info = schema.fields_info();
            if targets[0] == ProjectionTarget::AllFields {
                return Ok(fields_info
                    .keys()
                    .map(|k| FieldId {
                        table: name.clone(),
                        field: k.to_string(),
                    })
                    .collect());
            }
            let mut fields = Vec::with_capacity(targets.len());
            for col in targets {
                match col {
                    ProjectionTarget::FullyQualified(table, field) => {
                        if table == name && fields_info.contains_key(field.as_str()) {
                            fields.push(FieldId { table, field });
                        } else {
                            return Err(());
                        }
                    }
                    ProjectionTarget::Shorthand(field) => {
                        if fields_info.contains_key(field.as_str()) {
                            fields.push(FieldId {
                                table: name.clone(),
                                field,
                            });
                        } else {
                            return Err(());
                        }
                    }
                    _ => return Err(()), // because an "*" list was eliminated up
                }
            }
            return Ok(fields);
        }
        if let Some(schemas_map) = joined_schemas {
            if targets[0] == ProjectionTarget::AllFields {
                let fields = schemas_map
                    .iter()
                    .map(|(table, schema)| {
                        schema
                            .iter()
                            .map(|k| FieldId {
                                table: table.clone(),
                                field: k.to_string(),
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .flatten()
                    .collect();
                return Ok(fields);
            }

            let mut fields = Vec::with_capacity(targets.len() * 4);
            for col in targets {
                if let ProjectionTarget::FullyQualified(table, field) = col {
                    if schemas_map.get(&table).ok_or(())?.contains(field.as_str()) {
                        fields.push(FieldId { table, field })
                    }
                } else if let ProjectionTarget::Shorthand(field) = col {
                    let mut matching_schemas = schemas_map
                        .iter()
                        .filter(|(k, v)| v.contains(field.as_str()))
                        .map(|(k, v)| k)
                        .collect::<Vec<_>>();
                    if matching_schemas.len() == 1 {
                        let table = matching_schemas.pop().unwrap();
                        fields.push(FieldId {
                            table: table.clone(),
                            field,
                        })
                    } else {
                        return Err(());
                    }
                }
            }
            Ok(fields)
        } else {
            Err(())
        }

    }
    fn get_schemas(
        catalog: &Ref<CatalogManager>,
        from: FromClause,
        db: &str,
    ) -> (Option<Schema>, Option<HashMap<String, HashSet<String>>>) {
        match from {
            FromClause::Table(t) => (catalog.get_schema(db, t.as_str()), None),
            FromClause::JoinClause(j) => (
                None,
                Self::get_joined_tables_schemas(catalog, j.get_tables(), db).ok(),
            ),
        }
    }
    fn get_joined_tables_schemas(
        catalog: &Ref<CatalogManager>,
        joined_tables: Vec<String>,
        db: &str,
    ) -> Result<HashMap<String, HashSet<String>>, ()> {
        let mut schemas_map = HashMap::new();
        for t in joined_tables {
            if let Some(schema) = catalog.get_schema(db, &t) {
                let fields = schema
                    .fields_info()
                    .keys()
                    .map(|s| s.to_string())
                    .collect::<HashSet<_>>();
                schemas_map.insert(t, fields);
            } else {
                return Err(());
            }
        }
        Ok(schemas_map)
    }

    fn qualify_attributes(tree: &mut evalexpr::Node, single:&Option<Schema>, joined:&Option<HashMap<String, HashSet<String>>>) -> Result<Vec<FieldId>,()>{
        let mut variables = tree.iter_variable_identifiers().collect::<HashSet<_>>();
        let vars_len = variables.len();
        let targets  = variables
            .into_iter()
            .map_while(|v| v.parse::<ProjectionTarget>().ok().map(|t| (v.to_string(),t))).collect::<Vec<_>>();
        if targets.len() != vars_len{
            return Err(())
        }
        let targets = Self::field_target_list(targets,single,joined)?;
        boolean::replace_vars_map(tree, &targets);
        Ok(targets.into_values().collect())
    }

    fn field_target_list(
        targets: Vec<(String, ProjectionTarget)>,
        single_schema: &Option<Schema>,
        joined_schemas: &Option<HashMap<String, HashSet<String>>>,
    ) -> Result<HashMap<String, FieldId>, ()> {
        if let Some(schema) = single_schema {
            let name = schema.name().to_string();
            let fields_info = schema.fields_info();
            let mut fields = HashMap::new();
            for (original,col) in targets {
                match col {
                    ProjectionTarget::FullyQualified(table, field) => {
                        if *table == name && fields_info.contains_key(field.as_str()) {
                            fields.insert(original,FieldId{table,field});
                        } else {
                            return Err(());
                        }
                    }
                    ProjectionTarget::Shorthand(field) => {
                        if fields_info.contains_key(field.as_str()) {
                            fields.insert(original,FieldId {
                                table: name.clone(),
                                field,
                            });
                        } else {
                            return Err(());
                        }
                    }
                    _ => return Err(()), // because an "*" list was eliminated up
                }
            }
            return Ok(fields);
        }
        if let Some(schemas_map) = joined_schemas {
            let mut fields = HashMap::new();
            for (original,col) in targets {
                if let ProjectionTarget::FullyQualified(table, field) = col {
                    if schemas_map.get(&table).ok_or(())?.contains(field.as_str()) {
                        fields.insert(original,FieldId { table, field });
                    }
                } else if let ProjectionTarget::Shorthand(field) = col {
                    let mut matching_schemas = schemas_map
                        .iter()
                        .filter(|(k, v)| v.contains(field.as_str()))
                        .map(|(k, v)| k)
                        .collect::<Vec<_>>();
                    if matching_schemas.len() == 1 {
                        let table = matching_schemas.pop().unwrap();
                        fields.insert(original,FieldId {
                            table: table.clone(),
                            field,
                        });
                    } else {
                        return Err(());
                    }
                }
            }
            Ok(fields)
        } else {
            Err(())
        }

    }
}
