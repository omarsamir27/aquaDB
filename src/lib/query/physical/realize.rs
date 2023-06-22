use crate::common::boolean;
use crate::common::boolean::*;
use crate::database::plan_query::PlannerInfo;
use crate::query::algebra as Logical;
use crate::query::physical as Physical;
use crate::query::physical::PhysicalNode::AccessPath;
use crate::table::tablemgr::TableManager;
use std::str::FromStr;
use Logical::*;
use Physical::*;

pub trait FromLogicalNode<T> {
    fn from_logic(
        value: T,
        planner_info: &mut PlannerInfo,
        db_tables: &HashMap<String, TableManager>,
    ) -> Self;
}

impl FromLogicalNode<LogicalNode> for PhysicalNode {
    fn from_logic(
        value: LogicalNode,
        planner_info: &mut PlannerInfo,
        db_tables: &HashMap<String, TableManager>,
    ) -> Self {
        match value {
            LogicalNode::Project(a) => {
                Self::Project(Physical::Project::from_logic(a, planner_info, db_tables))
            }
            LogicalNode::Select(a) => {
                Self::Select(Physical::Select::from_logic(a, planner_info, db_tables))
            }
            LogicalNode::Cross(a) => unreachable!(),
            LogicalNode::Join(a) => PhysicalNode::from_logic(a, planner_info, db_tables),
            LogicalNode::Relation(a) => PhysicalNode::from_logic(a, planner_info, db_tables),
            LogicalNode::Sort(a) => {
                Self::Sort(Physical::Sort::from_logic(a, planner_info, db_tables))
            }
            LogicalNode::DeDup(a) => {
                Self::RemoveDuplicates(Physical::DeDup::from_logic(a, planner_info, db_tables))
            }
            LogicalNode::Empty => unreachable!(),
        }
    }
}

impl FromLogicalNode<Logical::Project> for Physical::Project {
    fn from_logic(
        value: Logical::Project,
        planner_info: &mut PlannerInfo,
        db_tables: &HashMap<String, TableManager>,
    ) -> Self {
        let child = PhysicalNode::from_logic(*value.child, planner_info, db_tables);

        Physical::Project {
            fields_map: value.fields_map,
            child: Box::new(child),
        }
    }
}

impl FromLogicalNode<Logical::Select> for Physical::Select {
    fn from_logic(
        value: Logical::Select,
        planner_info: &mut PlannerInfo,
        db_tables: &HashMap<String, TableManager>,
    ) -> Self {
        use evalexpr::Operator::*;
        let Logical::Select {
            condition,
            context_vars,
            child,
            fields_map,
        } = value;
        let child_is_base_rel = child.is_base_relation();
        let mut ctx_map = HashMapContext::new();
        let shortcut = if context_vars.len() == 1
            && child_is_base_rel
            && boolean::get_all_binary_clauses(&condition).len() == 1
        {
            let children = condition.children();
            if children.len() == 1 {
                let key_type = fields_map.get(&context_vars[0]).unwrap();
                let FieldId { table, field } = context_vars[0].clone();
                let tbl_mgr = db_tables.get(&table).unwrap();
                let (op, val) = get_single_binary_clause(&children[0]);
                let val = boolean::value_as_bytes(&val, *key_type);
                match op {
                    Eq => {
                        if let Some(hash) = tbl_mgr.hashscan_iter(&field) {
                            Some((
                                PhysicalNode::AccessPath(Box::new(AccessMethod::HashIter(
                                    table, hash,
                                ))),
                                val,
                            ))
                        } else {
                            tbl_mgr.btree_iter(&field, Eq).map(|mut btree| {
                                (
                                    PhysicalNode::AccessPath(Box::new(AccessMethod::BtreeIter(
                                        table, btree,
                                    ))),
                                    val,
                                )
                            })
                        }
                    }
                    Lt | Gt | Leq | Geq => tbl_mgr.btree_iter(&field, op).map(|mut btree| {
                        (
                            PhysicalNode::AccessPath(Box::new(AccessMethod::BtreeIter(
                                table, btree,
                            ))),
                            val,
                        )
                    }),
                    _ => None,
                }
            } else {
                None
            }
        } else {
            None
        };
        if let Some((child, key)) = shortcut {
            Physical::Select {
                fields_map,
                condition,
                context: ctx_map,
                child: Box::new(child),
                bridged: (false, Some(key)),
            }
        } else {
            let child = PhysicalNode::from_logic(*child, planner_info, db_tables);
            for f in context_vars {
                ctx_map.set_value(f.to_string(), Value::Empty);
            }
            Physical::Select {
                fields_map,
                condition,
                context: ctx_map,
                child: Box::new(child),
                bridged: (false, None),
            }
        }
    }
}

impl FromLogicalNode<Logical::DeDuplicate> for Physical::DeDup {
    fn from_logic(
        value: DeDuplicate,
        planner_info: &mut PlannerInfo,
        db_tables: &HashMap<String, TableManager>,
    ) -> Self {
        let Logical::DeDuplicate { child, fields_map } = value;
        let child = PhysicalNode::from_logic(*child, planner_info, db_tables);
        Physical::DeDup {
            fields_map,
            current_row: None,
            child: Box::new(child),
        }
    }
}

impl FromLogicalNode<Logical::Sorting> for Physical::Sort {
    fn from_logic(
        value: Sorting,
        planner_info: &mut PlannerInfo,
        db_tables: &HashMap<String, TableManager>,
    ) -> Self {
        let Logical::Sorting {
            mut sort_on,
            descending,
            child,
            fields_map,
        } = value;
        let child = PhysicalNode::from_logic(*child, planner_info, db_tables);
        Physical::Sort::new(fields_map, Box::new(child), sort_on.remove(0), descending)
    }
}

impl FromLogicalNode<Logical::BaseRelation> for PhysicalNode {
    fn from_logic(
        value: BaseRelation,
        planner_info: &mut PlannerInfo,
        db_tables: &HashMap<String, TableManager>,
    ) -> Self {
        let tbl_mgr = db_tables.get(&value.name).unwrap();
        let iter = tbl_mgr.heapscan_iter();
        PhysicalNode::AccessPath(Box::new(AccessMethod::HeapIter(value.name, iter)))
    }
}

impl FromLogicalNode<Logical::Join> for PhysicalNode {
    fn from_logic(
        value: Join,
        planner_info: &mut PlannerInfo,
        db_tables: &HashMap<String, TableManager>,
    ) -> Self {
        let Logical::Join {
            condition,
            join_type,
            left,
            right,
            fields_map,
        } = value;
        let mut identifiers = condition
            .iter_read_variable_identifiers()
            .map(|f| FieldId::from_str(f).unwrap())
            .collect::<Vec<_>>();
        let left_field = identifiers.pop().unwrap();
        let right_field = identifiers.pop().unwrap();
        let left_field_map = left.get_fields_map();
        let left_child = Box::new(PhysicalNode::from_logic(*left, planner_info, db_tables));
        let tbl_mgr = db_tables.get(&right_field.table).unwrap();
        if planner_info
            .table_info
            .get(&right_field.table)
            .unwrap()
            .has_index_for(&right_field.field)
        {
            let access = if let Some(iter) = tbl_mgr.hashscan_iter(&right_field.field) {
                AccessMethod::HashIter(right_field.table.clone(), iter)
            } else {
                let btree = tbl_mgr
                    .btree_iter(&right_field.field, evalexpr::Operator::Eq)
                    .unwrap();
                AccessMethod::BtreeIter(right_field.table.clone(), btree)
            };
            let access = Box::new(AccessPath(Box::new(access)));
            PhysicalNode::IndexedLoopJoin(IndexedJoin::new(
                fields_map,
                (left_field, right_field),
                left_child,
                access,
            ))
        } else {
            let heap_iter = tbl_mgr.heapscan_iter();
            let access = Box::new(AccessPath(Box::new(AccessMethod::HeapIter(
                right_field.table.clone(),
                heap_iter,
            ))));
            // let left_field_map = fields_map.iter().filter(|(f,_)| f.table == left_field.table).map(|(f,t)| (f.clone(),*t)).collect();
            let right_field_map = fields_map
                .iter()
                .filter(|(f, _)| f.table == right_field.table)
                .map(|(f, t)| (f.clone(), *t))
                .collect();
            PhysicalNode::MergeJoin(MergeJoin::new(
                fields_map,
                left_child,
                access,
                (left_field, right_field),
                left_field_map,
                right_field_map,
            ))
        }
    }
}

// fn transform_logical(root: LogicalNode) {}
