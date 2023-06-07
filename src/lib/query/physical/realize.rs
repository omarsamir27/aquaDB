use crate::query::algebra as Logical;
use crate::query::physical as Physical;
use Logical::*;
use Physical::*;
use crate::sql::parser::Rule::BOOL;

impl From<LogicalNode> for PhysicalNode {
    fn from(value: LogicalNode) -> Self {
        match value {
            LogicalNode::Project(_) => {}
            LogicalNode::Select(_) => {}
            LogicalNode::Cross(_) => {}
            LogicalNode::Join(_) => {}
            LogicalNode::Relation(_) => {}
            LogicalNode::Sort(_) => {}
            LogicalNode::DeDup(_) => {}
            LogicalNode::Empty => {}
        }
        todo!()
    }
}

impl From<Logical::Project> for Physical::Project {
    fn from(value: Logical::Project) -> Self {
        let child = PhysicalNode::from(*value.child);
        Physical::Project {
            fields: value.fields.into_iter().collect::<HashSet<FieldId>>(),
            child: Box::new(child),
        }
    }
}

impl From<Logical::Select> for Physical::Select {
    fn from(value: Logical::Select) -> Self {
        let Logical::Select{ condition,context_vars,child } = value;
        let mut ctx_map = HashMapContext::new();
        for f in context_vars{
            ctx_map.set_value(f.to_string(),Value::Empty);
        }
        let child = PhysicalNode::from(*child);
        Physical::Select{
            condition,
            context: ctx_map,
            child : Box::new(child)
        }

    }
}

impl From<Logical::DeDuplicate> for Physical::DeDup{
    fn from(value: DeDuplicate) -> Self {
        let Logical::DeDuplicate{ child } = value;
        let child = PhysicalNode::from(*child);
        Physical::DeDup{current_row:None,child:Box::new(child)}
    }
}

impl From<Logical::Sorting> for Physical::Sort{
    fn from(value: Sorting) -> Self {
        let Logical::Sorting{ sort_on, descending, child } = value;
        let child = PhysicalNode::from(*child);
        Physical::Sort{
            child: Box::new(child),
            table: None,
            table_iter: None,
            field: FieldId {},
            loaded: false,
        }
    }
}

fn transform_logical(root: LogicalNode) {}
