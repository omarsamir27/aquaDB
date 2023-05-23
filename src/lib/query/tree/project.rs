use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
// use crate::query::tree::node::Node;

type Row = HashMap<String, Option<Vec<u8>>>;
type TreeNode = Box<dyn Iterator<Item = Row>>;

trait QueryNode: Iterator<Item = Row> + Debug {}

// pub trait PrintDebug{
//     fn print_debug(&self);
// }

// impl PrintDebug for TreeNode{
//     fn print_debug(&self) {
//         self.
//     }
// }

// impl Debug for TreeNode{
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         f.write_fmt(format_args!("{:?}",self))
//     }
// }

pub struct ProjectNode {
    fields: HashSet<String>,
    child: TreeNode,
}
impl ProjectNode {
    pub fn new(fields: HashSet<String>, child: TreeNode) -> Self {
        Self { fields, child }
    }
}

impl Iterator for ProjectNode {
    type Item = Row;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(mut row) = self.child.next() {
            row.retain(|r, _| self.fields.contains(r));
            Some(row)
        } else {
            None
        }
    }
}

// impl PrintDebug for ProjectNode {
//     fn print_debug(&self) {
//         println!("{:?}",self.fields);
//     }
// }
