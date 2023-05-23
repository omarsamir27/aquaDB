// use std::fs::write;
// use ptree::{print_tree, Style, TreeItem};
// use crate::query::algebra::*;
//
// impl TreeItem for Box<LogicalNode>{
//     type Child = Self;
//
//     fn write_self<W: Write>(&self, f: &mut W, style: &Style) -> std::io::Result<()> {
//         self.as_ref().write_self(f,style)
//     }
//
//     fn children(&self) -> Cow<[Self::Child]> {
//         self.as_ref().children()
//     }
// }
//
// impl TreeItem for LogicalNode {
//     type Child = Box<Self>;
//
//     fn write_self<W: Write>(&self, f: &mut W, style: &Style) -> std::io::Result<()> {
//         match self{
//             LogicalNode::Project(p) => print_tree(p),
//             LogicalNode::Select(s) => print_tree(s),
//             LogicalNode::Cross(c) => print_tree(c),
//             LogicalNode::Join(j) => print_tree(j),
//             LogicalNode::Relation(t) => write!(f,"{t}"),
//             LogicalNode::Empty => write!(f,"")
//         }
//     }
//
//     fn children(&self) -> Cow<[Self::Child]> {
//         match self{
//             LogicalNode::Project(p) => p.children() ,
//             LogicalNode::Select(s) => s.children() ,
//             LogicalNode::Cross(c) => c.children(),
//             LogicalNode::Join(j) => j.children(),
//             LogicalNode::Relation(t) => Cow::from(vec![]),
//             LogicalNode::Empty => Cow::from(vec![])
//         }
//     }
// }
//
// impl TreeItem for super::Project{
//     type Child = Box<LogicalNode>;
//
//     fn write_self<W: Write>(&self, f: &mut W, style: &Style) -> std::io::Result<()> {
//         write!(f,"π\n{:?}",self.fields)
//     }
//
//     fn children(&self) -> Cow<[Self::Child]> {
//         Cow::from(vec![self.child.clone()])
//     }
// }
//
// impl TreeItem for super::Select {
//     type Child = Box<LogicalNode>;
//
//     fn write_self<W: Write>(&self, f: &mut W, style: &Style) -> std::io::Result<()> {
//         write!(f,"σ\n{}",self.condition)
//     }
//
//     fn children(&self) -> Cow<[Self::Child]> {
//         Cow::from(vec![self.child.clone()])
//     }
// }
//
// impl TreeItem for super::Join {
//     type Child = Box<LogicalNode>;
//
//     fn write_self<W: Write>(&self, f: &mut W, style: &Style) -> std::io::Result<()> {
//         write!(f,"⨝\n{}",self.condition.clone().unwrap_or_default())
//     }
//
//     fn children(&self) -> Cow<[Self::Child]> {
//         Cow::from(vec![self.right.clone(),self.left.clone()])
//     }
// }
//
// impl TreeItem for super::Cross {
//     type Child = Box<LogicalNode>;
//
//     fn write_self<W: Write>(&self, f: &mut W, style: &Style) -> std::io::Result<()> {
//         write!(f,"X")
//     }
//
//     fn children(&self) -> Cow<[Self::Child]> {
//         Cow::from(vec![self.right.clone(),self.left.clone()])
//     }
// }
//
