// use std::collections::HashMap;
// use crate::index::Index;
// use crate::query::tree::project::ProjectNode;
//
// type Row = HashMap<String,Option<Vec<u8>>>;
//
//
//
//
// pub enum Node{
//     Project(ProjectNode),
//     IndexIterator(Index),
// }
//
// impl Node{
//     pub fn project_node(node:ProjectNode) -> Self{
//         Node::Project(node)
//     }
// }
//
// impl Iterator for Node{
//     type Item = Row;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         match self{
//             Node::Project(prj) => prj.next(),
//         }
//     }
// }
