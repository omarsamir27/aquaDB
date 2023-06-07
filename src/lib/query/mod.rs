use std::collections::HashMap;
use crate::FieldId;

pub mod concrete_types;
pub mod executor;
mod select_node;
pub mod seq_scan;
pub mod physical;
mod tuple_table;
pub mod algebra;

pub(self) type MergedRow = HashMap<FieldId, Option<Vec<u8>>>;

