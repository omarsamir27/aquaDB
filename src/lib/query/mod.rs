use crate::FieldId;
use std::collections::HashMap;

pub mod algebra;
pub mod concrete_types;
pub mod executor;
pub mod physical;
mod select_node;
pub mod seq_scan;
mod tuple_table;

pub(self) type MergedRow = HashMap<FieldId, Option<Vec<u8>>>;
