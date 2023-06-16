use crate::common::numerical::ByteMagic;
use crate::query::concrete_types::ConcreteType;
use crate::schema::schema::{Layout, Schema};
use crate::schema::types::NumericType::{BigInt, SmallInt};
use crate::schema::types::Type;
use crate::schema::types::Type::Numeric;
use crate::storage::blockid::BlockId;
use crate::storage::buffermgr::FrameRef;
use crate::storage::heap::HeapPage;
use crate::storage::storagemgr::StorageManager;
use crate::storage::tuple::Tuple;
use pest::unicode::FORMAT;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::env;
use std::env::consts::OS;
use std::fmt::Display;
use std::io::Read;
use std::rc::Rc;

use super::Rid;

const BLOCK_NUM_SIZE: usize = 8;
const SLOT_NUM_SIZE: usize = 2;
const ORDER: usize = 4; // B+Tree order
const INDEX_RECORD_SIZE: usize = 14;

// Index record in the leaf node
#[derive(Debug, Clone)]
struct IndexRecord {
    key: Vec<u8>,
    value: Rid,
    layout: Rc<Layout>,
}

impl IndexRecord {
    // Convert IndexRecord to bytes
    fn to_tuple(&self) -> Tuple {
        let index_tuple_fields = vec![
            ("key".to_string(), Some(self.key.clone())),
            (
                "block_num".to_string(),
                Some(self.value.block_num.to_ne_bytes().to_vec()),
            ),
            (
                "slot_num".to_string(),
                Some(self.value.slot_num.to_ne_bytes().to_vec()),
            ),
        ];
        Tuple::new(index_tuple_fields, self.layout.clone())
    }

    // Convert bytes to IndexRecord
    fn from_bytes(mut bytes: HashMap<String, Option<Vec<u8>>>, layout: Rc<Layout>) -> IndexRecord {
        let key = bytes.remove("key").unwrap().unwrap();
        let block_num = bytes.remove("block_num").unwrap().unwrap();
        let slot_num = bytes.remove("slot_num").unwrap().unwrap();

        IndexRecord {
            key,
            value: Rid::new(block_num.to_u64(), slot_num.to_u16()),
            layout,
        }
    }
}

// B+Tree structure
#[derive(Clone)]
pub struct BPTree {
    root: NodePage,
    root_frame: FrameRef,
    key_type: Type,
    storage_manager: Rc<RefCell<StorageManager>>,
    internal_layout: Rc<Layout>,
    leaf_layout: Rc<Layout>,
    index_file: String,
}

// B+Tree implementation
impl BPTree {
    pub fn init(
        key_type: Type,
        storage_manager: Rc<RefCell<StorageManager>>,
        index_file: String,
    ) -> Self {
        let root_block = storage_manager
            .borrow_mut()
            .extend_file(index_file.as_str());
        let root_frame = storage_manager
            .borrow_mut()
            .pin(root_block.clone())
            .unwrap();

        let mut internal_schema = Schema::new();
        internal_schema.add_field_default_constraints("key", key_type, None);
        internal_schema.add_field_default_constraints("block_num", Type::Numeric(BigInt), None);
        let internal_layout = Rc::new(internal_schema.to_layout());

        let mut leaf_schema = Schema::new();
        leaf_schema.add_field_default_constraints("key", key_type, None);
        leaf_schema.add_field_default_constraints("block_num", Type::Numeric(BigInt), None);
        leaf_schema.add_field_default_constraints("slot_num", Type::Numeric(SmallInt), None);
        let leaf_layout = Rc::new(leaf_schema.to_layout());

        let root_heap = HeapPage::new_from_empty_special(
            root_frame.clone(),
            &root_block,
            leaf_layout.clone(),
            16,
        );

        BPTree {
            root: NodePage::init(
                root_frame.clone(),
                key_type,
                storage_manager.clone(),
                internal_layout.clone(),
                leaf_layout.clone(),
                index_file.clone(),
            ),
            root_frame,
            key_type,
            storage_manager,
            internal_layout,
            leaf_layout,
            index_file,
        }
    }

    // Create a new B+Tree
    pub fn new(
        root_block: BlockId,
        key_type: Type,
        storage_manager: Rc<RefCell<StorageManager>>,
        index_file: String,
    ) -> Self {
        let root_frame = storage_manager
            .borrow_mut()
            .pin(root_block.clone())
            .unwrap();

        let mut internal_schema = Schema::new();
        internal_schema.add_field_default_constraints("key", key_type, None);
        internal_schema.add_field_default_constraints("block_num", Type::Numeric(BigInt), None);
        let internal_layout = Rc::new(internal_schema.to_layout());

        let mut leaf_schema = Schema::new();
        leaf_schema.add_field_default_constraints("key", key_type, None);
        leaf_schema.add_field_default_constraints("block_num", Type::Numeric(BigInt), None);
        leaf_schema.add_field_default_constraints("slot_num", Type::Numeric(SmallInt), None);
        let leaf_layout = Rc::new(leaf_schema.to_layout());

        BPTree {
            root: NodePage::new(
                root_frame.clone(),
                key_type,
                storage_manager.clone(),
                internal_layout.clone(),
                leaf_layout.clone(),
                index_file.clone(),
            ),
            root_frame,
            key_type,
            storage_manager,
            internal_layout,
            leaf_layout,
            index_file,
        }
    }

    // Insert a key-value pair into the B+Tree
    pub fn insert(&mut self, key: Vec<u8>, value: Rid) {
        let mut free_space = 0_i32;
        match self.root.clone() {
            NodePage::Internal(root) => {
                free_space = (root.heap_page.free_space()) as i32 - 4;
            }
            NodePage::Leaf(root) => {
                free_space = (root.heap_page.free_space()) as i32 - 4;
            }
        }
        let (split_key, split_block_num) = self.root.insert(key.clone(), value.clone());
        if let Some(split_key) = split_key {
            let dummy_key: Vec<u8> = Vec::new();
            match self.root.clone() {
                NodePage::Internal(mut root_page) => {
                    let right = (root_page.heap_page.tuple_pointers.len() - 1) as u16;
                    let child_index = root_page.binary_search_child_node(1, right, key.clone());

                    let new_child_tuple = ChildNode {
                        key: split_key.clone(),
                        block_num: split_block_num.unwrap(),
                        layout: self.internal_layout.clone(),
                    }
                    .to_tuple();

                    if free_space > (new_child_tuple.tuple_size() as i32) {
                        if child_index == 0 {
                            let first_child_block_num = root_page
                                .heap_page
                                .get_field("block_num", 0)
                                .unwrap()
                                .as_slice()
                                .extract_u64(0);
                            root_page.heap_page.mark_delete(0);
                            root_page.heap_page.vacuum();

                            root_page.insert_child(split_key, first_child_block_num);
                            let mut last_inserted =
                                root_page.heap_page.tuple_pointers.pop().unwrap();
                            root_page
                                .heap_page
                                .tuple_pointers
                                .insert(child_index, last_inserted);

                            root_page.insert_child(dummy_key.clone(), split_block_num.unwrap());
                            last_inserted = root_page.heap_page.tuple_pointers.pop().unwrap();
                            root_page
                                .heap_page
                                .tuple_pointers
                                .insert(child_index, last_inserted);

                            root_page.heap_page.rewrite_tuple_pointers_to_frame();
                        } else {
                            root_page.heap_page.insert_tuple(new_child_tuple);
                            let last_inserted = root_page.heap_page.tuple_pointers.pop().unwrap();
                            root_page
                                .heap_page
                                .tuple_pointers
                                .insert(child_index + 1, last_inserted);
                            root_page.heap_page.rewrite_tuple_pointers_to_frame();
                        }
                    } else {
                        let root_payload = self.root_frame.borrow().page.payload.clone();
                        self.root_frame
                            .borrow_mut()
                            .page
                            .write_bytes(vec![0; root_payload.len()].as_slice(), 0);
                        let idx_file = self
                            .root_frame
                            .borrow()
                            .blockid
                            .as_ref()
                            .unwrap()
                            .filename
                            .clone();
                        let root_block = BlockId::new(idx_file.as_str(), 0);
                        let new_root_heap = HeapPage::new_from_empty(
                            self.root_frame.clone(),
                            &root_block,
                            self.internal_layout.clone(),
                        );

                        let mut new_root = InternalNodePage::new(
                            new_root_heap,
                            self.key_type,
                            self.storage_manager.clone(),
                            self.internal_layout.clone(),
                            self.leaf_layout.clone(),
                            self.index_file.clone(),
                        );

                        let left_blockid = self
                            .storage_manager
                            .borrow_mut()
                            .extend_file(&self.index_file);
                        let mut left_frame = self
                            .storage_manager
                            .borrow_mut()
                            .pin(left_blockid.clone())
                            .unwrap();
                        left_frame.borrow_mut().page.payload = root_payload;
                        let left_heap = HeapPage::new(
                            left_frame.clone(),
                            &left_blockid.clone(),
                            self.internal_layout.clone(),
                        );
                        let split_blockid = BlockId {
                            block_num: split_block_num.unwrap(),
                            filename: self.index_file.to_string(),
                        };
                        let split_frame = self
                            .storage_manager
                            .borrow_mut()
                            .pin(split_blockid.clone())
                            .unwrap();
                        let right_heap = HeapPage::new(
                            split_frame.clone(),
                            &split_blockid.clone(),
                            self.internal_layout.clone(),
                        );
                        self.storage_manager.borrow_mut().unpin(left_frame);
                        self.storage_manager.borrow_mut().unpin(split_frame);

                        new_root.insert_child(dummy_key, left_blockid.block_num);
                        new_root.insert_child(split_key.clone(), split_block_num.unwrap());
                        self.root = NodePage::Internal(new_root);
                    }
                }
                NodePage::Leaf(_) => {
                    let root_payload = self.root_frame.borrow().page.payload.clone();
                    self.root_frame
                        .borrow_mut()
                        .page
                        .write_bytes(vec![0; root_payload.len()].as_slice(), 0);
                    let idx_file = self
                        .root_frame
                        .borrow()
                        .blockid
                        .as_ref()
                        .unwrap()
                        .filename
                        .clone();
                    let root_block = BlockId::new(idx_file.as_str(), 0);
                    let new_root_heap = HeapPage::new_from_empty(
                        self.root_frame.clone(),
                        &root_block,
                        self.internal_layout.clone(),
                    );

                    let mut new_root = InternalNodePage::new(
                        new_root_heap,
                        self.key_type,
                        self.storage_manager.clone(),
                        self.internal_layout.clone(),
                        self.leaf_layout.clone(),
                        self.index_file.clone(),
                    );

                    let right_blockid = BlockId {
                        block_num: split_block_num.unwrap(),
                        filename: self.index_file.clone(),
                    };
                    let right_frame = self.storage_manager.borrow_mut().pin(right_blockid.clone());

                    let left_blockid = self
                        .storage_manager
                        .borrow_mut()
                        .extend_file(&self.index_file);
                    let mut left_frame = self
                        .storage_manager
                        .borrow_mut()
                        .pin(left_blockid.clone())
                        .unwrap();
                    left_frame.borrow_mut().page.payload = root_payload;

                    let mut right_heap = HeapPage::new(
                        right_frame.unwrap(),
                        &right_blockid,
                        self.leaf_layout.clone(),
                    );
                    let mut right_leaf = LeafNodePage::new(
                        right_heap.clone(),
                        self.key_type,
                        self.storage_manager.clone(),
                        self.leaf_layout.clone(),
                        self.index_file.clone(),
                    );
                    right_leaf.meta_data.prev_node_blockid = left_blockid.block_num;
                    right_heap.write_special_area(right_leaf.meta_data.to_bytes());

                    new_root.insert_child(dummy_key, left_blockid.block_num);
                    new_root.insert_child(split_key.clone(), split_block_num.unwrap());
                    self.root = NodePage::Internal(new_root);
                }
            }
        }
    }

    // Search for a key in the B+Tree and return the associated values
    pub fn search(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        self.root.search(key)
    }

    pub fn get_greater_than(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        self.root.get_greater_than(key)
    }

    pub fn get_less_than(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        self.root.get_less_than(key)
    }

    pub fn get_greater_than_or_equal(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        self.root.get_greater_than_or_equal(key)
    }

    pub fn get_less_than_or_equal(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        self.root.get_less_than_or_equal(key)
    }

    pub fn print_root(&self) {
        self.root.print_node();
    }
}

// B+Tree node variants
#[derive(Clone)]
pub enum NodePage {
    Internal(InternalNodePage),
    Leaf(LeafNodePage),
}

// B+Tree node implementation
impl NodePage {
    fn new(
        frame: FrameRef,
        key_type: Type,
        storage_manager: Rc<RefCell<StorageManager>>,
        internal_layout: Rc<Layout>,
        leaf_layout: Rc<Layout>,
        index_file: String,
    ) -> Self {
        let block_id = frame.borrow().blockid.as_ref().unwrap().clone();
        let mut heap_page = HeapPage::new(frame.clone(), &block_id, Rc::new(Layout::default()));
        if heap_page.get_special_area().is_empty() {
            heap_page.layout = internal_layout.clone();
            NodePage::Internal(InternalNodePage::new(
                heap_page,
                key_type,
                storage_manager.clone(),
                internal_layout,
                leaf_layout,
                index_file,
            ))
        } else {
            heap_page.layout = leaf_layout.clone();
            NodePage::Leaf(LeafNodePage::new(
                heap_page,
                key_type,
                storage_manager.clone(),
                leaf_layout,
                index_file,
            ))
        }
    }

    pub fn init(
        frame: FrameRef,
        key_type: Type,
        storage_manager: Rc<RefCell<StorageManager>>,
        internal_layout: Rc<Layout>,
        leaf_layout: Rc<Layout>,
        index_file: String,
    ) -> Self {
        let block_id = frame.borrow().blockid.as_ref().unwrap().clone();
        let mut heap_page =
            HeapPage::new_from_empty_special(frame.clone(), &block_id, leaf_layout.clone(), 16);
        NodePage::Leaf(LeafNodePage::new(
            heap_page,
            key_type,
            storage_manager.clone(),
            leaf_layout,
            index_file,
        ))
    }

    // Insert a key-value pair into the node
    fn insert(&mut self, key: Vec<u8>, value: Rid) -> (Option<Vec<u8>>, Option<u64>) {
        match self {
            NodePage::Internal(node) => node.insert(key, value),
            NodePage::Leaf(node) => node.insert(key, value),
        }
    }

    // Search for a key in the node and return the associated values
    fn search(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        match self {
            NodePage::Internal(node) => node.search(key),
            NodePage::Leaf(node) => node.search(key),
        }
    }

    fn get_greater_than(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        match self {
            NodePage::Internal(node) => node.get_greater_than(key),
            NodePage::Leaf(node) => node.get_greater_than(key),
        }
    }

    fn get_less_than(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        match self {
            NodePage::Internal(node) => node.get_less_than(key),
            NodePage::Leaf(node) => node.get_less_than(key),
        }
    }

    fn get_greater_than_or_equal(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        match self {
            NodePage::Internal(node) => node.get_greater_than_or_equal(key),
            NodePage::Leaf(node) => node.get_greater_than(key),
        }
    }

    fn get_less_than_or_equal(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        match self {
            NodePage::Internal(node) => node.get_less_than(key),
            NodePage::Leaf(node) => node.get_less_than(key),
        }
    }

    fn is_leaf(&self) -> bool {
        match self {
            NodePage::Internal(_) => false,
            NodePage::Leaf(_) => true,
        }
    }

    pub fn print_node(&self) {
        match self {
            NodePage::Internal(node) => {
                node.print_internal_node();
            }
            NodePage::Leaf(_) => {
                unreachable!()
            }
        }
    }
}

struct ChildNode {
    key: Vec<u8>,
    block_num: u64,
    layout: Rc<Layout>,
}

impl ChildNode {
    fn to_tuple(&self) -> Tuple {
        let index_tuple_fields = vec![
            (
                "block_num".to_string(),
                Some(self.block_num.to_ne_bytes().to_vec()),
            ),
            ("key".to_string(), Some(self.key.clone())),
        ];
        Tuple::new(index_tuple_fields, self.layout.clone())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(self.block_num.to_ne_bytes().as_slice());
        bytes.extend_from_slice(self.key.as_slice());
        bytes
    }

    pub fn from_bytes(
        mut bytes: HashMap<String, Option<Vec<u8>>>,
        layout: Rc<Layout>,
    ) -> ChildNode {
        let key = bytes.remove("key").unwrap().unwrap();
        let block_num = bytes
            .remove("block_num")
            .unwrap()
            .unwrap()
            .as_slice()
            .extract_u64(0);

        ChildNode {
            key,
            block_num,
            layout,
        }
    }
}

// Internal node of the B+Tree
#[derive(Clone)]
pub struct InternalNodePage {
    heap_page: HeapPage,
    key_type: Type,
    storage_manager: Rc<RefCell<StorageManager>>,
    internal_layout: Rc<Layout>,
    leaf_layout: Rc<Layout>,
    index_file: String,
}

/*impl Drop for InternalNodePage {
    fn drop(&mut self) {
        self.storage_manager.borrow_mut().unpin(self.heap_page.frame.clone());
    }
}*/

// Internal node implementation
impl InternalNodePage {
    pub fn new(
        heap_page: HeapPage,
        key_type: Type,
        storage_manager: Rc<RefCell<StorageManager>>,
        internal_layout: Rc<Layout>,
        leaf_layout: Rc<Layout>,
        index_file: String,
    ) -> InternalNodePage {
        Self {
            heap_page,
            key_type,
            storage_manager,
            internal_layout,
            leaf_layout,
            index_file,
        }
    }

    // Insert a key-value pair into the internal node
    fn insert(&mut self, key: Vec<u8>, value: Rid) -> (Option<Vec<u8>>, Option<u64>) {
        let right = (self.heap_page.tuple_pointers.len() - 1) as u16;
        let child_index = self.binary_search_child_node(1, right, key.clone());

        let target_block_num = self
            .heap_page
            .get_field("block_num", child_index as u16)
            .unwrap()
            .as_slice()
            .extract_u64(0);
        let target_blockid = BlockId::new(&self.index_file, target_block_num);

        let frame = self
            .storage_manager
            .borrow_mut()
            .pin(target_blockid.clone())
            .unwrap();
        let target_heap_page = HeapPage::new(
            frame.clone(),
            &target_blockid,
            self.heap_page.layout.clone(),
        );

        let mut node_page = NodePage::new(
            frame.clone(),
            self.key_type,
            self.storage_manager.clone(),
            self.internal_layout.clone(),
            self.leaf_layout.clone(),
            self.index_file.clone(),
        );

        match node_page {
            NodePage::Internal(ref mut target_node) => {
                let free_space = self.heap_page.free_space() as i32 - 4;
                let (split_key, split_block_num) = target_node.insert(key.clone(), value);
                if target_node.heap_page.blk.block_num == 0 {
                    dbg!("Freeing ROOT !!");
                }
                self.storage_manager
                    .borrow_mut()
                    .unpin(target_node.heap_page.frame.clone());
                if let Some(split_key) = split_key {
                    let dummy_key: Vec<u8> = Vec::new();

                    let right = (self.heap_page.tuple_pointers.len() - 1) as u16;
                    let child_index = self.binary_search_child_node(1, right, key.clone());

                    let new_child_tuple = ChildNode {
                        key: split_key.clone(),
                        block_num: split_block_num.unwrap(),
                        layout: self.internal_layout.clone(),
                    }
                    .to_tuple();

                    if free_space > new_child_tuple.tuple_size() as i32 {
                        if child_index == 0 {
                            let first_child_block_num = self
                                .heap_page
                                .get_field("block_num", 0)
                                .unwrap()
                                .as_slice()
                                .extract_u64(0);
                            self.heap_page.mark_delete(0);
                            self.heap_page.vacuum();

                            self.insert_child(split_key, first_child_block_num);
                            let mut last_inserted = self.heap_page.tuple_pointers.pop().unwrap();
                            self.heap_page
                                .tuple_pointers
                                .insert(child_index, last_inserted);

                            self.insert_child(dummy_key.clone(), split_block_num.unwrap());
                            last_inserted = self.heap_page.tuple_pointers.pop().unwrap();
                            self.heap_page
                                .tuple_pointers
                                .insert(child_index, last_inserted);

                            self.heap_page.rewrite_tuple_pointers_to_frame();
                        } else {
                            self.heap_page.insert_tuple(new_child_tuple);
                            let last_inserted = self.heap_page.tuple_pointers.pop().unwrap();
                            self.heap_page
                                .tuple_pointers
                                .insert(child_index + 1, last_inserted);
                            self.heap_page.rewrite_tuple_pointers_to_frame();
                        }
                    } else {
                        let pointers_num = self.heap_page.tuple_pointers.len();
                        let mid = self.heap_page.tuple_pointers.len() as u16 / 2;
                        let split_key = self.heap_page.get_field("key", mid);
                        let mut right_child_nodes = Vec::new();
                        for tuple_pointer_index in mid..pointers_num as u16 {
                            let tuple_bytes = self.heap_page.get_multiple_fields(
                                vec!["key".to_string(), "block_num".to_string()],
                                tuple_pointer_index,
                            );
                            let child_node =
                                ChildNode::from_bytes(tuple_bytes, self.heap_page.layout.clone());
                            right_child_nodes.push(child_node);
                            // vacuum the split records
                            self.heap_page.mark_delete(tuple_pointer_index as usize);
                        }
                        let right_tuple_pointers =
                            self.heap_page.tuple_pointers.split_off(mid as usize);
                        self.heap_page.vacuum();

                        let new_block = self
                            .storage_manager
                            .borrow_mut()
                            .extend_file(&self.index_file);
                        let right_frame = self
                            .storage_manager
                            .borrow_mut()
                            .pin(new_block.clone())
                            .unwrap();
                        let mut right_heap_page = HeapPage::new_from_empty(
                            right_frame,
                            &new_block,
                            self.heap_page.layout.clone(),
                        );
                        for child_node in right_child_nodes {
                            right_heap_page.insert_tuple(child_node.to_tuple());
                        }
                        return (split_key, Some(new_block.block_num));
                    }
                }
            }
            NodePage::Leaf(ref mut target_node) => {
                let (split_key, split_block_num) = target_node.insert(key, value);
                self.storage_manager
                    .borrow_mut()
                    .unpin(target_node.heap_page.frame.clone());
                if let Some(split_key) = split_key {
                    let new_child_tuple = ChildNode {
                        key: split_key.clone(),
                        block_num: split_block_num.unwrap(),
                        layout: target_heap_page.layout.clone(),
                    }
                    .to_tuple();

                    let free_space = self.heap_page.free_space() as i32 - 4;
                    if free_space > new_child_tuple.tuple_size() as i32 {
                        self.heap_page.insert_tuple(new_child_tuple);
                        let last_inserted = self.heap_page.tuple_pointers.pop().unwrap();
                        self.heap_page
                            .tuple_pointers
                            .insert(child_index + 1, last_inserted);
                        self.heap_page.rewrite_tuple_pointers_to_frame();
                    } else {
                        let pointers_num = self.heap_page.tuple_pointers.len();
                        let mid = self.heap_page.tuple_pointers.len() as u16 / 2;
                        let new_split_key = self.heap_page.get_field("key", mid);
                        let mut right_child_nodes = Vec::new();
                        for tuple_pointer_index in mid..pointers_num as u16 {
                            let tuple_bytes = self.heap_page.get_multiple_fields(
                                vec!["key".to_string(), "block_num".to_string()],
                                tuple_pointer_index,
                            );
                            let child_node =
                                ChildNode::from_bytes(tuple_bytes, self.heap_page.layout.clone());
                            right_child_nodes.push(child_node);
                            // vacuum the split records
                            self.heap_page.mark_delete(tuple_pointer_index as usize);
                        }
                        let right_tuple_pointers =
                            self.heap_page.tuple_pointers.split_off(mid as usize);
                        self.heap_page.vacuum();

                        let new_block = self
                            .storage_manager
                            .borrow_mut()
                            .extend_file(&self.index_file);
                        let right_frame = self
                            .storage_manager
                            .borrow_mut()
                            .pin(new_block.clone())
                            .unwrap();
                        let mut right_heap_page = HeapPage::new_from_empty(
                            right_frame.clone(),
                            &new_block,
                            self.heap_page.layout.clone(),
                        );
                        for child_node in right_child_nodes {
                            right_heap_page.insert_tuple(child_node.to_tuple());
                        }
                        if child_index >= mid as usize {
                            right_heap_page.insert_tuple(new_child_tuple);
                            let last_inserted = right_heap_page.tuple_pointers.pop().unwrap();
                            let index_to_put = child_index - self.heap_page.tuple_pointers.len();
                            right_heap_page
                                .tuple_pointers
                                .insert(index_to_put, last_inserted);
                            right_heap_page.rewrite_tuple_pointers_to_frame();
                        } else {
                            self.heap_page.insert_tuple(new_child_tuple);
                            let last_inserted = self.heap_page.tuple_pointers.pop().unwrap();
                            self.heap_page
                                .tuple_pointers
                                .insert(child_index, last_inserted);
                            self.heap_page.rewrite_tuple_pointers_to_frame();
                        }
                        self.storage_manager.borrow_mut().unpin(right_frame);
                        return (new_split_key, Some(new_block.block_num));
                    }
                }
            }
        };
        return (None, None);
    }

    // Search for a key in the internal node and return the associated values
    fn search(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        let right = (self.heap_page.tuple_pointers.len() - 1) as u16;
        let child_index = self.binary_search_child_node(1, right, key.clone());

        let target_block_num = self
            .heap_page
            .get_field("block_num", child_index as u16)
            .unwrap()
            .as_slice()
            .extract_u64(0);
        let target_blockid = BlockId::new(&self.index_file, target_block_num);

        let target_frame = self
            .storage_manager
            .borrow_mut()
            .pin(target_blockid.clone())
            .unwrap();

        let target_heap_page = HeapPage::new(
            target_frame.clone(),
            &target_blockid,
            self.heap_page.layout.clone(),
        );

        let mut node_page = NodePage::new(
            target_frame,
            self.key_type,
            self.storage_manager.clone(),
            self.internal_layout.clone(),
            self.leaf_layout.clone(),
            self.index_file.clone(),
        );

        match &node_page {
            NodePage::Internal(target_node) => {
                let results = target_node.search(key);
                self.storage_manager
                    .borrow_mut()
                    .unpin(target_node.heap_page.frame.clone());
                results
            }
            NodePage::Leaf(target_node) => {
                let results = target_node.search(key);
                self.storage_manager
                    .borrow_mut()
                    .unpin(target_node.heap_page.frame.clone());
                results
            }
        }
    }

    fn get_less_than_or_equal(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        let right = (self.heap_page.tuple_pointers.len() - 1) as u16;
        let child_index = self.binary_search_child_node(1, right, key.clone());

        let target_block_num = self
            .heap_page
            .get_field("block_num", child_index as u16)
            .unwrap()
            .as_slice()
            .extract_u64(0);
        let target_blockid = BlockId::new(&self.index_file, target_block_num);

        let target_frame = self
            .storage_manager
            .borrow_mut()
            .pin(target_blockid.clone())
            .unwrap();

        let target_heap_page = HeapPage::new(
            target_frame.clone(),
            &target_blockid,
            self.heap_page.layout.clone(),
        );

        let mut node_page = NodePage::new(
            target_frame,
            self.key_type,
            self.storage_manager.clone(),
            self.internal_layout.clone(),
            self.leaf_layout.clone(),
            self.index_file.clone(),
        );

        match &node_page {
            NodePage::Internal(target_node) => {
                let results = target_node.get_less_than_or_equal(key);
                self.storage_manager
                    .borrow_mut()
                    .unpin(target_node.heap_page.frame.clone());
                results
            }
            NodePage::Leaf(target_node) => {
                let results = target_node.get_less_than_or_equal(key);
                self.storage_manager
                    .borrow_mut()
                    .unpin(target_node.heap_page.frame.clone());
                results
            }
        }
    }

    fn get_greater_than_or_equal(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        let right = (self.heap_page.tuple_pointers.len() - 1) as u16;
        let child_index = self.binary_search_child_node(1, right, key.clone());

        let target_block_num = self
            .heap_page
            .get_field("block_num", child_index as u16)
            .unwrap()
            .as_slice()
            .extract_u64(0);
        let target_blockid = BlockId::new(&self.index_file, target_block_num);

        let target_frame = self
            .storage_manager
            .borrow_mut()
            .pin(target_blockid.clone())
            .unwrap();

        let target_heap_page = HeapPage::new(
            target_frame.clone(),
            &target_blockid,
            self.heap_page.layout.clone(),
        );

        let mut node_page = NodePage::new(
            target_frame,
            self.key_type,
            self.storage_manager.clone(),
            self.internal_layout.clone(),
            self.leaf_layout.clone(),
            self.index_file.clone(),
        );

        match &node_page {
            NodePage::Internal(target_node) => {
                let results = target_node.get_greater_than_or_equal(key);
                self.storage_manager
                    .borrow_mut()
                    .unpin(target_node.heap_page.frame.clone());
                results
            }
            NodePage::Leaf(target_node) => {
                let results = target_node.get_greater_than_or_equal(key);
                self.storage_manager
                    .borrow_mut()
                    .unpin(target_node.heap_page.frame.clone());
                results
            }
        }
    }

    fn get_greater_than(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        let right = (self.heap_page.tuple_pointers.len() - 1) as u16;
        let child_index = self.binary_search_child_node(1, right, key.clone());

        let target_block_num = self
            .heap_page
            .get_field("block_num", child_index as u16)
            .unwrap()
            .as_slice()
            .extract_u64(0);
        let target_blockid = BlockId::new(&self.index_file, target_block_num);

        let target_frame = self
            .storage_manager
            .borrow_mut()
            .pin(target_blockid.clone())
            .unwrap();

        let target_heap_page = HeapPage::new(
            target_frame.clone(),
            &target_blockid,
            self.heap_page.layout.clone(),
        );

        let mut node_page = NodePage::new(
            target_frame,
            self.key_type,
            self.storage_manager.clone(),
            self.internal_layout.clone(),
            self.leaf_layout.clone(),
            self.index_file.clone(),
        );

        match &node_page {
            NodePage::Internal(target_node) => {
                let results = target_node.get_greater_than(key);
                self.storage_manager
                    .borrow_mut()
                    .unpin(target_node.heap_page.frame.clone());
                results
            }
            NodePage::Leaf(target_node) => {
                let results = target_node.get_greater_than(key);
                self.storage_manager
                    .borrow_mut()
                    .unpin(target_node.heap_page.frame.clone());
                results
            }
        }
    }

    fn get_less_than(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        let right = (self.heap_page.tuple_pointers.len() - 1) as u16;
        let child_index = self.binary_search_child_node(1, right, key.clone());

        let target_block_num = self
            .heap_page
            .get_field("block_num", child_index as u16)
            .unwrap()
            .as_slice()
            .extract_u64(0);
        let target_blockid = BlockId::new(&self.index_file, target_block_num);

        let target_frame = self
            .storage_manager
            .borrow_mut()
            .pin(target_blockid.clone())
            .unwrap();

        let target_heap_page = HeapPage::new(
            target_frame.clone(),
            &target_blockid,
            self.heap_page.layout.clone(),
        );

        let mut node_page = NodePage::new(
            target_frame,
            self.key_type,
            self.storage_manager.clone(),
            self.internal_layout.clone(),
            self.leaf_layout.clone(),
            self.index_file.clone(),
        );

        match &node_page {
            NodePage::Internal(target_node) => {
                let results = target_node.get_less_than(key);
                self.storage_manager
                    .borrow_mut()
                    .unpin(target_node.heap_page.frame.clone());
                results
            }
            NodePage::Leaf(target_node) => {
                let results = target_node.get_less_than(key);
                self.storage_manager
                    .borrow_mut()
                    .unpin(target_node.heap_page.frame.clone());
                results
            }
        }
    }

    pub fn insert_child(&mut self, key: Vec<u8>, block_num: u64) {
        let child_node = ChildNode {
            key,
            block_num,
            layout: self.internal_layout.clone(),
        }
        .to_tuple();
        self.heap_page.insert_tuple(child_node);
    }

    pub fn binary_search_child_node(&self, mut left: u16, mut right: u16, key: Vec<u8>) -> usize {
        if left == right {
            let retrieved_key = self.heap_page.get_field("key", left).unwrap();
            let key_to_original = ConcreteType::from_bytes(self.key_type, &key);
            let retrieved_key_to_original = ConcreteType::from_bytes(self.key_type, &retrieved_key);
            let ordering = key_to_original.cmp(&retrieved_key_to_original);
            match ordering {
                Ordering::Less => (left - 1) as usize,
                Ordering::Equal => left as usize,
                Ordering::Greater => left as usize,
            }
        } else {
            let mid_index = (right + left) / 2;
            let retrieved_key = self.heap_page.get_field("key", mid_index).unwrap();
            let key_to_original = ConcreteType::from_bytes(self.key_type, &key);
            let retrieved_key_to_original = ConcreteType::from_bytes(self.key_type, &retrieved_key);
            let ordering = key_to_original.cmp(&retrieved_key_to_original);
            // let new_pointer_indexes = tuple_pointer_indexes.split_off(mid_index as usize);
            match ordering {
                Ordering::Less => self.binary_search_child_node(left, mid_index, key),
                Ordering::Equal => self.binary_search_child_node(mid_index, mid_index, key),
                Ordering::Greater => self.binary_search_child_node(mid_index + 1, right, key),
            }
        }
    }

    pub fn print_internal_node(&self) {
        for tuple_pointer_index in
            (0..(self.heap_page.tuple_pointers.len() as u16)).collect::<Vec<u16>>()
        {
            let tuple = self
                .heap_page
                .get_tuple_fields(tuple_pointer_index as usize);
            let key = ConcreteType::from_bytes(
                self.key_type,
                tuple.get("key").unwrap().as_ref().unwrap(),
            );
            let block_num = ConcreteType::from_bytes(
                Numeric(BigInt),
                tuple.get("block_num").unwrap().as_ref().unwrap(),
            );
            println!("{:?} {:?}", key, block_num);
        }
    }
}

#[derive(Clone)]
pub struct LeafMetaData {
    next_node_blockid: u64,
    prev_node_blockid: u64,
}

impl From<Vec<u8>> for LeafMetaData {
    fn from(value: Vec<u8>) -> Self {
        Self {
            next_node_blockid: value.as_slice().extract_u64(0),
            prev_node_blockid: value.as_slice().extract_u64(8),
        }
    }
}

impl LeafMetaData {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(self.next_node_blockid.to_ne_bytes().as_slice());
        bytes.extend_from_slice(self.prev_node_blockid.to_ne_bytes().as_slice());
        bytes
    }
}

#[derive(Clone)]
pub struct LeafNodePage {
    heap_page: HeapPage,
    meta_data: LeafMetaData,
    key_type: Type,
    storage_manager: Rc<RefCell<StorageManager>>,
    leaf_layout: Rc<Layout>,
    index_file: String,
}

/*impl Drop for LeafNodePage {
    fn drop(&mut self) {
        self.storage_manager.borrow_mut().unpin(self.heap_page.frame.clone());
    }
}*/

impl LeafNodePage {
    pub fn new(
        heap_page: HeapPage,
        key_type: Type,
        storage_manager: Rc<RefCell<StorageManager>>,
        leaf_layout: Rc<Layout>,
        index_file: String,
    ) -> LeafNodePage {
        let meta_data = LeafMetaData::from(heap_page.get_special_area());
        Self {
            heap_page,
            meta_data,
            key_type,
            storage_manager,
            leaf_layout,
            index_file,
        }
    }

    // Insert a key-value pair into the leaf node
    fn insert(&mut self, key: Vec<u8>, value: Rid) -> (Option<Vec<u8>>, Option<u64>) {
        let index = match (0..(self.heap_page.tuple_pointers.len() as u16))
            .collect::<Vec<u16>>()
            .binary_search_by_key(
                &ConcreteType::from_bytes(self.key_type, key.as_slice()),
                |slot_num| {
                    ConcreteType::from_bytes(
                        self.key_type,
                        self.heap_page
                            .get_field("key", *slot_num as u16)
                            .unwrap()
                            .as_slice(),
                    )
                },
            ) {
            Ok(index) => index,
            Err(index) => index,
        };

        let new_index_record = IndexRecord {
            key,
            value,
            layout: self.heap_page.layout.clone(),
        }
        .to_tuple();

        let free_space = self.heap_page.free_space() as i32 - 4;

        if free_space > new_index_record.tuple_size() as i32 {
            self.heap_page.insert_tuple(new_index_record);
            let last_inserted = self.heap_page.tuple_pointers.pop().unwrap();
            self.heap_page.tuple_pointers.insert(index, last_inserted);
            self.heap_page.rewrite_tuple_pointers_to_frame();
        } else {
            let pointers_num = self.heap_page.tuple_pointers.len();
            let mid = self.heap_page.tuple_pointers.len() as u16 / 2;
            let split_key = self.heap_page.get_field("key", mid);
            let mut right_index_records = Vec::new();
            let mut left_index_records = Vec::new();
            for tuple_pointer_index in 0..mid {
                let tuple_bytes = self.heap_page.get_multiple_fields(
                    vec![
                        "key".to_string(),
                        "block_num".to_string(),
                        "slot_num".to_string(),
                    ],
                    tuple_pointer_index,
                );
                let index_record =
                    IndexRecord::from_bytes(tuple_bytes, self.heap_page.layout.clone());
                left_index_records.push(index_record);
            }
            for tuple_pointer_index in mid..pointers_num as u16 {
                let tuple_bytes = self.heap_page.get_multiple_fields(
                    vec![
                        "key".to_string(),
                        "block_num".to_string(),
                        "slot_num".to_string(),
                    ],
                    tuple_pointer_index,
                );
                let index_record =
                    IndexRecord::from_bytes(tuple_bytes, self.heap_page.layout.clone());
                right_index_records.push(index_record);
                // vacuum the split records
                self.heap_page.mark_delete(tuple_pointer_index as usize);
            }
            let right_tuple_pointers = self.heap_page.tuple_pointers.split_off(mid as usize);
            self.heap_page.vacuum();
            let new_block = self
                .storage_manager
                .borrow_mut()
                .extend_file(&self.index_file);
            let right_frame = self
                .storage_manager
                .borrow_mut()
                .pin(new_block.clone())
                .unwrap();
            let mut right_heap_page = HeapPage::new_from_empty_special(
                right_frame,
                &new_block,
                self.heap_page.layout.clone(),
                16,
            );
            for record in right_index_records {
                right_heap_page.insert_tuple(record.to_tuple());
            }

            if index >= mid as usize {
                right_heap_page.insert_tuple(new_index_record);
                let last_inserted = right_heap_page.tuple_pointers.pop().unwrap();
                let index_to_put = index - self.heap_page.tuple_pointers.len();
                right_heap_page
                    .tuple_pointers
                    .insert(index_to_put, last_inserted);
                right_heap_page.rewrite_tuple_pointers_to_frame();
            } else {
                self.heap_page.insert_tuple(new_index_record);
                let last_inserted = self.heap_page.tuple_pointers.pop().unwrap();
                self.heap_page.tuple_pointers.insert(index, last_inserted);
                self.heap_page.rewrite_tuple_pointers_to_frame();
            }

            self.meta_data.next_node_blockid = new_block.block_num;
            self.heap_page.write_special_area(self.meta_data.to_bytes());
            let right_meta_data = LeafMetaData {
                next_node_blockid: 0,
                prev_node_blockid: self.heap_page.blk.block_num,
            };
            right_heap_page.write_special_area(right_meta_data.to_bytes());
            self.storage_manager
                .borrow_mut()
                .unpin(right_heap_page.frame.clone());

            return (split_key, Some(new_block.block_num));
        }

        return (None, None);
    }

    // Search for a key in the leaf node and return the associated values
    fn search(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        let mut results = Vec::new();

        for tuple_pointer_index in 0..self.heap_page.tuple_pointers.len() as u16 {
            let mut index_record = self.heap_page.get_multiple_fields(
                vec![
                    "key".to_string(),
                    "block_num".to_string(),
                    "slot_num".to_string(),
                ],
                tuple_pointer_index,
            );
            if index_record.remove("key").unwrap().unwrap() == key {
                let rid = Rid::new(
                    index_record.remove("block_num").unwrap().unwrap().to_u64(),
                    index_record.remove("slot_num").unwrap().unwrap().to_u16(),
                );
                results.push(rid);
            }
        }

        if !results.is_empty() {
            Some(results)
        } else {
            None
        }
    }

    pub fn get_greater_than_or_equal(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        let mut results = Vec::new();

        let mut tuple_index = 0_u16;
        for tuple_pointer_index in tuple_index..self.heap_page.tuple_pointers.len() as u16 {
            let mut index_record = self.heap_page.get_multiple_fields(
                vec![
                    "key".to_string(),
                    "block_num".to_string(),
                    "slot_num".to_string(),
                ],
                tuple_pointer_index,
            );
            if index_record.remove("key").unwrap().unwrap() >= key {
                tuple_index = tuple_pointer_index;
                break;
            }
        }

        results = self.get_index_records_from_position(tuple_index);

        let mut next_block = if self.meta_data.next_node_blockid == 0 {
            None
        } else {
            Some(BlockId {
                block_num: self.meta_data.next_node_blockid,
                filename: self.index_file.clone(),
            })
        };

        while next_block.is_some() {
            let block_id = next_block.unwrap();
            let next_frame = self
                .storage_manager
                .borrow_mut()
                .pin(block_id.clone())
                .unwrap();
            let next_heap = HeapPage::new(next_frame.clone(), &block_id, self.leaf_layout.clone());
            let next_leaf = LeafNodePage::new(
                next_heap,
                self.key_type,
                self.storage_manager.clone(),
                self.leaf_layout.clone(),
                self.index_file.clone(),
            );
            let new_results = next_leaf.get_index_records_from_position(0);

            if next_leaf.meta_data.next_node_blockid == 0 {
                next_block = None;
            } else {
                next_block = Some(BlockId {
                    block_num: next_leaf.meta_data.next_node_blockid,
                    filename: self.index_file.clone(),
                })
            }
            self.storage_manager.borrow_mut().unpin(next_frame);
            results.extend(new_results);
        }

        Some(results)
    }

    pub fn get_less_than_or_equal(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        let mut results = Vec::new();

        let mut tuple_index = self.heap_page.tuple_pointers.len() as u16 - 1;
        for tuple_pointer_index in (0..=tuple_index).rev() {
            let mut index_record = self.heap_page.get_multiple_fields(
                vec![
                    "key".to_string(),
                    "block_num".to_string(),
                    "slot_num".to_string(),
                ],
                tuple_pointer_index,
            );
            if index_record.remove("key").unwrap().unwrap() <= key {
                tuple_index = tuple_pointer_index;
                break;
            }
        }

        results = self.get_index_records_from_position_backwards(tuple_index);

        let mut prev_block = if self.meta_data.prev_node_blockid == 0 {
            None
        } else {
            Some(BlockId {
                block_num: self.meta_data.prev_node_blockid,
                filename: self.index_file.clone(),
            })
        };

        while prev_block.is_some() {
            let block_id = prev_block.unwrap();
            let prev_frame = self
                .storage_manager
                .borrow_mut()
                .pin(block_id.clone())
                .unwrap();
            let prev_heap = HeapPage::new(prev_frame.clone(), &block_id, self.leaf_layout.clone());
            let prev_leaf = LeafNodePage::new(
                prev_heap,
                self.key_type,
                self.storage_manager.clone(),
                self.leaf_layout.clone(),
                self.index_file.clone(),
            );
            let new_results = prev_leaf.get_index_records_from_position_backwards(
                prev_leaf.heap_page.tuple_pointers.len() as u16 - 1,
            );

            if prev_leaf.meta_data.prev_node_blockid == 0 {
                prev_block = None;
            } else {
                prev_block = Some(BlockId {
                    block_num: prev_leaf.meta_data.prev_node_blockid,
                    filename: self.index_file.clone(),
                })
            }
            self.storage_manager.borrow_mut().unpin(prev_frame);
            results.extend(new_results);
        }

        Some(results)
    }

    pub fn get_greater_than(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        let mut results = Vec::new();

        let mut tuple_index = 0_u16;
        for tuple_pointer_index in tuple_index..self.heap_page.tuple_pointers.len() as u16 {
            let mut index_record = self.heap_page.get_multiple_fields(
                vec![
                    "key".to_string(),
                    "block_num".to_string(),
                    "slot_num".to_string(),
                ],
                tuple_pointer_index,
            );
            if index_record.remove("key").unwrap().unwrap() > key {
                tuple_index = tuple_pointer_index;
                break;
            }
        }

        results = self.get_index_records_from_position(tuple_index);

        let mut next_block = if self.meta_data.next_node_blockid == 0 {
            None
        } else {
            Some(BlockId {
                block_num: self.meta_data.next_node_blockid,
                filename: self.index_file.clone(),
            })
        };

        while next_block.is_some() {
            let block_id = next_block.unwrap();
            let next_frame = self
                .storage_manager
                .borrow_mut()
                .pin(block_id.clone())
                .unwrap();
            let next_heap = HeapPage::new(next_frame.clone(), &block_id, self.leaf_layout.clone());
            let next_leaf = LeafNodePage::new(
                next_heap,
                self.key_type,
                self.storage_manager.clone(),
                self.leaf_layout.clone(),
                self.index_file.clone(),
            );
            let new_results = next_leaf.get_index_records_from_position(0);

            if next_leaf.meta_data.next_node_blockid == 0 {
                next_block = None;
            } else {
                next_block = Some(BlockId {
                    block_num: next_leaf.meta_data.next_node_blockid,
                    filename: self.index_file.clone(),
                })
            }
            self.storage_manager.borrow_mut().unpin(next_frame);
            results.extend(new_results);
        }

        Some(results)
    }

    pub fn get_less_than(&self, key: Vec<u8>) -> Option<Vec<Rid>> {
        let mut results = Vec::new();

        let mut tuple_index = self.heap_page.tuple_pointers.len() as u16 - 1;
        for tuple_pointer_index in (0..=tuple_index).rev() {
            let mut index_record = self.heap_page.get_multiple_fields(
                vec![
                    "key".to_string(),
                    "block_num".to_string(),
                    "slot_num".to_string(),
                ],
                tuple_pointer_index,
            );
            if index_record.remove("key").unwrap().unwrap() < key {
                tuple_index = tuple_pointer_index;
                break;
            }
        }

        results = self.get_index_records_from_position_backwards(tuple_index);

        let mut prev_block = if self.meta_data.prev_node_blockid == 0 {
            None
        } else {
            Some(BlockId {
                block_num: self.meta_data.prev_node_blockid,
                filename: self.index_file.clone(),
            })
        };

        while prev_block.is_some() {
            let block_id = prev_block.unwrap();
            let prev_frame = self
                .storage_manager
                .borrow_mut()
                .pin(block_id.clone())
                .unwrap();
            let prev_heap = HeapPage::new(prev_frame.clone(), &block_id, self.leaf_layout.clone());
            let prev_leaf = LeafNodePage::new(
                prev_heap,
                self.key_type,
                self.storage_manager.clone(),
                self.leaf_layout.clone(),
                self.index_file.clone(),
            );
            let new_results = prev_leaf.get_index_records_from_position_backwards(
                prev_leaf.heap_page.tuple_pointers.len() as u16 - 1,
            );

            if prev_leaf.meta_data.prev_node_blockid == 0 {
                prev_block = None;
            } else {
                prev_block = Some(BlockId {
                    block_num: prev_leaf.meta_data.prev_node_blockid,
                    filename: self.index_file.clone(),
                })
            }
            self.storage_manager.borrow_mut().unpin(prev_frame);
            results.extend(new_results);
        }

        Some(results)
    }

    pub fn get_index_records_from_position(&self, pos: u16) -> Vec<Rid> {
        let mut records = Vec::new();
        for record_index in pos..self.heap_page.tuple_pointers.len() as u16 {
            let mut index_record = self.heap_page.get_multiple_fields(
                vec![
                    "key".to_string(),
                    "block_num".to_string(),
                    "slot_num".to_string(),
                ],
                record_index,
            );
            let rid = Rid::new(
                index_record.remove("block_num").unwrap().unwrap().to_u64(),
                index_record.remove("slot_num").unwrap().unwrap().to_u16(),
            );
            records.push(rid);
        }
        records
    }

    pub fn get_index_records_from_position_backwards(&self, pos: u16) -> Vec<Rid> {
        let mut records = Vec::new();
        for record_index in (0..=pos).rev() {
            let mut index_record = self.heap_page.get_multiple_fields(
                vec![
                    "key".to_string(),
                    "block_num".to_string(),
                    "slot_num".to_string(),
                ],
                record_index,
            );
            let rid = Rid::new(
                index_record.remove("block_num").unwrap().unwrap().to_u64(),
                index_record.remove("slot_num").unwrap().unwrap().to_u16(),
            );
            records.push(rid);
        }
        records
    }
}
