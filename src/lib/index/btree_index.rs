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

const BLOCK_NUM_SIZE: usize = 8;
const SLOT_NUM_SIZE: usize = 2;
const ORDER: usize = 4; // B+Tree order
const INDEX_RECORD_SIZE: usize = 14;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Rid {
    block_num: u64,
    slot_num: u16,
}

impl Rid {
    pub fn new(block_num: u64, slot_num: u16) -> Self {
        Self {
            block_num,
            slot_num,
        }
    }

    // Convert Rid to bytes
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(BLOCK_NUM_SIZE + SLOT_NUM_SIZE);
        bytes.extend_from_slice(&self.block_num.to_ne_bytes());
        bytes.extend_from_slice(&self.slot_num.to_ne_bytes());
        bytes
    }

    // Convert bytes to Rid
    fn from_bytes(bytes: &[u8]) -> Rid {
        let block_num = bytes.extract_u64(0);
        let slot_num = bytes.extract_u16(8);

        Rid {
            block_num,
            slot_num,
        }
    }
}

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
pub struct BPTree {
    root: NodePage,
    root_frame: FrameRef,
    key_type: Type,
    storage_manager: Rc<RefCell<StorageManager>>,
    internal_layout: Rc<Layout>,
    leaf_layout: Rc<Layout>,
}

// B+Tree implementation
impl BPTree {
    pub fn init(
        root_block: BlockId,
        key_type: Type,
        storage_manager: Rc<RefCell<StorageManager>>,
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
            root: NodePage::init(
                root_frame.clone(),
                key_type,
                storage_manager.clone(),
                internal_layout.clone(),
                leaf_layout.clone(),
            ),
            root_frame,
            key_type,
            storage_manager,
            internal_layout,
            leaf_layout,
        }
    }

    // Create a new B+Tree
    pub fn new(
        root_block: BlockId,
        key_type: Type,
        storage_manager: Rc<RefCell<StorageManager>>,
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
            ),
            root_frame,
            key_type,
            storage_manager,
            internal_layout,
            leaf_layout,
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
                        let root_frame = self.root_frame.borrow().blockid.as_ref().unwrap().clone();
                        let new_root_heap = HeapPage::new_from_empty(
                            self.root_frame.clone(),
                            &root_frame,
                            self.internal_layout.clone(),
                        );

                        let mut new_root = InternalNodePage::new(
                            new_root_heap,
                            self.key_type,
                            self.storage_manager.clone(),
                            self.internal_layout.clone(),
                            self.leaf_layout.clone(),
                        );

                        let left_blockid =
                            self.storage_manager.borrow_mut().extend_file("test_btree");
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
                            filename: "test_btree".to_string(),
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
                    let root_frame = self.root_frame.borrow().blockid.as_ref().unwrap().clone();
                    let new_root_heap = HeapPage::new_from_empty(
                        self.root_frame.clone(),
                        &root_frame,
                        self.internal_layout.clone(),
                    );

                    let mut new_root = InternalNodePage::new(
                        new_root_heap,
                        self.key_type,
                        self.storage_manager.clone(),
                        self.internal_layout.clone(),
                        self.leaf_layout.clone(),
                    );

                    let right_blockid = BlockId {
                        block_num: split_block_num.unwrap(),
                        filename: "test_btree".to_string(),
                    };
                    let right_frame = self.storage_manager.borrow_mut().pin(right_blockid.clone());
                    let left_blockid = self.storage_manager.borrow_mut().extend_file("test_btree");
                    let mut left_frame = self
                        .storage_manager
                        .borrow_mut()
                        .pin(left_blockid.clone())
                        .unwrap();
                    left_frame.borrow_mut().page.payload = root_payload;
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
            ))
        } else {
            heap_page.layout = leaf_layout.clone();
            NodePage::Leaf(LeafNodePage::new(
                heap_page,
                key_type,
                storage_manager.clone(),
                leaf_layout,
            ))
        }
    }

    pub fn init(
        frame: FrameRef,
        key_type: Type,
        storage_manager: Rc<RefCell<StorageManager>>,
        internal_layout: Rc<Layout>,
        leaf_layout: Rc<Layout>,
    ) -> Self {
        let block_id = frame.borrow().blockid.as_ref().unwrap().clone();
        let mut heap_page =
            HeapPage::new_from_empty_special(frame.clone(), &block_id, leaf_layout.clone(), 16);
        NodePage::Leaf(LeafNodePage::new(
            heap_page,
            key_type,
            storage_manager.clone(),
            leaf_layout,
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
}

// Internal node implementation
impl InternalNodePage {
    pub fn new(
        heap_page: HeapPage,
        key_type: Type,
        storage_manager: Rc<RefCell<StorageManager>>,
        internal_layout: Rc<Layout>,
        leaf_layout: Rc<Layout>,
    ) -> InternalNodePage {
        Self {
            heap_page,
            key_type,
            storage_manager,
            internal_layout,
            leaf_layout,
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
        let target_blockid = BlockId::new("test_btree", target_block_num);

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
        );

        match node_page {
            NodePage::Internal(mut target_node) => {
                let free_space = self.heap_page.free_space() as i32 - 4;
                let (split_key, split_block_num) = target_node.insert(key.clone(), value);
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

                        let new_block = self.storage_manager.borrow_mut().extend_file("test_btree");
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
            NodePage::Leaf(mut target_node) => {
                let (split_key, split_block_num) = target_node.insert(key, value);
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

                        let new_block = self.storage_manager.borrow_mut().extend_file("test_btree");
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
        let target_blockid = BlockId::new("test_btree", target_block_num);

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
        );

        match node_page {
            NodePage::Internal(target_node) => target_node.search(key),
            NodePage::Leaf(target_node) => {
                let results = target_node.search(key);
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
}

impl LeafNodePage {
    pub fn new(
        heap_page: HeapPage,
        key_type: Type,
        storage_manager: Rc<RefCell<StorageManager>>,
        leaf_layout: Rc<Layout>,
    ) -> LeafNodePage {
        let meta_data = LeafMetaData::from(heap_page.get_special_area());
        Self {
            heap_page,
            meta_data,
            key_type,
            storage_manager,
            leaf_layout,
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
            let new_block = self.storage_manager.borrow_mut().extend_file("test_btree");
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

            let mut last_record_for_check = self.heap_page.tuple_pointers.len() - 1;
            let mut last_index_rec = self.heap_page.get_tuple_fields(last_record_for_check);

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

            last_record_for_check = self.heap_page.tuple_pointers.len() - 1;
            last_index_rec = self.heap_page.get_tuple_fields(last_record_for_check);

            self.meta_data.next_node_blockid = new_block.block_num;
            self.heap_page.write_special_area(self.meta_data.to_bytes());
            let right_meta_data = LeafMetaData {
                next_node_blockid: 0,
                prev_node_blockid: self.heap_page.blk.block_num,
            };

            last_record_for_check = self.heap_page.tuple_pointers.len() - 1;
            last_index_rec = self.heap_page.get_tuple_fields(last_record_for_check);

            right_heap_page.write_special_area(right_meta_data.to_bytes());

            last_record_for_check = self.heap_page.tuple_pointers.len() - 1;
            last_index_rec = self.heap_page.get_tuple_fields(last_record_for_check);
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
}
