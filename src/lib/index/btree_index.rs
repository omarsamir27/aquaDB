use std::cell::RefCell;
use crate::common::numerical::ByteMagic;
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
    key: u32,
    value: Rid,
}

impl IndexRecord {
    // Convert IndexRecord to bytes
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(std::mem::size_of::<u32>() + BLOCK_NUM_SIZE + SLOT_NUM_SIZE);
        bytes.extend_from_slice(&self.key.to_ne_bytes());
        bytes.extend_from_slice(&self.value.to_bytes());
        bytes
    }

    // Convert bytes to IndexRecord
    fn from_bytes(bytes: &[u8]) -> IndexRecord {
        let key = bytes.extract_u32(0);
        let value = Rid::from_bytes(&bytes[4..]);

        IndexRecord { key, value }
    }
}

// B+Tree structure
#[derive(Debug, Clone)]
pub struct BPTree {
    root: Rc<RefCell<Node>>,
    latest_id: u64,
}

// B+Tree implementation
impl BPTree {
    // Create a new B+Tree
    pub fn new() -> Self {
        BPTree {
            root: Rc::new(RefCell::from(Node::Leaf(LeafNode {
                id: 1,
                index_records: Vec::new(),
                next: None,
            }))),
            latest_id: 1,
        }
    }

    // Insert a key-value pair into the B+Tree
    pub fn insert(&mut self, key: u32, value: Rid) {
        let (split_key, right) = self.root.borrow_mut().insert(key, value, self.latest_id + 1);
        self.latest_id += 2;
        if let Some(split_key) = split_key {
            let new_internal = InternalNode {
                keys: vec![split_key],
                children: vec![self.root.clone(), right],
            };
            self.root = Rc::new(RefCell::from(Node::Internal(new_internal)));
        }
    }

    // Search for a key in the B+Tree and return the associated values
    pub fn search(&self, key: u32) -> Option<Vec<Rid>> {
        let node = self.root.borrow();
        node.search(key)
    }
}

// B+Tree node variants
#[derive(Debug, Clone)]
pub enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

// B+Tree node implementation
impl Node {
    // Insert a key-value pair into the node
    fn insert(&mut self, key: u32, value: Rid, id: u64) -> (Option<u32>, Rc<RefCell<Node>>) {
        match self {
            Node::Internal(node) => node.insert(key, value, id),
            Node::Leaf(node) => node.insert(key, value, id),
        }
    }

    // Search for a key in the node and return the associated values
    fn search(&self, key: u32) -> Option<Vec<Rid>> {
        match self {
            Node::Internal(node) => node.search(key),
            Node::Leaf(node) => node.search(key),
        }
    }

    // Convert Node to bytes
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            Node::Internal(node) => node.to_bytes(),
            Node::Leaf(node) => node.to_bytes(),
        }
    }

    // Convert bytes to Node
    fn from_bytes(bytes: &[u8]) -> Node {
        if bytes.is_empty() {
            panic!("Invalid bytes to construct Node");
        }

        match bytes[0] {
            0 => {
                let internal_node = InternalNode::from_bytes(&bytes[1..]);
                Node::Internal(internal_node)
            }
            1 => {
                let leaf_node = LeafNode::from_bytes(&bytes[1..]);
                Node::Leaf(leaf_node)
            }
            _ => panic!("Invalid bytes to construct Node"),
        }
    }

    // Get the size of the Node in bytes
    fn size(&self) -> usize {
        match self {
            Node::Internal(node) => 1 + node.to_bytes().len(),
            Node::Leaf(node) => 1 + node.to_bytes().len(),
        }
    }

    fn get_internal(&self) -> Result<&InternalNode, ()> {
        match self {
            Node::Internal(n) => {
                Ok(n)
            }
            Node::Leaf(_) => {
                Err(())
            }
        }
    }

    fn get_leaf(&self) -> Result<&LeafNode, ()> {
        match self {
            Node::Internal(_) => {
                Err(())
            }
            Node::Leaf(n) => {
                Ok(n)
            }
        }
    }
}

// Internal node of the B+Tree
#[derive(Debug, Clone)]
pub struct InternalNode {
    keys: Vec<u32>,
    children: Vec<Rc<RefCell<Node>>>,
}

// Internal node implementation
impl InternalNode {
    // Insert a key-value pair into the internal node
    fn insert(&mut self, key: u32, value: Rid, id: u64) -> (Option<u32>, Rc<RefCell<Node>>) {
        let child_index = match self.keys.binary_search(&key) {
            Ok(index) => index + 1,
            Err(index) => index,
        };

        let (split_key, right) =
            self.children[child_index].borrow_mut().insert(key, value, id);

        if let Some(split_key) = split_key {
            let mut new_keys = self.keys.clone();
            let mut new_children = self.children.clone();

            self.keys.insert(child_index, split_key);
            self.children.insert(child_index + 1, right);

            if self.keys.len() <= ORDER {
                return (
                    None,
                    Rc::from(RefCell::from(Node::Internal(InternalNode {
                        keys: vec![],
                        children: vec![],
                    }))),
                )
            }

            let mid = self.keys.len() / 2;
            let split_key = self.keys.remove(mid);
            let right_keys = self.keys.split_off(mid);
            let right_children = self.children.split_off(mid + 1);

            (
                Some(split_key),
                Rc::from(RefCell::from(Node::Internal(InternalNode {
                    keys: right_keys,
                    children: right_children,
                }))),
            )
        } else {
            (
                None,
                Rc::from(RefCell::from(Node::Internal(self.clone()))),
            )
        }
    }

    // Search for a key in the internal node and return the associated values
    fn search(&self, key: u32) -> Option<Vec<Rid>> {
        let child_index = match self.keys.binary_search(&key) {
            Ok(index) => index + 1,
            Err(index) => index,
        };

        self.children[child_index].borrow().search(key)
    }

    // Convert InternalNode to bytes
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.push(0_u8);

        for key in &self.keys {
            bytes.extend_from_slice(&key.to_ne_bytes());
        }

        for child in &self.children {
            bytes.extend_from_slice(&child.borrow().to_bytes());
        }

        bytes
    }

    // Convert bytes to InternalNode
    fn from_bytes(bytes: &[u8]) -> InternalNode {
        let mut keys = Vec::new();
        let mut children = Vec::new();

        let key_size = 4_u32;

        let mut offset = 0;
        while offset < bytes.len() {
            let key = bytes.extract_u32(0);
            keys.push(key);
            offset += key_size as usize;
        }

        while offset < bytes.len() {
            let child_bytes = &bytes[offset..];
            let child = Node::from_bytes(child_bytes);
            offset += child.size();
            children.push(Rc::new(RefCell::from(child)));
        }

        InternalNode { keys, children }
    }
}

// Leaf node of the B+Tree
#[derive(Debug, Clone)]
pub struct LeafNode {
    id: u64,
    index_records: Vec<IndexRecord>,
    next: Option<Rc<RefCell<Node>>>,
}

// Leaf node implementation
impl LeafNode {
    // Insert a key-value pair into the leaf node
    fn insert(&mut self, key: u32, value: Rid, id: u64) -> (Option<u32>, Rc<RefCell<Node>>) {
        let index = match self
            .index_records
            .binary_search_by_key(&key, |record| record.key)
        {
            Ok(index) => index,
            Err(index) => index,
        };

        self.index_records.insert(index, IndexRecord { key, value });

        if self.index_records.len() <= ORDER {
            return (
                None,
                Rc::from(RefCell::from(Node::Leaf(LeafNode {
                    id: 0,
                    index_records: vec![],
                    next: self.next.clone(),
                })))
            );
        }

        let mid = self.index_records.len() / 2;
        let split_key = self.index_records[mid].key;
        let right_records = self.index_records.split_off(mid);
        let id = self.id + 1;

        self.next = Some(Rc::new(RefCell::from(Node::Leaf(LeafNode {
            id,
            index_records: right_records,
            next: self.next.clone(),
        }))));



        (
            Some(split_key),
            self.next.as_ref().unwrap().clone(),
        )
    }

    // Search for a key in the leaf node and return the associated values
    fn search(&self, key: u32) -> Option<Vec<Rid>> {
        let mut results = Vec::new();

        for record in &self.index_records {
            if record.key == key {
                results.push(record.value.clone());
            }
        }

        if !results.is_empty() {
            Some(results)
        } else {
            None
        }
    }

    // Convert LeafNode to bytes
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(1_u8);
        bytes.extend_from_slice(&self.id.to_ne_bytes());
        bytes.extend_from_slice(&self.index_records.len().to_ne_bytes());

        // Serialize the index records
        for record in &self.index_records {
            bytes.extend_from_slice(&record.to_bytes());
        }

        // Serialize the next node or append zeros if there is no next node
        if let Some(next_node) = &self.next {
            bytes.extend_from_slice(&next_node.borrow().get_leaf().unwrap().id.to_ne_bytes());
        } else {
            bytes.extend_from_slice(&[0_u8; 8]);
        }

        bytes
    }

    // Convert bytes to LeafNode

    fn from_bytes(bytes: &[u8]) -> Self {
        todo!();
        // let mut index_records = Vec::new();
        // let mut offset = 8;
        //
        // let id = bytes.extract_u64(0);
        // let num_records = bytes.extract_usize(8);
        // // Deserialize the index records
        // for i in 0..num_records {
        //     let record_bytes = &bytes[offset..offset + INDEX_RECORD_SIZE];
        //     let record = IndexRecord::from_bytes(record_bytes);
        //     index_records.push(record);
        //     offset += INDEX_RECORD_SIZE;
        // }
        //
        // // Deserialize the next node if not all zeros
        // let next_node_bytes = &bytes[offset..];
        // let next_node_id = if next_node_bytes.iter().all(|&byte| byte == 0) {
        //     None
        // } else {
        //     Some(next_node_bytes.extract_u64(0))
        // };
        //
        // LeafNode {
        //     id,
        //     index_records,
        //     next: next_node_id,
        // }
    }

    fn get_id(&self) -> u64 {
        self.id
    }
}

