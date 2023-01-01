use bincode::{Decode, Encode};
use std::collections::btree_map::{Entry, RangeMut};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display};
use std::ops::Range;
// use serde::{Deserialize,};

/// Custom BTree Wrapper that allows for multiple Values using a vector as a value for standard library Btree
#[derive(Debug, Encode, Decode)]
pub struct BTreeMultimap<T: std::cmp::Ord, U> {
    btreemap: BTreeMap<T, Vec<U>>,
}

impl<T: Ord + ToOwned<Owned = T> + Encode + Decode, U: Encode + Decode> BTreeMultimap<T, U> {
    /// Creates an empty BTreeMultimap
    pub fn new() -> BTreeMultimap<T, U> {
        Self {
            btreemap: BTreeMap::<T, Vec<U>>::new(),
        }
    }

    /// Insert a vector into the BTreeMultimap joining the the value vector if one exists for the same key
    pub fn insert_vec_concat(&mut self, k: T, v: &[U])
    where
        U: Clone,
    {
        match self.btreemap.entry(k) {
            Entry::Vacant(vacant) => {
                vacant.insert(v.to_vec());
                ()
            }
            Entry::Occupied(mut occupied) => occupied.get_mut().extend_from_slice(v),
        };
    }
    /// Insert a vector into the BTreeMultimap appending the the value vector if one exists for the same key
    pub fn insert_vec(&mut self, k: T, v: &[U])
    where
        U: Clone,
    {
        self.btreemap.insert(k, v.to_vec());
    }

    /// Insert an element by pushing it into an existing vector V for key K , or allocates the vector
    /// if it does not exist
    pub fn insert_element(&mut self, k: T, v: U)
    where
        U: Clone,
    {
        match self.btreemap.entry(k) {
            Entry::Vacant(vacant) => {
                vacant.insert(vec![v]);
                ()
            }
            Entry::Occupied(mut occupied) => occupied.get_mut().push(v),
        };
    }

    /// Returns an Iterator over the internal Btree
    pub fn range(&self, range: Range<T>) -> std::collections::btree_map::Range<'_, T, Vec<U>> {
        self.btreemap.range(range)
    }

    /// Returns a mutable Iterator over the internal Btree
    pub fn range_mut(&mut self, range: Range<T>) -> RangeMut<'_, T, Vec<U>> {
        self.btreemap.range_mut(range)
    }

    /// Returns a reference for vector V matching key K
    pub fn get(&self, k: T) -> Option<&Vec<U>> {
        self.btreemap.get(&k)
    }

    /// Returns a mutable reference for vector V matching key K
    pub fn get_mut(&mut self, k: T) -> Option<&mut Vec<U>> {
        self.btreemap.get_mut(&k)
    }

    /// Pops the last element of the vector V matching key K if it exists and removes the entry if
    /// the vector is empty after that
    pub fn pop_first_exact(&mut self, k: T) -> Option<U> {
        let vec = self.btreemap.get_mut(&k);
        if let Some(vec) = vec {
            let element = vec.pop().unwrap();
            if vec.is_empty() {
                self.btreemap.remove(&k);
            }
            Some(element)
        } else {
            None
        }
    }

    /// Pops the first element satisfying a predicate  in the vector V matching key K if it exists
    /// and removes the entry if the vector is empty after that
    pub fn pop_predicate_exact<P>(&mut self, k: T, mut predicate: P) -> Option<U>
    where
        P: FnMut(&U) -> bool,
    {
        let vec = self.btreemap.get_mut(&k);
        if let Some(vec) = vec {
            let idx = vec.iter().position(predicate);
            match idx {
                None => None,
                Some(idx) => {
                    let element = vec.remove(idx);
                    if vec.is_empty() {
                        self.btreemap.remove(&k);
                    }
                    Some(element)
                }
            }
        } else {
            None
        }
    }

    /// Pops the last element of the vector V matching the first key K in a range if it exists
    /// and removes the entry if the vector is empty after that
    pub fn pop_first_range(&mut self, range: Range<T>) -> Option<(T, U)> {
        let entry = {
            let mut iter = self.range_mut(range);
            iter.nth(0)
        };
        let entry = match entry {
            None => None,
            Some((k, v)) => Some((k.to_owned(), v.pop().unwrap(), v.is_empty())),
        };
        match entry {
            None => None,
            Some((k, v, empty)) => {
                if empty {
                    self.btreemap.remove(&k);
                }
                Some((k, v))
            }
        }
    }

    /// Pops the first element satisfying a predicate in the vector V matching the first key K in a range if it exists
    /// and removes the entry if the vector is empty after that
    pub fn pop_predicate_range<P>(&mut self, range: Range<T>, predicate: P) -> Option<(T, U)>
    where
        P: FnMut(&U) -> bool,
    {
        let entry = {
            let mut iter = self.range_mut(range);
            iter.nth(0)
        };
        let entry = match entry {
            None => None,
            Some((k, v)) => {
                let idx = v.iter().position(predicate);
                match idx {
                    None => None,
                    Some(idx) => {
                        let element = v.swap_remove(idx);
                        Some((k.to_owned(), element, v.is_empty()))
                    }
                }
            }
        };
        match entry {
            None => None,
            Some((k, v, empty)) => {
                if empty {
                    self.btreemap.remove(&k);
                }
                Some((k, v))
            }
        }
    }

    /// Removes the last element of vector V matching key K such that K is the first key satisfying
    /// a predicate
    pub fn pop_first_key_match<P>(&mut self, predicate: P) -> Option<(T, U)>
    where
        P: FnMut(&(&T, &mut Vec<U>)) -> bool,
    {
        let entry = self.btreemap.iter_mut().find(predicate);
        let entry = match entry {
            None => None,
            Some((k, v)) => Some((k.to_owned(), v.pop().unwrap(), v.is_empty())),
        };
        match entry {
            None => None,
            Some((k, v, empty)) => {
                if empty {
                    self.btreemap.remove(&k);
                }
                Some((k, v))
            }
        }
    }

    /// Removes the last element of vector V matching key K such that K is the first key bigger than
    /// a given value
    pub fn pop_first_bigger_than(&mut self, value: T) -> Option<(T, U)> {
        self.pop_first_key_match(|(k, v)| **k >= value)
    }

    /// Helper function to print the tree
    pub fn print_all(&self)
    where
        T: Display,
        U: Display + Debug,
    {
        for (k, v) in self.btreemap.iter() {
            println!("K : {} , V : {:?} ", k, v)
        }
    }

    pub fn from_bytes(data: &[u8]) -> BTreeMultimap<T, U> {
        let config = bincode::config::standard();
        let tree: BTreeMultimap<T, U> = bincode::decode_from_slice(data, config).unwrap().0;
        tree
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let config = bincode::config::standard();
        bincode::encode_to_vec(self.clone(), config).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::common::btree_multimap::BTreeMultimap;

    #[test]
    fn insert_vec() {
        let mut multimap = BTreeMultimap::new();
        multimap.insert_vec(100_u16, vec![1_u16, 2, 3, 4].as_slice());
        assert_eq!(multimap.get(100).unwrap(), &vec![1_u16, 2, 3, 4]);
    }

    #[test]
    fn insert_vec_concat() {
        let mut multimap = BTreeMultimap::new();
        multimap.insert_vec(100_u16, vec![1_u16, 2, 3, 4].as_slice());
        multimap.insert_vec_concat(100, vec![5, 6, 7].as_slice());
        assert_eq!(multimap.get(100).unwrap(), &vec![1_u16, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn range() {
        let mut multimap = BTreeMultimap::new();
        multimap.insert_vec(100_u16, vec![1_u16, 2, 3, 4].as_slice());
        multimap.insert_vec(150, vec![5, 6].as_slice());
        multimap.insert_vec(200, vec![7, 8].as_slice());
        let mut range = multimap.range(70..110);
        let (k, v) = range.nth(0).unwrap();
        assert_eq!((k, v), (&100, &vec![1, 2, 3, 4]));
        // multimap.print_all()
    }

    #[test]
    fn range_mut() {
        let mut multimap = BTreeMultimap::new();
        multimap.insert_vec(100_u16, vec![1_u16, 2, 3, 4].as_slice());
        multimap.insert_vec(150, vec![5, 6].as_slice());
        multimap.insert_vec(200, vec![7, 8].as_slice());
        let mut range = multimap.range_mut(70..110);
        range.nth(0).unwrap().1.push(99);
        let v = multimap.get(100).unwrap();
        assert_eq!(v, &vec![1, 2, 3, 4, 99]);
        // multimap.print_all()
    }
}
