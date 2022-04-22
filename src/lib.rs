#![deny(missing_docs)]
//! This is key-value store lib

use std::collections::HashMap;

/// KvStore is the core data structure keeping all KV pairs
pub struct KvStore {
    table: HashMap<String, String>,
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore {
    /// Creates a new instance of an `KvStore`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use kvs::KvStore;
    /// # fn main() {
    /// #   let mut store = KvStore::new();
    /// #   store.set("a".to_string(), "A".to_string());
    /// # }
    /// ```
    pub fn new() -> Self {
        KvStore {
            table: HashMap::new(),
        }
    }

    /// Insert/Update key-value
    ///
    /// # Example
    ///
    /// ```rust
    /// # use kvs::KvStore;
    /// # fn main() {
    /// #   let mut store = KvStore::new();
    /// #   store.set("a".to_string(), "A".to_string());
    /// # }
    /// ```
    pub fn set(&mut self, key: String, val: String) {
        self.table.insert(key, val);
    }

    /// Get value by key
    ///
    /// # Example
    ///
    /// ```rust
    /// # use kvs::KvStore;
    /// # fn main() {
    /// #   let mut store = KvStore::new();
    /// #   if let Some(v) = store.get("a".to_string()) {
    /// #       println!("{}", v)
    /// #   }
    /// # }
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        self.table.get(&key).cloned()
    }

    /// Remove value by key
    ///
    /// # Example
    ///
    /// ```rust
    /// # use kvs::KvStore;
    /// # fn main() {
    /// #   let mut store = KvStore::new();
    /// #   store.remove("a".to_string());
    /// # }
    /// ```
    pub fn remove(&mut self, key: String) {
        self.table.remove(&key);
    }
}
