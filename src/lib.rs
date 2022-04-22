use std::collections::HashMap;

pub struct KvStore {
    table: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore {
            table: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, val: String) {
        self.table.insert(key, val);
    }

    pub fn get(&self, key: String) -> Option<String> {
        if let Some(s) = self.table.get(&key) {
            Some(s.clone().to_string())
        } else {
            None
        }
    }

    pub fn remove(&mut self, key: String) {
        self.table.remove(&key);
    }
}
