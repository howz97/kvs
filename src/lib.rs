// #![deny(missing_docs)]
//! This is key-value store lib

use failure::Error;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::{File, read_dir};
use std::io;
use std::io::{Read, Seek};
use std::path::{PathBuf, Path};
use std::result;

/// KvStore is the core data structure keeping all KV pairs
pub struct KvStore {
    table: HashMap<String, Index>,
    active_file: File,
}

pub struct Index {
    file: u32,
    offset: u32,
}

impl Index {
    fn new(file: u32, offset: u32) -> Self {
        Index { file, offset }
    }
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
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Command::put(key, val);
        let offset = 
        Result::Ok(())
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
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Result::Ok(self.table.get(&key).cloned())
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
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.table.remove(&key);
        Result::Ok(())
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir = read_dir(path.into())?
        .map(|res| res.map(|e| e.path()))
        .collect::<result::Result<Vec<_>, io::Error>>()?;
        dir.sort();
        for e in dir {

        }
        // todo
        let file = File::open(path.into())?;
        let file_len = file.metadata()?.len() as u32;
        let offset: u32 = 0;
        let store = KvStore::default();
        while offset < file_len {
            let mut cmd_len = [0; 4];
            file.read_exact(&mut cmd_len)?;
            let cmd_len = u32::from_be_bytes(cmd_len);
            let mut cmd = vec![0; cmd_len as usize];
            file.read_exact(&mut cmd);
            let cmd: Command = serde_json::from_slice(&cmd)?;
            if !cmd.is_del {
                store.table.insert(cmd.key, Index::new(0, offset));
            } else {
                store.table.remove(&cmd.key);
            }
            let offset = file.stream_position()? as u32;
        }
        Ok(store)
    }
}

pub type Result<T> = result::Result<T, Error>;

#[derive(Deserialize, Debug)]
struct Command {
    key: String,
    val: String,
    is_del: bool,
}

impl Command {
    fn put(key: String, val: String) -> Self {
        let is_del = false;
        Command { key, val, is_del }
    }
    fn del(key: String) -> Self {
        let val = String::new();
        let is_del = true;
        Command { key, val, is_del }
    }
}
