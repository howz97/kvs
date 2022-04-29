// #![deny(missing_docs)]
//! This is key-value store lib

use failure;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::{read_dir, rename, File};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::result;

/// KvStore is the core data structure keeping all KV pairs
pub struct KvStore {
    table: HashMap<String, Index>,
    active_file: File, // todo: wrap buffer
    older_files: Vec<File>,
    dir_path: PathBuf,
}

#[derive(Debug)]
pub struct Index {
    file: u32,
    offset: u32,
}

impl Index {
    fn new(file: u32, offset: u32) -> Self {
        Index { file, offset }
    }
}

impl KvStore {
    /// Insert/Update key-value
    ///
    /// # Example
    ///
    /// ```rust
    /// # use kvs::{KvStore, Result};
    /// # use tempfile::TempDir;
    /// # fn main() -> Result<()> {
    /// #   let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    /// #   let mut store = KvStore::open(temp_dir.path())?;
    /// #   store.set("a".to_string(), "A".to_string())
    /// # }
    /// ```
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let offset = self.active_file.stream_position()? as u32;
        let cmd = serde_json::to_vec(&Command::put(key.clone(), val))?;
        let cmd_len = cmd.len() as u32;
        self.active_file.write_all(&cmd_len.to_be_bytes())?;
        self.active_file.write_all(&cmd)?;
        self.table
            .insert(key, Index::new(self.older_files.len() as u32, offset));
        Ok(())
    }

    /// Get value by key
    ///
    /// # Example
    ///
    /// ```rust
    /// # use kvs::{KvStore, Result};
    /// # use tempfile::TempDir;
    /// # fn main() -> Result<()> {
    /// #   let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    /// #   let mut store = KvStore::open(temp_dir.path())?;
    /// #   if let Some(v) = store.get("a".to_string())? {
    /// #       println!("{}", v)
    /// #   }
    /// #   Ok(())
    /// # }
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(idx) = self.table.get(&key) {
            let mut file;
            if idx.file == self.older_files.len() as u32 {
                file = &mut self.active_file
            } else {
                file = self.older_files.get_mut(idx.file as usize).unwrap();
            }
            file.seek(SeekFrom::Start(idx.offset as u64))?;
            let cmd = read_cmd(&mut file)?;
            Ok(Some(cmd.val))
        } else {
            return Ok(None);
        }
    }

    /// Remove value by key
    ///
    /// # Example
    ///
    /// ```rust
    /// # use kvs::{KvStore, Result};
    /// # use tempfile::TempDir;
    /// # fn main() -> Result<()> {
    /// #   let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    /// #   let mut store = KvStore::open(temp_dir.path())?;
    /// #   store.set("a".to_string(), "A".to_string())?;
    /// #   store.remove("a".to_string())
    /// # }
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.table.remove(&key).ok_or(MyErr::KeyNotFound)?;
        let cmd = serde_json::to_vec(&Command::del(key.clone()))?;
        let cmd_len = cmd.len() as u32;
        self.active_file.write_all(&cmd_len.to_be_bytes())?;
        self.active_file.write_all(&cmd)?;
        Ok(())
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir_path = path.into();
        let mut dir = read_dir(dir_path.clone())?
            .map(|res| res.map(|e| e.path()))
            .filter(|res| {
                if let Ok(p) = res {
                    if p.extension().unwrap() == "kvs" {
                        return true;
                    }
                }
                false
            })
            .collect::<result::Result<Vec<_>, io::Error>>()?;

        dir.sort();
        let mut table = HashMap::new();
        let mut older = Vec::new();
        for (i, e) in dir.iter().enumerate() {
            let mut file = File::open(e)?;
            let file_len = file.metadata()?.len() as u32;
            let mut offset: u32 = 0;
            while offset < file_len {
                let cmd = read_cmd(&mut file)?;
                if !cmd.is_del {
                    table.insert(cmd.key, Index::new(i as u32, offset));
                } else {
                    table.remove(&cmd.key);
                }
                offset = file.stream_position()? as u32;
            }
            older.push(file);
        }

        let mut active_path = dir_path.clone();
        active_path.push("active.kvs");
        let store = KvStore {
            table: table,
            active_file: File::options()
                .read(true)
                .append(true)
                .create_new(true)
                .open(active_path.as_path())?,
            older_files: older,
            dir_path: dir_path,
        };
        Ok(store)
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        let mut from = self.dir_path.clone();
        from.push("active.kvs");
        let mut to = self.dir_path.clone();
        to.push(format!("{}_older.kvs", self.older_files.len()));
        rename(from, to).expect("failed to rename active file");
    }
}

fn read_cmd(file: &mut File) -> Result<Command> {
    let mut cmd_len = [0; 4];
    file.read_exact(&mut cmd_len)?;
    let cmd_len = u32::from_be_bytes(cmd_len);
    let mut cmd = vec![0; cmd_len as usize];
    file.read_exact(&mut cmd)?;
    let cmd: Command = serde_json::from_slice(&cmd)?;
    Ok(cmd)
}

pub type Result<T> = result::Result<T, failure::Error>;

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Debug)]
pub enum MyErr {
    KeyNotFound,
}

impl fmt::Display for MyErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            MyErr::KeyNotFound => write!(f, "Key not found"),
        }
    }
}

impl Error for MyErr {}
