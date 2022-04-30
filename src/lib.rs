// #![deny(missing_docs)]
//! This is key-value store lib

use failure;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::fs::{read_dir, remove_file, rename, File};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::result;

const SEGMENT_SIZE: u64 = 1 << 20;
const COMPACT_THRESHOLD: usize = 10;

/// KvStore is the core data structure keeping all KV pairs
pub struct KvStore {
    table: HashMap<String, Index>,
    older_files: BTreeMap<u32, File>,
    active_file: File,
    active_id: u32,
    dir_path: PathBuf,
}

#[derive(Debug)]
pub struct Index {
    file: u32, // file_id
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
        append_cmd(&mut self.active_file, Command::put(key.clone(), val))?;
        self.table.insert(key, Index::new(self.active_id, offset));
        self.maybe_cut();
        Ok(())
    }

    fn maybe_cut(&mut self) {
        if self
            .active_file
            .metadata()
            .expect("active file metadata unknown")
            .len()
            >= SEGMENT_SIZE
        {
            let older = self
                .rename_active()
                .expect("failed to rename active file when drop KvStore");
            self.active_file =
                new_active_file(self.dir_path.clone()).expect("failed to create active file");
            self.older_files
                .insert(self.active_id, File::open(older).unwrap());
            self.active_id += 1;
            self.maybe_compaction();
        }
    }

    fn maybe_compaction(&mut self) {
        if self.older_files.len() >= COMPACT_THRESHOLD {
            // todo: trigger by size of file contents
            let mut path = self.dir_path.clone();
            path.push("compacting.kvs");
            let mut compacted = File::options()
                .append(true)
                .create_new(true)
                .open(path.clone())
                .expect("failed to create compacting file");
            // todo: choose files to compact
            while let Some((id, file)) = self.older_files.iter_mut().take(4).next() {
                let compact_log = |offset, cmd: Command| {
                    let mut keep = false;
                    let opt_idx = self.table.get(&cmd.key);
                    if cmd.is_del {
                        if opt_idx.is_none() {
                            // maybe a put command exists in previous log
                            keep = true;
                        }
                    } else {
                        if let Some(idx) = opt_idx {
                            if idx.file == *id && idx.offset == offset {
                                keep = true;
                            }
                        }
                    }
                    if keep {
                        append_cmd(&mut compacted, cmd).expect("failed to write");
                    }
                };
                iter_cmds(file, compact_log);
            }
            let mut keys = Vec::new();
            while let Some(&id) = self.older_files.keys().take(4).next() {
                keys.push(id);
            }
            for id in keys {
                self.older_files.remove(&id);
                remove_file(format!("{}.kvs", id)).unwrap();
            }
            let mut to_path = self.dir_path.clone();
            to_path.push("1.kvs");
            rename(path, to_path).expect("compaction failed");
        }
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
            if idx.file == self.active_id {
                file = &mut self.active_file
            } else {
                file = self.older_files.get_mut(&idx.file).unwrap();
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
        append_cmd(&mut self.active_file, Command::del(key.clone()))?;
        self.maybe_cut();
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
        let mut older = BTreeMap::new();
        let mut file_id: u32 = 0;
        for e in dir.iter() {
            file_id = e
                .file_stem()
                .expect("invalid file")
                .to_str()
                .unwrap()
                .parse()?;
            let mut file = File::open(e)?;
            let handle_log = |offset, cmd: Command| {
                if !cmd.is_del {
                    table.insert(cmd.key, Index::new(file_id, offset));
                } else {
                    table.remove(&cmd.key);
                }
            };
            iter_cmds(&mut file, handle_log);
            older.insert(file_id, file);
        }

        let store = KvStore {
            table: table,
            older_files: older,
            active_file: new_active_file(dir_path.clone())?,
            active_id: file_id + 1,
            dir_path: dir_path,
        };
        Ok(store)
    }

    fn rename_active(&self) -> Result<PathBuf> {
        let mut from = self.dir_path.clone();
        from.push("active.kvs");
        let mut to = self.dir_path.clone();
        to.push(format!("{}.kvs", self.active_id));
        rename(from, to.clone())?;
        Ok(to)
    }
}

fn new_active_file(mut path: PathBuf) -> Result<File> {
    path.push("active.kvs");
    let file = File::options()
        .read(true)
        .append(true)
        .create_new(true)
        .open(path)?;
    Ok(file)
}

impl Drop for KvStore {
    fn drop(&mut self) {
        self.rename_active()
            .expect("failed to rename active file when drop KvStore");
    }
}

fn iter_cmds<F: FnMut(u32, Command)>(file: &mut File, mut f: F) {
    let file_len = file.metadata().unwrap().len() as u32;
    let mut offset: u32 = 0;
    while offset < file_len {
        let cmd = read_cmd(file).unwrap();
        f(offset, cmd);
        offset = file.stream_position().unwrap() as u32;
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

fn append_cmd(file: &mut File, cmd: Command) -> Result<()> {
    let cmd = serde_json::to_vec(&cmd)?;
    let cmd_len = cmd.len() as u32;
    file.write_all(&cmd_len.to_be_bytes())?;
    file.write_all(&cmd)?;
    Ok(())
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
