use crate::{KvsEngine, MyErr, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json;
use std::clone::Clone;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::{read_dir, remove_file, rename, File};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::windows::fs::FileExt;
use std::path::PathBuf;
use std::result;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use tracing::{debug, error, info};

const SEGMENT_SIZE: u64 = 10 << 20;
const COMPACT_THRESHOLD: usize = 100;

#[derive(Debug, Clone)]
pub struct Index {
    file: u32,
    len: u32,
    offset: u64,
}

impl Index {
    fn new(file: u32, len: u32, offset: u64) -> Self {
        Index { file, len, offset }
    }
}

impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(file={}, len={}, off={})",
            self.file, self.len, self.offset
        )
    }
}

#[derive(Clone)]
pub struct KvStore {
    dir_path: PathBuf,
    reader: Arc<RwLock<BTreeMap<u32, File>>>,
    writer: Arc<Mutex<Writer>>,
    table: Arc<DashMap<String, Index>>,
}

struct Writer {
    file_id: u32,
    file: File,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let dir_path = path.into();
        let mut dir = read_dir(&dir_path)?
            .map(|res| res.map(|e| e.path()))
            .filter(|res| {
                if let Ok(p) = res {
                    if let Some(ext) = p.extension() {
                        if ext == "kvs" {
                            return true;
                        }
                    }
                }
                false
            })
            .collect::<result::Result<Vec<_>, io::Error>>()?;
        debug!("total {} kvs files found", dir.len());
        dir.sort();
        let table = DashMap::new();
        let mut handles = BTreeMap::new();
        let mut file_id: u32 = 0;
        for e in dir.iter() {
            file_id = e
                .file_stem()
                .expect("invalid file")
                .to_str()
                .unwrap()
                .parse()?;
            let mut file = File::open(e)?;
            let load_entry = |offset, bytes: Vec<u8>| {
                let cmd: Entry = serde_json::from_slice(&bytes).expect("Unmarshal failed");
                if !cmd.is_del {
                    table.insert(cmd.key, Index::new(file_id, bytes.len() as u32, offset));
                } else {
                    table.remove(&cmd.key);
                }
            };
            iter_entries(&mut file, load_entry);
            handles.insert(file_id, file);
        }
        file_id += 1;
        let w = Writer {
            file_id,
            file: new_active_file(&dir_path, file_id)?,
        };
        handles.insert(file_id, w.file.try_clone()?);
        let store = KvStore {
            dir_path: dir_path,
            table: Arc::new(table),
            reader: Arc::new(RwLock::new(handles)),
            writer: Arc::new(Mutex::new(w)),
        };
        Ok(store)
    }
    fn cut(&self, mut writer: MutexGuard<Writer>) {
        writer.file_id += 1;
        writer.file = new_active_file(&self.dir_path, writer.file_id).unwrap();
        let mut reader = self.reader.write().unwrap();
        reader.insert(writer.file_id, writer.file.try_clone().unwrap());
    }
    // fn compaction(&mut self) {
    //     let mut path = self.dir_path.clone();
    //     path.push("compacting.kvs");
    //     let mut compacted = File::options()
    //         .append(true)
    //         .create_new(true)
    //         .open(&path)
    //         .expect("failed to create compacting file");
    //     // todo: choose files to compact
    //     let mut choosed = self.older_files.iter_mut().take(4);
    //     while let Some((id, file)) = choosed.next() {
    //         let compact_log = |offset, ent: Entry| {
    //             let opt_idx = self.table.get_mut(&ent.key);
    //             if ent.is_del {
    //                 if opt_idx.is_none() {
    //                     // maybe a put Entry exists in previous log
    //                     append_entry(&mut compacted, ent).expect("failed to write");
    //                 }
    //             } else {
    //                 if let Some(idx) = opt_idx {
    //                     if idx.file == *id && idx.offset == offset {
    //                         idx.file = 1;
    //                         idx.offset = compacted.stream_position().unwrap();
    //                         append_entry(&mut compacted, ent).expect("failed to write");
    //                     }
    //                 }
    //             }
    //         };
    //         debug!("start to compact file {}.kvs", id);
    //         iter_entries(file, compact_log);
    //     }
    //     // remove old files
    //     let mut removing = self.older_files.keys().take(4);
    //     let mut keys = Vec::new();
    //     while let Some(&id) = removing.next() {
    //         keys.push(id);
    //     }
    //     for id in keys {
    //         self.older_files.remove(&id);
    //         let mut rm_path = self.dir_path.clone();
    //         rm_path.push(format!("{}.kvs", id));
    //         remove_file(rm_path).unwrap();
    //         debug!("file {}.kvs removed", id);
    //     }
    //     // adopt compacted file
    //     drop(compacted); // reopen in Read-Only later
    //     let mut to_path = self.dir_path.clone();
    //     to_path.push("1.kvs");
    //     rename(path, &to_path).expect("compaction failed");
    //     let compacted = File::open(to_path).unwrap();
    //     self.older_files.insert(1, compacted);
    //     info!("finish compaction");
    // }

    // fn rename_active(&self) -> Result<PathBuf> {
    //     let mut from = self.dir_path.clone();
    //     from.push("active.kvs");
    //     let mut to = self.dir_path.clone();
    //     to.push(format!("{}.kvs", self.active_id));
    //     rename(from, to.clone())?;
    //     Ok(to)
    // }
}

fn new_active_file(dir: &PathBuf, id: u32) -> Result<File> {
    let file = File::options()
        .read(true)
        .append(true)
        .create_new(true)
        .open(path_push(dir, format!("{}.kvs", id).as_str()))?;
    Ok(file)
}

fn path_push(dir: &PathBuf, f: &str) -> PathBuf {
    let mut dir = dir.clone();
    dir.push(f);
    dir
}

impl KvsEngine for KvStore {
    /// Insert/Update key-value
    ///
    /// # Example
    ///
    /// ```rust
    /// # use kvs::{Result, KvsEngine};
    /// # use kvs::my_engine::KvStore;
    /// # use tempfile::TempDir;
    /// # fn main() -> Result<()> {
    /// #   let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    /// #   let mut store = KvStore::open(temp_dir.path())?;
    /// #   store.set("a".to_string(), "A".to_string())
    /// # }
    /// ```
    fn set(&self, key: String, val: String) -> Result<()> {
        let mut w = self.writer.lock().unwrap();
        let (len, offset) = append_entry(&mut w.file, Entry::put(key.clone(), val))?;
        let idx = Index::new(w.file_id, len, offset);
        self.table.insert(key, idx);
        if offset + len as u64 >= SEGMENT_SIZE {
            self.cut(w);
        }
        Ok(())
    }
    /// Remove value by key
    ///
    /// # Example
    ///
    /// ```rust
    /// # use kvs::{Result, KvsEngine};
    /// # use kvs::my_engine::KvStore;
    /// # use tempfile::TempDir;
    /// # fn main() -> Result<()> {
    /// #   let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    /// #   let mut store = KvStore::open(temp_dir.path())?;
    /// #   store.set("a".to_string(), "A".to_string())?;
    /// #   store.remove("a".to_string())
    /// # }
    /// ```
    fn remove(&self, key: String) -> Result<()> {
        let mut w = self.writer.lock().unwrap();
        self.table.remove(&key).ok_or(MyErr::KeyNotFound)?;
        let (len, offset) = append_entry(&mut w.file, Entry::del(key.clone()))?;
        if offset + len as u64 >= SEGMENT_SIZE {
            self.cut(w);
        }
        Ok(())
    }
    /// Get value by key
    ///
    /// # Example
    ///
    /// ```rust
    /// # use kvs::{Result, KvsEngine};
    /// # use kvs::my_engine::KvStore;
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
    fn get(&self, key: String) -> Result<Option<String>> {
        let r = self.reader.read().unwrap();
        if let Some(idx) = self.table.get(&key) {
            let file = r.get(&idx.file).unwrap();
            let mut ent = vec![0; idx.len as usize];
            let l = file.seek_read(&mut ent, idx.offset)?;
            if l != idx.len as usize {
                error!("engine:get, read {} bytes, expect {}", l, idx.len);
            }
            let ent: Entry = serde_json::from_slice(&ent)?;
            Ok(Some(ent.val))
        } else {
            info!("key {} not exist", key);
            return Ok(None);
        }
    }
}

fn iter_entries<F: FnMut(u64, Vec<u8>)>(file: &mut File, mut f: F) {
    let file_len = file.metadata().unwrap().len();
    let mut offset: u64 = file.seek(SeekFrom::Start(0)).unwrap();
    while offset < file_len {
        let ent = read_entry(file).expect("failed to read entry");
        f(offset + 4, ent);
        offset = file.stream_position().unwrap();
    }
}

fn read_entry(file: &mut File) -> Result<Vec<u8>> {
    let mut length = [0; 4];
    file.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length);
    let mut ent = vec![0; length as usize];
    file.read_exact(&mut ent)?;
    Ok(ent)
}

fn append_entry(file: &mut File, ent: Entry) -> Result<(u32, u64)> {
    let ent = serde_json::to_vec(&ent)?;
    let len = ent.len() as u32;
    file.write_all(&len.to_be_bytes())?;
    let offset = file.metadata().unwrap().len();
    file.write_all(&ent)?;
    Ok((len, offset))
}

#[derive(Serialize, Deserialize, Debug)]
struct Entry {
    key: String,
    val: String,
    is_del: bool,
}

impl Entry {
    fn put(key: String, val: String) -> Self {
        let is_del = false;
        Entry { key, val, is_del }
    }
    fn del(key: String) -> Self {
        let val = String::new();
        let is_del = true;
        Entry { key, val, is_del }
    }
}
