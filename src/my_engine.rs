use crate::{KvsEngine, MyErr, Result};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::{BTreeMap, HashMap};

use std::clone::Clone;
use std::fs::{read_dir, remove_file, rename, File};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::result;
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

const SEGMENT_SIZE: u64 = 10 << 20;
const COMPACT_THRESHOLD: usize = 100;

struct InnerStore {
    table: HashMap<String, Index>,
    older_files: BTreeMap<u32, File>,
    active_file: File,
    active_id: u32,
    dir_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct Index {
    file: u32, // file_id
    offset: u64,
}

impl Index {
    fn new(file: u32, offset: u64) -> Self {
        Index { file, offset }
    }
}

impl InnerStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let dir_path = path.into();
        debug!("opening path {:?}", dir_path);
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
        let mut table = HashMap::new();
        let mut older = BTreeMap::new();
        let mut file_id: u32 = 0;
        for e in dir.iter() {
            let mut file;
            if e.ends_with("active.kvs") {
                file_id += 1;
                let mut from = dir_path.clone();
                from.push("active.kvs");
                let mut to = dir_path.clone();
                to.push(format!("{}.kvs", file_id));
                rename(from, to.clone())?;
                file = File::open(to)?;
            } else {
                file_id = e
                    .file_stem()
                    .expect("invalid file")
                    .to_str()
                    .unwrap()
                    .parse()?;
                file = File::open(e)?;
            }
            let handle_log = |offset, cmd: Entry| {
                if !cmd.is_del {
                    table.insert(cmd.key, Index::new(file_id, offset));
                } else {
                    table.remove(&cmd.key);
                }
            };
            iter_entries(&mut file, handle_log);
            older.insert(file_id, file);
        }
        debug!("succeed to open dir");
        let store = InnerStore {
            table: table,
            older_files: older,
            active_file: new_active_file(dir_path.clone())?,
            active_id: file_id + 1,
            dir_path: dir_path,
        };
        Ok(store)
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
            info!("start compaction");
            // todo: trigger by size of file contents
            let mut path = self.dir_path.clone();
            path.push("compacting.kvs");
            let mut compacted = File::options()
                .append(true)
                .create_new(true)
                .open(&path)
                .expect("failed to create compacting file");
            // todo: choose files to compact
            let mut choosed = self.older_files.iter_mut().take(4);
            while let Some((id, file)) = choosed.next() {
                let compact_log = |offset, ent: Entry| {
                    let opt_idx = self.table.get_mut(&ent.key);
                    if ent.is_del {
                        if opt_idx.is_none() {
                            // maybe a put Entry exists in previous log
                            append_entry(&mut compacted, ent).expect("failed to write");
                        }
                    } else {
                        if let Some(idx) = opt_idx {
                            if idx.file == *id && idx.offset == offset {
                                idx.file = 1;
                                idx.offset = compacted.stream_position().unwrap();
                                append_entry(&mut compacted, ent).expect("failed to write");
                            }
                        }
                    }
                };
                debug!("start to compact file {}.kvs", id);
                iter_entries(file, compact_log);
            }
            // remove old files
            let mut removing = self.older_files.keys().take(4);
            let mut keys = Vec::new();
            while let Some(&id) = removing.next() {
                keys.push(id);
            }
            for id in keys {
                self.older_files.remove(&id);
                let mut rm_path = self.dir_path.clone();
                rm_path.push(format!("{}.kvs", id));
                remove_file(rm_path).unwrap();
                debug!("file {}.kvs removed", id);
            }
            // adopt compacted file
            drop(compacted); // reopen in Read-Only later
            let mut to_path = self.dir_path.clone();
            to_path.push("1.kvs");
            rename(path, &to_path).expect("compaction failed");
            let compacted = File::open(to_path).unwrap();
            self.older_files.insert(1, compacted);
            info!("finish compaction");
        }
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

#[derive(Clone)]
pub struct KvStore {
    db: Arc<Mutex<InnerStore>>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let inner = InnerStore::open(path)?;
        Ok(KvStore {
            db: Arc::new(Mutex::new(inner)),
        })
    }
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
        let mut store = self.db.lock().unwrap();
        let offset = store.active_file.stream_position()?;
        append_entry(&mut store.active_file, Entry::put(key.clone(), val))?;
        let idx = Index::new(store.active_id, offset);
        store.table.insert(key, idx);
        store.maybe_cut();
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
        let mut store = self.db.lock().unwrap();
        if let Some(idx) = store.table.get(&key) {
            let idx = idx.clone();
            let mut file;
            if idx.file == store.active_id {
                file = &mut store.active_file
            } else {
                file = store.older_files.get_mut(&idx.file).unwrap();
            }
            file.seek(SeekFrom::Start(idx.offset))?;
            let ent = read_entry(&mut file)?;
            Ok(Some(ent.val))
        } else {
            return Ok(None);
        }
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
        let mut store = self.db.lock().unwrap();
        store.table.remove(&key).ok_or(MyErr::KeyNotFound)?;
        append_entry(&mut store.active_file, Entry::del(key.clone()))?;
        store.maybe_cut();
        Ok(())
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

impl Drop for InnerStore {
    fn drop(&mut self) {
        self.rename_active()
            .expect("failed to rename active file when drop KvStore");
    }
}

fn iter_entries<F: FnMut(u64, Entry)>(file: &mut File, mut f: F) {
    let file_len = file.metadata().unwrap().len();
    let mut offset: u64 = file.seek(SeekFrom::Start(0)).unwrap();
    while offset < file_len {
        let ent = read_entry(file).expect("failed to read entry");
        f(offset, ent);
        offset = file.stream_position().unwrap();
    }
}

fn read_entry(file: &mut File) -> Result<Entry> {
    let mut length = [0; 4];
    file.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length);
    let mut ent = vec![0; length as usize];
    file.read_exact(&mut ent)?;
    let ent: Entry = serde_json::from_slice(&ent)?;
    Ok(ent)
}

fn append_entry(file: &mut File, ent: Entry) -> Result<()> {
    let ent = serde_json::to_vec(&ent)?;
    let length = ent.len() as u32;
    file.write_all(&length.to_be_bytes())?;
    file.write_all(&ent)?;
    Ok(())
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
