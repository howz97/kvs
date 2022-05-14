use crate::{KvsEngine, MyErr, Result};
use crossbeam::{channel, select};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json;
use std::clone::Clone;
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{read_dir, remove_file, rename, File};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::windows::fs::FileExt;
use std::path::PathBuf;
use std::result;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tracing::{debug, info, trace};

const SEGMENT_SIZE: u64 = 1 * 1024;
const COMPACT_THRESHOLD: u64 = 2 * SEGMENT_SIZE;
const COMPACT_CHECK: u64 = 1;

#[derive(Debug)]
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

pub struct KvStore {
    dir_path: PathBuf,
    reader: Arc<RwLock<BTreeMap<u32, File>>>,
    writer: Arc<Mutex<Writer>>,
    table: Arc<DashMap<String, Index>>,
    compactor: Option<Compactor>,
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        KvStore {
            dir_path: self.dir_path.clone(),
            reader: self.reader.clone(),
            writer: self.writer.clone(),
            table: self.table.clone(),
            compactor: None,
        }
    }
}

struct Writer {
    file_id: u32,
    file: File,
    uncompacted: u64,
}

struct Compactor {
    handle: JoinHandle<()>,
    sender: channel::Sender<()>,
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
        let mut uncompacted: u64 = 0;
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
                    let idx = Index::new(file_id, bytes.len() as u32, offset);
                    if let Some(old) = table.insert(cmd.key, idx) {
                        uncompacted += 4 + old.len as u64;
                    }
                } else {
                    if let Some((_, old)) = table.remove(&cmd.key) {
                        uncompacted += 4 + old.len as u64;
                    }
                    uncompacted += 4 + bytes.len() as u64;
                }
            };
            iter_entries(&mut file, load_entry);
            handles.insert(file_id, file);
        }
        file_id += 1;
        let w = Writer {
            file_id,
            file: new_active_file(&dir_path, file_id)?,
            uncompacted,
        };
        handles.insert(file_id, w.file.try_clone()?);

        let mut store = KvStore {
            dir_path: dir_path,
            table: Arc::new(table),
            reader: Arc::new(RwLock::new(handles)),
            writer: Arc::new(Mutex::new(w)),
            compactor: None,
        };
        let (sdr, rcv) = channel::bounded(0);
        // start compactor in backend
        let cp = store.clone();
        let h = thread::spawn(move || loop {
            select! {
                recv(rcv) -> _ => break,
                default(Duration::from_secs(COMPACT_CHECK)) => {
                    debug!("checking compaction");
                },
            }
            if cp.writer.lock().unwrap().uncompacted < COMPACT_THRESHOLD {
                continue;
            }
            cp.compact()
        });
        store.compactor = Some(Compactor {
            handle: h,
            sender: sdr,
        });
        Ok(store)
    }
    fn cut(&self, mut writer: MutexGuard<Writer>) {
        writer.file_id += 1;
        writer.file = new_active_file(&self.dir_path, writer.file_id).unwrap();
        let mut reader = self.reader.write().unwrap();
        reader.insert(writer.file_id, writer.file.try_clone().unwrap());
    }
    fn compact(&self) {
        let path = path_push(&self.dir_path, "compacting");
        let mut compact_dst = File::options()
            .append(true)
            .create_new(true)
            .open(&path)
            .expect(format!("failed to create compacting file: {:?}", path).as_str());
        let compact_src = {
            // todo: optimize strategy to choose compacting files
            let hanldes = self.reader.read().unwrap();
            let mut tk = hanldes.iter().take(2);
            let mut c: Vec<(u32, File)> = Vec::new();
            while let Some((&id, file)) = tk.next() {
                let file = file.try_clone().expect("failed to clone file");
                c.push((id, file));
            }
            c
        };
        // write to compacting destination
        let mut moved: Vec<(String, u32, u64, u64)> = Vec::new();
        for (id, file) in &compact_src {
            let compact_log = |offset, bytes: Vec<u8>| {
                let ent: Entry = serde_json::from_slice(&bytes).expect("Unmarshal failed");
                if ent.is_del {
                    if !self.table.contains_key(&ent.key) {
                        // maybe a put Entry exists in previous log
                        append_entry_2(&mut compact_dst, bytes).expect("failed to write");
                    }
                } else {
                    let mut keep = false;
                    if let Some(idx) = self.table.get_mut(&ent.key) {
                        if idx.file == *id && idx.offset == offset {
                            // release reference, then write to disk
                            keep = true;
                        }
                    }
                    if keep {
                        let new_pos =
                            append_entry_2(&mut compact_dst, bytes).expect("failed to write");
                        moved.push((ent.key, *id, offset, new_pos));
                    }
                }
            };
            debug!("start to compact file {}.kvs", id);
            iter_entries_2(file, compact_log);
        }

        // clean source files
        let mut handles = self.reader.write().unwrap();
        for (id, _) in compact_src {
            handles.remove(&id);
            let path_src = kvs_path(&self.dir_path, id);
            remove_file(path_src).unwrap();
            debug!("file {}.kvs removed", id);
        }
        // adopt compacted file
        drop(compact_dst); // reopen in Read-Only mode
        let path_dst = kvs_path(&self.dir_path, 1);
        rename(path, &path_dst).expect("compaction failed");
        let compacted = File::open(path_dst).unwrap();
        handles.insert(1, compacted);
        for (key, old_f, old_pos, pos) in moved {
            if let Some(mut idx) = self.table.get_mut(&key) {
                // make sure index is unmodified
                if idx.file == old_f && idx.offset == old_pos {
                    idx.file = 1;
                    idx.offset = pos;
                }
            }
        }
        info!("finish compaction");
    }

    // fn rename_active(&self) -> Result<PathBuf> {
    //     let mut from = self.dir_path.clone();
    //     from.push("active.kvs");
    //     let mut to = self.dir_path.clone();
    //     to.push(format!("{}.kvs", self.active_id));
    //     rename(from, to.clone())?;
    //     Ok(to)
    // }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        if let Some(c) = self.compactor.take() {
            c.sender
                .send(())
                .expect("failed to notify compactor to exit");
            c.handle.join().expect("failed to kill compactor");
            info!("KvStore closed gracefully!");
        }
    }
}

fn kvs_path(dir: &PathBuf, id: u32) -> PathBuf {
    path_push(dir, format!("{:09}.kvs", id).as_str())
}

fn new_active_file(dir: &PathBuf, id: u32) -> Result<File> {
    let path = kvs_path(dir, id);
    trace!("creating active file {:?}", path);
    let file = File::options()
        .read(true)
        .append(true)
        .create_new(true)
        .open(path)?;
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
        if let Some(old) = self.table.insert(key, idx) {
            w.uncompacted += 4 + old.len as u64;
        }
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
        let (_, old) = self.table.remove(&key).ok_or(MyErr::KeyNotFound)?;
        let (len, offset) = append_entry(&mut w.file, Entry::del(key))?;
        w.uncompacted += 4 + old.len as u64;
        w.uncompacted += 4 + len as u64;
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
        let handles = self.reader.read().unwrap();
        if let Some(idx) = self.table.get(&key) {
            let file = handles.get(&idx.file).unwrap();
            let mut ent = vec![0; idx.len as usize];
            pread_exact(file, &mut ent, idx.offset)?;
            let ent: Entry = serde_json::from_slice(&ent)?;
            Ok(Some(ent.val))
        } else {
            return Ok(None);
        }
    }
}

fn iter_entries_2<F: FnMut(u64, Vec<u8>)>(file: &File, mut f: F) {
    let file_len = file.metadata().unwrap().len();
    let mut offset = 0;
    while offset < file_len {
        let mut length = [0; 4];
        pread_exact(file, &mut length, offset).unwrap();
        offset += 4;
        let length = u32::from_be_bytes(length);
        let mut ent = vec![0; length as usize];
        pread_exact(file, &mut ent, offset).unwrap();
        f(offset, ent);
        offset += length as u64;
    }
}

fn pread_exact(file: &File, mut buf: &mut [u8], mut offset: u64) -> Result<()> {
    while buf.len() > 0 {
        let r = file.seek_read(buf, offset)?;
        buf = &mut buf[r..];
        offset += r as u64;
    }
    Ok(())
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

fn append_entry_2(file: &mut File, ent: Vec<u8>) -> Result<u64> {
    let len = ent.len() as u32;
    file.write_all(&len.to_be_bytes())?;
    let offset = file.metadata().unwrap().len();
    file.write_all(&ent)?;
    Ok(offset)
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
