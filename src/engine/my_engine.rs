use crate::thread_pool::ThreadPool;
use crate::{KvsEngine, MyErr, Result};
use async_trait::async_trait;
use crossbeam::{channel, select};
use dashmap::DashMap;
use failure;
use serde::{Deserialize, Serialize};
use serde_json;
use std::clone::Clone;
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{read_dir, remove_file, rename, File};
use std::future::Future;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::windows::fs::FileExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::task::{Context, Poll};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, trace};

const SEGMENT_SIZE: u64 = 1 * 1024;
const COMPACT_THRESHOLD: u64 = 2 * SEGMENT_SIZE;
const COMPACT_CHECK: u64 = 1;

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

pub struct KvStore<ThreadPool> {
    dir_path: PathBuf,
    reader: Reader,
    writer: Arc<Mutex<Writer>>,
    compactor: Option<CompactorHandle>,
    tp: Arc<Mutex<ThreadPool>>,
}

impl<P: ThreadPool> Clone for KvStore<P> {
    fn clone(&self) -> Self {
        KvStore {
            dir_path: self.dir_path.clone(),
            reader: self.reader.clone(),
            writer: self.writer.clone(),
            compactor: None,
            tp: self.tp.clone(),
        }
    }
}

struct Writer {
    dir: PathBuf,
    file_id: u32,
    file: File,
    indices: Arc<DashMap<String, Index>>,
    reader: Arc<RwLock<BTreeMap<u32, File>>>,
    uncompacted: Arc<AtomicU64>,
}

impl Writer {
    fn set(&mut self, key: String, val: String) -> Result<()> {
        let (len, offset) = append_entry(&mut self.file, Entry::put(key.clone(), val))?;
        let idx = Index::new(self.file_id, len, offset);
        if let Some(old) = self.indices.insert(key, idx) {
            self.uncompacted
                .fetch_add(4 + old.len as u64, Ordering::Relaxed);
        }
        if offset + len as u64 >= SEGMENT_SIZE {
            if let Err(err) = self.cut() {
                error!("failed to cut {}", err);
            }
        }
        Ok(())
    }
    fn remove(&mut self, key: String) -> Result<()> {
        let (_, old) = self.indices.remove(&key).ok_or(MyErr::KeyNotFound)?;
        let (len, offset) = append_entry(&mut self.file, Entry::del(key))?;
        let uncmpct = 4 + old.len as u64 + 4 + len as u64;
        self.uncompacted.fetch_add(uncmpct, Ordering::Relaxed);
        if offset + len as u64 >= SEGMENT_SIZE {
            if let Err(err) = self.cut() {
                error!("failed to cut {}", err);
            }
        }
        Ok(())
    }
    fn cut(&mut self) -> Result<()> {
        self.file_id += 1;
        self.file = new_active_file(&self.dir, self.file_id)?;
        let mut reader = self.reader.write().unwrap();
        reader.insert(self.file_id, self.file.try_clone()?);
        Ok(())
    }
}

// Lock order: handles -> indices (if both of them needed)
#[derive(Clone)]
struct Reader {
    handles: Arc<RwLock<BTreeMap<u32, File>>>, // todo: lock-free
    indices: Arc<DashMap<String, Index>>,
}

impl Reader {
    fn get(&self, key: String) -> Result<Option<String>> {
        let (file, len, offset) = {
            let handles = self.handles.read().unwrap();
            if let Some(idx) = self.indices.get(&key) {
                (
                    handles.get(&idx.file).unwrap().try_clone().unwrap(),
                    idx.len,
                    idx.offset,
                )
            } else {
                return Ok(None);
            }
        };
        // Read disk witout lock, trade consistency for performance
        let mut bytes = vec![0; len as usize];
        pread_exact(&file, &mut bytes, offset)?;
        let ent: Entry = serde_json::from_slice(&bytes)?;
        Ok(Some(ent.val))
    }
}

struct CompactorHandle {
    handle: JoinHandle<()>,
    sender: channel::Sender<()>,
}

impl<P: ThreadPool> KvStore<P> {
    pub fn open(path: impl Into<PathBuf>, tp: P) -> Result<Self> {
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
        let active = new_active_file(&dir_path, file_id)?;
        handles.insert(file_id, active.try_clone()?);
        let handles = Arc::new(RwLock::new(handles));
        let indices = Arc::new(table);
        let uncompacted = Arc::new(AtomicU64::new(uncompacted));
        let mut store = KvStore {
            dir_path: dir_path.clone(),
            reader: Reader {
                handles: handles.clone(),
                indices: indices.clone(),
            },
            writer: Arc::new(Mutex::new(Writer {
                dir: dir_path.clone(),
                file_id,
                file: active,
                indices: indices.clone(),
                reader: handles.clone(),
                uncompacted: uncompacted.clone(),
            })),
            compactor: None,
            tp: Arc::new(Mutex::new(tp)),
        };
        let compactor = Compactor {
            dir_path: dir_path,
            reader: handles,
            indices: indices,
            uncompacted: uncompacted,
        };
        store.compactor = Some(compactor.run());
        Ok(store)
    }
}

struct Compactor {
    dir_path: PathBuf,
    reader: Arc<RwLock<BTreeMap<u32, File>>>, // todo: lock-free
    // Lock order: reader -> indices (if both of them needed)
    indices: Arc<DashMap<String, Index>>,
    uncompacted: Arc<AtomicU64>,
}

impl Compactor {
    fn run(self) -> CompactorHandle {
        let (sdr, rcv) = channel::bounded(0);
        // run compactor in backend
        let h = thread::spawn(move || loop {
            select! {
                recv(rcv) -> _ => break,
                default(Duration::from_secs(COMPACT_CHECK)) => {
                    debug!("checking compaction");
                },
            }
            if self.uncompacted.load(Ordering::Acquire) < COMPACT_THRESHOLD {
                continue;
            }
            self.compact()
        });
        CompactorHandle {
            handle: h,
            sender: sdr,
        }
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
                c.push((id, file.try_clone().unwrap()));
            }
            c
        };
        // write to compacting destination
        let mut moved: Vec<(String, u32, u64, u64)> = Vec::new();
        for (id, file) in &compact_src {
            let compact_log = |offset, bytes: Vec<u8>| {
                let ent: Entry = serde_json::from_slice(&bytes).expect("Unmarshal failed");
                if ent.is_del {
                    if !self.indices.contains_key(&ent.key) {
                        // maybe a put Entry exists in previous log
                        append_entry_2(&mut compact_dst, bytes).expect("failed to write");
                    }
                } else {
                    if let Some(idx) = self.load_index(&ent.key) {
                        if idx.file == *id && idx.offset == offset {
                            let pos =
                                append_entry_2(&mut compact_dst, bytes).expect("failed to write");
                            moved.push((ent.key, *id, offset, pos));
                        }
                    }
                }
            };
            debug!("start to compact file {}.kvs", id);
            iter_entries_2(file, compact_log);
        }
        // clean source files
        let mut off: u64 = 0;
        let mut handles = self.reader.write().unwrap();
        for (id, src) in compact_src {
            off += src.metadata().unwrap().len();
            handles.remove(&id).expect("file handle not exist");
            remove_file(kvs_path(&self.dir_path, id)).unwrap();
            debug!("file {}.kvs removed", id);
        }
        // adopt compacted file
        drop(compact_dst); // reopen in Read-Only mode
        let path_dst = kvs_path(&self.dir_path, 1);
        rename(path, &path_dst).expect("compaction failed");
        let compacted = File::open(path_dst).unwrap();
        off -= compacted.metadata().unwrap().len();
        handles.insert(1, compacted);
        for (key, old_f, old_pos, pos) in moved {
            if let Some(mut idx) = self.indices.get_mut(&key) {
                // make sure index is unmodified
                if idx.file == old_f && idx.offset == old_pos {
                    idx.file = 1;
                    idx.offset = pos;
                }
            }
        }
        self.uncompacted.fetch_sub(off, Ordering::Relaxed);
        info!("compaction finished, {} bytes disk freed", off);
    }
    fn load_index(&self, key: &String) -> Option<Index> {
        self.indices.get(key).map(|idx| idx.clone())
    }
}

impl<ThreadPool> Drop for KvStore<ThreadPool> {
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

#[async_trait]
impl<P: ThreadPool> KvsEngine for KvStore<P> {
    /// Insert/Update key-value
    async fn set(self, key: String, val: String) -> Result<()> {
        let (sdr, rcv) = oneshot::channel();
        let w = self.writer.clone();
        self.tp.lock().unwrap().spawn(move || {
            let rlt = w.lock().unwrap().set(key, val);
            sdr.send(rlt).expect("oneshot send failed");
        });
        rcv.await?
    }
    /// Remove value by key
    async fn remove(self, key: String) -> Result<()> {
        let (sdr, rcv) = oneshot::channel();
        let w = self.writer.clone();
        self.tp.lock().unwrap().spawn(move || {
            let rlt = w.lock().unwrap().remove(key);
            sdr.send(rlt).expect("oneshot send failed");
        });
        rcv.await?
    }
    /// Get value by key, sequential consistency is guaranteed
    async fn get(self, key: String) -> Result<Option<String>> {
        let (sdr, rcv) = oneshot::channel();
        let r = self.reader.clone();
        self.tp.lock().unwrap().spawn(move || {
            let rlt = r.get(key);
            sdr.send(rlt).expect("oneshot send failed");
        });
        rcv.await?
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
