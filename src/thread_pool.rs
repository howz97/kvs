use crate::Result;
use crossbeam::channel;
use rayon;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::thread;
use tracing::{debug, error};

pub trait ThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(_: u32) -> Result<Self> {
        Ok(NaiveThreadPool {})
    }
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}

enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

pub struct SharedQueueThreadPool {
    handles: Vec<thread::JoinHandle<()>>,
    sender: channel::Sender<ThreadPoolMessage>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        debug!("creating thread pool, size={}", threads);
        let (sdr, rcv) = channel::bounded(4096);
        let rcv = Arc::new(Mutex::new(rcv));
        let mut handles: Vec<thread::JoinHandle<()>> = Vec::new();
        for id in 0..threads {
            let receiver = rcv.clone();
            let worker = move || loop {
                let res = receiver.lock().unwrap().recv();
                if let Err(e) = res {
                    debug!("thread {} received error: {}", id, e);
                    break;
                }
                match res.unwrap() {
                    ThreadPoolMessage::RunJob(f) => {
                        debug!("thread {} received job", id);
                        if let Err(e) = catch_unwind(AssertUnwindSafe(f)) {
                            error!("panic occur on thread {}: {:?}", id, e);
                        };
                        debug!("thread {} finished job", id);
                    }
                    ThreadPoolMessage::Shutdown => {
                        debug!("thread {} received shutdown", id);
                        break;
                    }
                }
            };
            handles.push(thread::spawn(worker));
        }
        Ok(SharedQueueThreadPool {
            handles,
            sender: sdr,
        })
    }
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .send(ThreadPoolMessage::RunJob(Box::new(job)))
            .expect("failed to dispatch job");
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in &self.handles {
            self.sender
                .send(ThreadPoolMessage::Shutdown)
                .expect("failed to terminate worker thread");
        }
        for handle in self.handles.drain(..) {
            handle.join().expect("failed to terminate thread");
        }
    }
}

pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()?;
        Ok(RayonThreadPool { pool })
    }
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.install(|| job());
    }
}
