use clap::{Arg, Command};
use crossbeam::channel;
use kvs::server::run;
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, MyErr, Result, SledKvsEngine};
use num_cpus;
use std::fs::read_dir;
use tokio;
use tracing::{debug, error};
use tracing_subscriber;

const DEFAULT_DIR: &str = ".";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let m = Command::new("kvs-server")
        .author(env!("CARGO_PKG_AUTHORS"))
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::new("addr")
                .long("addr")
                .default_value("127.0.0.1:4000"),
        )
        .arg(
            Arg::new("engine")
                .long("engine")
                .possible_values(["kvs", "sled"]),
        )
        .arg(
            Arg::new("pool")
                .long("thread-pool")
                .possible_values(["naive", "better"])
                .default_value("better"),
        )
        .after_help("--Over--")
        .get_matches();
    let addr = m.value_of("addr").unwrap();
    let mut eng = "kvs".to_owned();
    if let Some(e) = m.value_of("engine") {
        eng = e.to_owned();
        if let Some(last) = last_engine()? {
            if eng != last {
                error!("failed to start, because wrong engine is specified");
                Err(MyErr::WrongEngine)?
            }
        }
    } else if let Some(last) = last_engine()? {
        eng = last;
    }
    let pool = SharedQueueThreadPool::new(num_cpus::get() as u32)?;
    eprintln!(
        "kvs-server[v{}] starting...addr={}, engine={}",
        env!("CARGO_PKG_VERSION"),
        addr,
        eng
    );
    if eng == "kvs" {
        run(addr, KvStore::open(DEFAULT_DIR, pool)?).await
    } else if eng == "sled" {
        run(addr, SledKvsEngine::open(DEFAULT_DIR)?).await
    } else {
        panic!("never execute")
    }
}

fn last_engine() -> Result<Option<String>> {
    for entry in read_dir(DEFAULT_DIR)? {
        let entry = entry?;
        if let Some(ext) = entry.path().extension() {
            if ext == "kvs" {
                debug!("last_engine: kvs");
                return Ok(Some("kvs".to_owned()));
            }
        } else if entry.path().ends_with("db") {
            debug!("last_engine: sled");
            return Ok(Some("sled".to_owned()));
        }
        debug!("last_engine: file found {:?}", entry.path());
    }
    debug!("last_engine: none");
    Ok(None)
}
