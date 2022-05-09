use clap::{Arg, Command};
use kvs::my_engine;
use kvs::protocol;
use kvs::sled_engine::SledKvEngine;
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::{KvsEngine, MyErr, Result};
use num_cpus;
use std::fs::read_dir;
use std::io::{self, BufRead, Read, Write};
use std::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};
use tracing_subscriber;

const DEFAULT_DIR: &str = ".";
static X: &[char] = &['\n', '\t', ' '];

fn main() -> Result<()> {
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
        run(addr, my_engine::KvStore::open(DEFAULT_DIR)?, pool)
    } else if eng == "sled" {
        run(addr, SledKvEngine::open(DEFAULT_DIR)?, pool)
    } else {
        panic!("never execute")
    }
}

fn run<E: KvsEngine, P: ThreadPool>(addr: &str, engine: E, pool: P) -> Result<()> {
    info!("kvs-server is running...");
    let listener = TcpListener::bind(addr)?;
    // accept connections and process them serially
    for stream in listener.incoming() {
        let stream = stream?;
        let eng = engine.clone();
        debug!("start serving client");
        pool.spawn(move || {
            if let Err(e) = handle_client(stream, eng) {
                error!("error on serving client: {}", e)
            }
        });
    }
    info!("kvs-server shutdown!");
    Ok(())
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

fn handle_client<E: KvsEngine>(stream: TcpStream, eng: E) -> Result<()> {
    let mut reader = io::BufReader::new(stream.try_clone().unwrap());
    let mut writer = io::BufWriter::new(stream);
    let mut op = [0; 1];
    loop {
        reader.read_exact(&mut op)?;
        match *op.get(0).unwrap() {
            protocol::OP_SET => {
                let mut key = String::new();
                reader.read_line(&mut key)?;
                key = key.trim_matches(X).to_owned();
                if key.len() == 0 {
                    writer.write_all("ErrNoKey\n".as_bytes())?;
                    break;
                }
                let mut val = String::new();
                reader.read_line(&mut val)?;
                val = val.trim_matches(X).to_owned();
                if val.len() == 0 {
                    writer.write_all("ErrNoVal\n".as_bytes())?;
                    break;
                }
                if let Err(_) = eng.set(key, val) {
                    writer.write_all("ErrInternal\n".as_bytes())?;
                } else {
                    writer.write_all("OK\n".as_bytes())?;
                }
            }
            protocol::OP_RM => {
                let mut key = String::new();
                reader.read_line(&mut key)?;
                key = key.trim_matches(X).to_owned();
                if key.len() == 0 {
                    writer.write_all("ErrNoKey\n".as_bytes())?;
                    break;
                }
                debug!("Removing {}", key);
                if let Err(e) = eng.remove(key) {
                    writer.write_all(e.to_string().as_bytes())?;
                    writer.write_all(&['\n' as u8])?;
                } else {
                    writer.write_all("OK\n".as_bytes())?;
                }
            }
            protocol::OP_GET => {
                let mut key = String::new();
                reader.read_line(&mut key)?;
                key = key.trim_matches(X).to_owned();
                if key.len() == 0 {
                    writer.write_all(&[protocol::GET_ERR])?;
                    writer.write_all("ErrNoKey\n".as_bytes())?;
                    break;
                }
                let res = eng.get(key);
                if let Err(_) = res {
                    writer.write_all(&[protocol::GET_ERR])?;
                    writer.write_all("ErrInternal\n".as_bytes())?;
                } else {
                    if let Some(v) = res.unwrap() {
                        writer.write_all(&[protocol::GET_VAL])?;
                        writer.write_all(v.as_bytes())?;
                    } else {
                        writer.write_all(&[protocol::GET_NIL])?;
                    }
                    writer.write_all(&['\n' as u8])?;
                }
            }
            _ => {
                writer.write_all("ErrOp\n".as_bytes())?;
            }
        }
        break; // todo
    }
    Ok(())
}
