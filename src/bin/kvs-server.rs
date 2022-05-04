use clap::{Arg, Command};
use kvs::my_engine;
use kvs::protocol;
use kvs::sled_engine::SledKvEngine;
use kvs::{KvsEngine, MyErr, Result};
use std::fs::read_dir;
use std::io::{self, BufRead, Read, Write};
use std::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};
use tracing_subscriber;

const DEFAULT_DIR: &str = ".\\testdata";
static X: &[char] = &['\n', '\t', ' '];

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let m = Command::new("kvs-server")
        .author(env!("CARGO_PKG_AUTHORS"))
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(Arg::new("addr").default_value("127.0.0.1:4000"))
        .arg(Arg::new("engine").possible_values(["kvs", "sled"]))
        .after_help("--Over--")
        .get_matches();
    let addr = m.value_of("addr").unwrap();
    let engine = m.value_of("engine");
    info!(addr, engine, "kvs-server start...");

    let mut store: Box<dyn KvsEngine>;
    let mut eng = "kvs".to_owned();
    if let Some(e) = m.value_of("engine") {
        eng = e.to_owned();
    } else {
        for entry in read_dir(DEFAULT_DIR)? {
            eng = entry?
                .path()
                .extension()
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();
            break;
        }
    }
    if eng == "kvs" {
        store = Box::new(my_engine::KvStore::open(DEFAULT_DIR)?);
    } else if eng == "sled" {
        store = Box::new(SledKvEngine::open(DEFAULT_DIR)?);
    } else {
        Err(MyErr::ErrExtension)?;
        panic!("never execute")
    }
    let listener = TcpListener::bind(addr)?;
    // accept connections and process them serially
    for stream in listener.incoming() {
        debug!("connected");
        if let Err(_) = handle_client(stream?, &mut store) {
            error!("TCP is disconnected")
        }
    }
    info!("kvs-server shutdown!");
    Ok(())
}

fn handle_client(stream: TcpStream, store: &mut Box<dyn KvsEngine>) -> Result<()> {
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
                if let Err(_) = store.set(key, val) {
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
                if let Err(_) = store.remove(key) {
                    writer.write_all("ErrInternal\n".as_bytes())?;
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
                let res = store.get(key);
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
