use clap::{Arg, Command};
use kvs::my_engine;
use kvs::protocol;
use kvs::sled_engine;
use kvs::{KvsEngine, Result};
use std::io::{self, BufRead, Read, Write};
use std::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};
use tracing_subscriber;

static X: &[char] = &['\n', '\t', ' '];

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let m = Command::new("kvs-server")
        .author(env!("CARGO_PKG_AUTHORS"))
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(Arg::new("addr").default_value("127.0.0.1:4000"))
        .arg(Arg::new("engine").default_value("kvs"))
        .after_help("--Over--")
        .get_matches();
    let addr = m.value_of("addr").unwrap();
    let engine = m.value_of("engine").unwrap();
    info!(addr, engine, "kvs-server start...");

    let mut store: Box<dyn KvsEngine>;
    if let Some(eng) = m.value_of("engine") {
        store = Box::new(my_engine::KvStore::open(".\\testdata")?);
    } else {
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

fn handle_client(mut stream: TcpStream, store: &mut KvStore) -> Result<()> {
    let mut reader = io::BufReader::new(stream.try_clone().unwrap());
    let mut op = [0; 1];
    loop {
        reader.read_exact(&mut op)?;
        match *op.get(0).unwrap() {
            protocol::OP_SET => {
                let mut key = String::new();
                reader.read_line(&mut key)?;
                key = key.trim_matches(X).to_owned();
                if key.len() == 0 {
                    stream.write_all("ErrNoKey".as_bytes())?;
                }
                let mut val = String::new();
                reader.read_line(&mut val)?;
                val = val.trim_matches(X).to_owned();
                if val.len() == 0 {
                    stream.write_all("ErrNoVal".as_bytes())?;
                }
                if let Err(_) = store.set(key, val) {
                    stream.write_all("ErrInternal".as_bytes())?;
                } else {
                    stream.write_all("OK".as_bytes())?;
                }
            }
            protocol::OP_RM => {
                let mut key = String::new();
                reader.read_line(&mut key)?;
                key = key.trim_matches(X).to_owned();
                if key.len() == 0 {
                    stream.write_all("ErrNoKey".as_bytes())?;
                }
                if let Err(_) = store.remove(key) {
                    stream.write_all("ErrInternal".as_bytes())?;
                } else {
                    stream.write_all("OK".as_bytes())?;
                }
            }
            protocol::OP_GET => {
                let mut key = String::new();
                reader.read_line(&mut key)?;
                key = key.trim_matches(X).to_owned();
                if key.len() == 0 {
                    stream.write(&[protocol::GET_ERR])?;
                    stream.write_all("ErrNoKey".as_bytes())?;
                }
                let res = store.get(key);
                if let Err(_) = res {
                    stream.write(&[protocol::GET_ERR])?;
                    stream.write_all("ErrInternal".as_bytes())?;
                } else {
                    if let Some(v) = res.unwrap() {
                        stream.write(&[protocol::GET_VAL])?;
                        stream.write_all(v.as_bytes())?;
                    } else {
                        stream.write_all(&[protocol::GET_NIL])?;
                    }
                }
            }
            _ => {
                stream.write_all("ErrOp".as_bytes())?;
            }
        }
    }
}
