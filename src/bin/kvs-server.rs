use clap::{Arg, Command};
use kvs::{KvStore, Result};
use std::io::{self, BufRead, Read, Write};
use std::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, trace};
use tracing_subscriber;

static x: &[char] = &['\n', '\t', ' '];
static store: KvStore = KvStore::open(".\\testdata").expect("open should't failed");

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

    let listener = TcpListener::bind(addr)?;
    // accept connections and process them serially
    for stream in listener.incoming() {
        if let Err(e) = handle_client(stream?) {
            error!(e, "TCP is disconnected")
        }
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream) -> Result<()> {
    let mut rdr = io::BufReader::new(stream.try_clone().unwrap());
    let mut op = [0; 1];
    loop {
        rdr.read_exact(&mut op)?;
        match *op.get(0).unwrap() as char {
            '+' => {
                let mut key = String::new();
                rdr.read_line(&mut key)?;
                key.trim_matches(x);
                if key.len() == 0 {
                    stream.write_all("ErrNoKey".as_bytes())?;
                }
                let mut val = String::new();
                rdr.read_line(&mut val)?;
                val.trim_matches(x);
                if val.len() == 0 {
                    stream.write_all("ErrNoVal".as_bytes())?;
                }
                if let Err(e) = store.set(key, val) {
                    stream.write_all("ErrInternal".as_bytes())?;
                } else {
                    stream.write_all("OK".as_bytes())?;
                }
            }
            '-' => {
                let mut key = String::new();
                rdr.read_line(&mut key)?;
                key.trim_matches(x);
                if key.len() == 0 {
                    stream.write_all("ErrNoKey".as_bytes())?;
                }
                if let Err(e) = store.remove(key) {
                    stream.write_all("ErrInternal".as_bytes())?;
                } else {
                    stream.write_all("OK".as_bytes())?;
                }
            }
            '?' => {
                let mut key = String::new();
                rdr.read_line(&mut key)?;
                key.trim_matches(x);
                if key.len() == 0 {
                    stream.write_all("ErrNoKey".as_bytes())?;
                }
                let res = store.get(key);
                if let Err(e) = res {
                    stream.write_all("ErrInternal".as_bytes())?;
                } else {
                    if let Some(v) = res.unwrap() {
                        stream.write_all(v.as_bytes())?;
                    } else {
                        stream.write_all("Nil".as_bytes())?;
                    }
                }
            }
            _ => {
                stream.write_all("ErrOp".as_bytes())?;
            }
        }
    }
}
