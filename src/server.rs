use crate::protocol;
use crate::thread_pool::ThreadPool;
use crate::{KvsEngine, Result};
use crossbeam::channel::Receiver;
use std::io::{self, BufRead, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;
use tracing::{debug, error, info};

static X: &[char] = &['\n', '\t', ' '];

pub fn run<E: KvsEngine, P: ThreadPool>(
    addr: &str,
    engine: E,
    pool: P,
    shutdown: Receiver<()>,
) -> Result<()> {
    info!("kvs-server is running...");
    let listener = TcpListener::bind(addr)?;
    for stream in listener.incoming() {
        if shutdown.try_recv().is_ok() {
            debug!("server received shutdown command!");
            break;
        }
        let stream = stream?;
        let eng = engine.clone();
        debug!("start serving client");
        pool.spawn(move || {
            if let Err(e) = handler(stream, eng) {
                error!("error on serving client: {}", e)
            }
        });
    }
    Ok(())
}

pub fn handler<E: KvsEngine>(stream: TcpStream, eng: E) -> Result<()> {
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("failed to set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(1)))
        .expect("failed to set write timeout");
    let mut reader = io::BufReader::new(&stream);
    let mut writer = io::BufWriter::new(&stream);
    let mut op = [0; 1];
    reader.read_exact(&mut op)?;
    match *op.get(0).unwrap() {
        protocol::OP_SET => {
            let mut key = String::new();
            reader.read_line(&mut key)?;
            key = key.trim_matches(X).to_owned();
            if key.len() == 0 {
                writer.write_all("ErrNoKey\n".as_bytes())?;
                return Ok(());
            }
            let mut val = String::new();
            reader.read_line(&mut val)?;
            val = val.trim_matches(X).to_owned();
            if val.len() == 0 {
                writer.write_all("ErrNoVal\n".as_bytes())?;
                return Ok(());
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
                return Ok(());
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
                return Ok(());
            }
            let res = eng.get(key);
            if let Err(e) = res {
                error!("OP_GET: {}", e);
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
    Ok(())
}
