use crate::protocol;
use crate::{KvsEngine, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

static X: &[char] = &['\n', '\t', ' '];

pub async fn run<E: KvsEngine>(addr: &str, engine: E) -> Result<()> {
    info!("kvs-server is running...");
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        let eng = engine.clone();
        debug!("connected socket {}", addr);
        tokio::spawn(handler(stream, eng));
    }
}

pub async fn handler<E: KvsEngine>(mut stream: TcpStream, eng: E) -> Result<()> {
    let (reader, writer) = stream.split();
    let mut reader = BufReader::with_capacity(1024, reader);
    let mut writer = BufWriter::with_capacity(1024, writer);
    match reader.read_u8().await? {
        protocol::OP_SET => {
            let mut key = String::new();
            reader.read_line(&mut key).await?;
            key = key.trim_matches(X).to_owned();
            if key.len() == 0 {
                writer.write_all("ErrNoKey\n".as_bytes()).await?;
                return Ok(());
            }
            let mut val = String::new();
            reader.read_line(&mut val).await?;
            val = val.trim_matches(X).to_owned();
            if val.len() == 0 {
                writer.write_all("ErrNoVal\n".as_bytes()).await?;
                return Ok(());
            }
            if let Err(_) = eng.set(key, val).await {
                writer.write_all("ErrInternal\n".as_bytes()).await?;
            } else {
                writer.write_all("OK\n".as_bytes()).await?;
            }
        }
        protocol::OP_RM => {
            let mut key = String::new();
            reader.read_line(&mut key).await?;
            key = key.trim_matches(X).to_owned();
            if key.len() == 0 {
                writer.write_all("ErrNoKey\n".as_bytes()).await?;
                return Ok(());
            }
            debug!("Removing {}", key);
            if let Err(e) = eng.remove(key).await {
                writer.write_all(e.to_string().as_bytes()).await?;
                writer.write_all(&['\n' as u8]).await?;
            } else {
                writer.write_all("OK\n".as_bytes()).await?;
            }
        }
        protocol::OP_GET => {
            let mut key = String::new();
            reader.read_line(&mut key).await?;
            key = key.trim_matches(X).to_owned();
            if key.len() == 0 {
                writer.write_u8(protocol::GET_ERR).await?;
                writer.write_all("ErrNoKey\n".as_bytes()).await?;
                return Ok(());
            }
            debug!("OP_GET key={}", key);
            let res = eng.get(key).await;
            if let Err(e) = res {
                error!("OP_GET: err={}", e);
                writer.write_u8(protocol::GET_ERR).await?;
                writer.write_all("ErrInternal\n".as_bytes()).await?;
            } else {
                if let Some(v) = res.unwrap() {
                    writer.write_u8(protocol::GET_VAL).await?;
                    writer.write_all(v.as_bytes()).await?;
                } else {
                    writer.write_u8(protocol::GET_NIL).await?;
                }
                writer.write_u8('\n' as u8).await?;
            }
        }
        _ => {
            panic!("unknown operation");
        }
    }
    writer.flush().await?;
    Ok(())
}
