use crate::protocol;
use crate::Result;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use tracing::debug;

pub struct Client {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl Client {
    pub fn new(stream: TcpStream) -> Self {
        Client {
            reader: BufReader::new(stream.try_clone().unwrap()),
            writer: BufWriter::new(stream),
        }
    }
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        self.writer.write(&[protocol::OP_SET])?;
        self.writer.write(key.as_bytes())?;
        self.writer.write(&['\n' as u8])?;
        self.writer.write(val.as_bytes())?;
        self.writer.write(&['\n' as u8])?;
        self.writer.flush()?;
        let mut ret = String::new();
        self.reader.read_line(&mut ret)?;
        debug!("response of set({},{}) received: {}", key, val, ret);
        Ok(())
    }
    pub fn remove(&mut self, key: String) -> Result<bool> {
        self.writer.write(&[protocol::OP_RM])?;
        self.writer.write(key.as_bytes())?;
        self.writer.write(&['\n' as u8])?;
        self.writer.flush()?;
        let mut ret = String::new();
        self.reader.read_line(&mut ret)?;
        if ret.contains("Key not found") {
            return Ok(false);
        }
        Ok(true)
    }
    pub fn get(&mut self, key: String) -> Result<String> {
        self.writer.write(&[protocol::OP_GET])?;
        self.writer.write(key.as_bytes())?;
        self.writer.write(&['\n' as u8])?;
        self.writer.flush()?;
        let mut header = [0 as u8; 1];
        self.reader.read_exact(&mut header)?;
        match *header.get(0).unwrap() {
            protocol::GET_VAL => {
                let mut val = String::new();
                self.reader.read_line(&mut val)?;
                Ok(val)
            }
            protocol::GET_NIL => Ok("Key not found".to_owned()),
            protocol::GET_ERR => {
                let mut err = String::new();
                self.reader.read_line(&mut err)?;
                Ok(format!("Err={}", err))
            }
            _ => Ok("Err = Protocol error".to_owned()),
        }
    }
}
