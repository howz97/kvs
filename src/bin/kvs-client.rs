use clap::{Arg, Command};
use kvs::protocol;
use kvs::Result;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::process::exit;

const ARG_KEY: &str = "key";
const ARG_VAL: &str = "value";

const CMD_SET: &str = "set";
const CMD_GET: &str = "get";
const CMD_RM: &str = "rm";

fn main() -> Result<()> {
    let m = Command::new("kvs-client")
        .author(env!("CARGO_PKG_AUTHORS"))
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommands(vec![
            Command::new(CMD_SET)
                .about("Insert/Update key value")
                .args(&[
                    Arg::new(ARG_KEY),
                    Arg::new(ARG_VAL),
                    Arg::new("addr").default_value("127.0.0.1:4000"),
                ]),
            Command::new(CMD_GET)
                .about("Get value by key")
                .arg(Arg::new(ARG_KEY))
                .arg(Arg::new("addr").default_value("127.0.0.1:4000")),
            Command::new(CMD_RM)
                .about("Remove value by key")
                .arg(Arg::new(ARG_KEY))
                .arg(Arg::new("addr").default_value("127.0.0.1:4000")),
        ])
        .after_help("--Over--")
        .get_matches();

    match m.subcommand() {
        Some((CMD_SET, sub_m)) => {
            let mut client = Client::new(TcpStream::connect(sub_m.value_of("addr").unwrap())?);
            let key = sub_m.value_of(ARG_KEY).unwrap().to_owned();
            let val = sub_m.value_of(ARG_VAL).unwrap().to_owned();
            client.set(key, val)
        }
        Some((CMD_GET, sub_m)) => {
            let mut client = Client::new(TcpStream::connect(sub_m.value_of("addr").unwrap())?);
            let key = sub_m.value_of(ARG_KEY).unwrap().to_owned();
            client.get(key)?;
            Ok(())
        }
        Some((CMD_RM, sub_m)) => {
            let mut client = Client::new(TcpStream::connect(sub_m.value_of("addr").unwrap())?);
            let key = sub_m.value_of(ARG_KEY).unwrap().to_owned();
            let result = client.remove(key);
            if let Err(e) = result {
                println!("{}", e);
                exit(1);
            }
            Ok(())
        }
        _ => {
            eprintln!("arguments needed, use --help to get more information");
            exit(1);
        }
    }
}

struct Client {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl Client {
    fn new(stream: TcpStream) -> Self {
        Client {
            reader: BufReader::new(stream.try_clone().unwrap()),
            writer: BufWriter::new(stream),
        }
    }
    fn set(&mut self, key: String, val: String) -> Result<()> {
        self.writer.write(&[protocol::OP_SET])?;
        self.writer.write(key.as_bytes())?;
        self.writer.write(&['\n' as u8])?;
        self.writer.write(val.as_bytes())?;
        self.writer.write(&['\n' as u8])?;
        self.writer.flush()?;
        let mut ret = String::new();
        self.reader.read_line(&mut ret)?;
        println!("{}", ret);
        Ok(())
    }
    fn remove(&mut self, key: String) -> Result<()> {
        self.writer.write(&[protocol::OP_RM])?;
        self.writer.write(key.as_bytes())?;
        self.writer.write(&['\n' as u8])?;
        self.writer.flush()?;
        let mut ret = String::new();
        self.reader.read_line(&mut ret)?;
        println!("{}", ret);
        Ok(())
    }
    fn get(&mut self, key: String) -> Result<()> {
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
                println!("Val={}", val);
            }
            protocol::GET_NIL => {
                println!("Nil");
            }
            protocol::GET_ERR => {
                let mut err = String::new();
                self.reader.read_line(&mut err)?;
                println!("Err={}", err);
            }
            _ => {
                println!("Err = Protocol error")
            }
        }
        Ok(())
    }
}
