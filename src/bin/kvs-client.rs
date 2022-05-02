use clap::{Arg, Command};
use kvs::{KvStore, Result};
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
                .args(&[Arg::new(ARG_KEY), Arg::new(ARG_VAL)]),
            Command::new(CMD_GET)
                .about("Get value by key")
                .arg(Arg::new(ARG_KEY)),
            Command::new(CMD_RM)
                .about("Remove value by key")
                .arg(Arg::new(ARG_KEY)),
        ])
        .after_help("--Over--")
        .get_matches();

    let mut stream = TcpStream::connect("127.0.0.1:4000")?;
    match m.subcommand() {
        Some((CMD_SET, sub_m)) => {
            let key = sub_m.value_of(ARG_KEY).unwrap().to_owned();
            let val = sub_m.value_of(ARG_VAL).unwrap().to_owned();
            set(&mut stream, key, val)
        }
        Some((CMD_GET, sub_m)) => {
            let key = sub_m.value_of(ARG_KEY).unwrap().to_owned();
            let opt_val = get(&mut stream, key)?;
            if let Some(val) = opt_val {
                print!("{}", val);
            } else {
                println!("Key not found");
            }
            Ok(())
        }
        Some((CMD_RM, sub_m)) => {
            let key = sub_m.value_of(ARG_KEY).unwrap().to_owned();
            let result = remove(&mut stream, key);
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

fn set(stream: &mut TcpStream, key: String, val: String) -> Result<()> {}

fn get(stream: &mut TcpStream, key: String) -> Result<Option<String>> {}

fn remove(stream: &mut TcpStream, key: String) -> Result<()> {}
