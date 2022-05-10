use clap::{Arg, Command};
use kvs::client::Client;
use kvs::Result;
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
                    Arg::new("addr")
                        .long("addr")
                        .default_value("127.0.0.1:4000"),
                ]),
            Command::new(CMD_GET)
                .about("Get value by key")
                .arg(Arg::new(ARG_KEY))
                .arg(
                    Arg::new("addr")
                        .long("addr")
                        .default_value("127.0.0.1:4000"),
                ),
            Command::new(CMD_RM)
                .about("Remove value by key")
                .arg(Arg::new(ARG_KEY))
                .arg(
                    Arg::new("addr")
                        .long("addr")
                        .default_value("127.0.0.1:4000"),
                ),
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
            client.remove(key)
        }
        _ => {
            eprintln!("arguments needed, use --help to get more information");
            exit(1);
        }
    }
}
