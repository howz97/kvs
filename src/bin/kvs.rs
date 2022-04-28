use clap::{Arg, Command};
use kvs::{KvStore, MyErr, Result};
use std::process::exit;

const ARG_KEY: &str = "key";
const ARG_VAL: &str = "value";

const CMD_SET: &str = "set";
const CMD_GET: &str = "get";
const CMD_RM: &str = "rm";

fn main() -> Result<()> {
    let m = Command::new(env!("CARGO_PKG_NAME"))
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

    let mut store = KvStore::open(".\\")?;
    match m.subcommand() {
        Some((CMD_SET, sub_m)) => {
            let key = sub_m.value_of(ARG_KEY).unwrap().to_owned();
            let val = sub_m.value_of(ARG_VAL).unwrap().to_owned();
            store.set(key, val)
        }
        Some((CMD_GET, sub_m)) => {
            let key = sub_m.value_of(ARG_KEY).unwrap().to_owned();
            let opt_val = store.get(key)?;
            if let Some(val) = opt_val {
                print!("{:?}", val);
            } else {
                println!("Key not found");
            }
            Ok(())
        }
        Some((CMD_RM, sub_m)) => {
            let key = sub_m.value_of(ARG_KEY).unwrap().to_owned();
            let result = store.remove(key);
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
