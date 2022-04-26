use clap::{Arg, Command};
use kvs::Result;
use std::process::exit;

fn main() -> Result<()> {
    let m = Command::new(env!("CARGO_PKG_NAME"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommands(vec![
            Command::new("set")
                .about("Insert/Update key value")
                .args(&[Arg::new("key"), Arg::new("value")]),
            Command::new("get")
                .about("Get value by key")
                .arg(Arg::new("key")),
            Command::new("rm")
                .about("Remove value by key")
                .arg(Arg::new("key")),
        ])
        .after_help("--Over--")
        .get_matches();

    match m.subcommand() {
        Some(("set", _)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        Some(("get", _)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        Some(("rm", _)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        _ => {
            eprintln!("arguments needed, use --help to get more information");
            exit(1);
        }
    }
}
