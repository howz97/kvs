use clap::{Arg, Command};
use std::process::exit;

fn main() {
    let m = Command::new("kvs")
        .author("howz97, <964701944@qq.com>")
        .version("v0.1.0")
        .about("Key value store")
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

    if let Some(_) = m.value_of("version") {
        println!("kvs v0.1.0");
        exit(0);
    }
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
