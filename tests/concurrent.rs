use assert_cmd::cargo::CommandCargoExt;
use kvs::client::Client;
use std::net::TcpStream;
use std::process::Command;
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const ADDR: &str = "127.0.0.1:4004";

fn concurrent_access_server(engine: &str, num: usize) {
    let (sender, receiver) = mpsc::sync_channel(0);
    let temp_dir = TempDir::new().unwrap();
    let mut server = Command::cargo_bin("kvs-server").unwrap();
    let mut child = server
        .args(&["--engine", engine, "--addr", ADDR])
        .current_dir(&temp_dir)
        .spawn()
        .unwrap();
    let handle = thread::spawn(move || {
        let _ = receiver.recv(); // wait for main thread to finish
        child.kill().expect("server exited before killed");
    });
    thread::sleep(Duration::from_secs(1));

    let mut handles = Vec::new();
    let barrier = Arc::new(Barrier::new(num + 1));
    for id in 0..num {
        let ba = barrier.clone();
        let h = thread::spawn(move || {
            ba.wait();
            client_set(id, "1");
            client_set(id, "2");
            client_get(id, "2\n");

            client_rm(id);
            client_get(id, "Key not found");

            client_set(id, "3");
            for _ in 0..10 {
                client_get(id, "3\n");
            }
        });
        handles.push(h);
    }
    barrier.wait();
    let now = Instant::now();
    for h in handles {
        h.join().expect("join falied");
    }
    println!("elapsed: {:?}", now.elapsed());
    sender.send(()).expect("can not notify server to exit");
    handle.join().expect("can not stop server")
}

fn client_set(id: usize, val: &str) {
    let stream = TcpStream::connect(ADDR).expect("client can not connect");
    let mut cli = Client::new(stream);
    let key = gen_key(id);
    cli.set(key, val.to_owned()).expect("client can not set");
}

fn client_rm(id: usize) {
    let stream = TcpStream::connect(ADDR).expect("client can not connect");
    let mut cli = Client::new(stream);
    let key = gen_key(id);
    cli.remove(key).expect("client can not rm");
}

fn client_get(id: usize, expect: &str) {
    let stream = TcpStream::connect(ADDR).expect("client can not connect");
    let mut cli = Client::new(stream);
    let key = gen_key(id);
    let got = cli.get(key).expect("can not get");
    let expect = expect.to_owned();
    assert_eq!(got, expect);
}

fn gen_key(id: usize) -> String {
    format!(
        "key-prefix-is-long-long-long-long-long-long-long-long-long-long-long {}",
        id
    )
    .to_owned()
}

#[test]
fn concurrent_access_server_kvs_engine() {
    concurrent_access_server("kvs", 1000);
}
