use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use crossbeam::channel;
use kvs::client::Client;
use kvs::my_engine::KvStore;
use kvs::server;
use kvs::sled_engine::SledKvEngine;
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::KvsEngine;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::net::TcpStream;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use tracing::{debug, error, info, trace};
use tracing_subscriber;

const NUM_CLIENT: usize = 200;
const SERVER_ADDR: &str = "127.0.0.1:4000";

pub fn thread_pool_bench(c: &mut Criterion) {
    tracing_subscriber::fmt().init();
    let inputs = &[256];
    let mut rng = thread_rng();
    let mut data: Vec<String> = Vec::new();
    for _ in 0..NUM_CLIENT {
        let key: String = rng
            .sample_iter(&Alphanumeric)
            .take(1000)
            .map(char::from)
            .collect();
        data.push(key);
    }
    // let data2 = data.clone();
    // c.bench_function_over_inputs(
    //     "write-heavey",
    //     move |b, &&num| concurrent_bench(b, num, &data2, true),
    //     inputs,
    // );
    c.bench_function_over_inputs(
        "read-heavey",
        move |b, &&num| concurrent_bench(b, num, &data, false),
        inputs,
    );
}

fn concurrent_bench(b: &mut Bencher, num: u32, data: &Vec<String>, is_write: bool) {
    debug!("bench for {} thread pool", num);
    // start server
    let (shut_sdr, shut_rcv) = channel::bounded(1);
    let dir = TempDir::new().unwrap();
    let server_handle = thread::spawn(move || {
        server::run(
            SERVER_ADDR,
            KvStore::open(dir.path()).unwrap(),
            SharedQueueThreadPool::new(num).unwrap(),
            shut_rcv,
        )
        .expect("failed to start server");
    });

    // prepare client thread
    let mut handles = Vec::new();
    let (clients_ctl, cmd_input) = channel::bounded(0);
    let cmd_input = Arc::new(Mutex::new(cmd_input));
    let barrier_end = Arc::new(Barrier::new(NUM_CLIENT + 1));
    for (i, k) in data.iter().enumerate() {
        let k = k.to_owned();
        let ba_end = barrier_end.clone();
        let cmd_input = cmd_input.clone();
        let h = thread::spawn(move || loop {
            let cmd = cmd_input.lock().unwrap().recv().unwrap();
            trace!("client {} received command {}", i, cmd);
            if cmd == 1 {
                if is_write {
                    client_write_heavy(k.to_owned());
                } else {
                    client_read_heavy(k.to_owned());
                }
            } else {
                break;
            }
            if ba_end.wait().is_leader() {
                debug!("barrier passed");
            }
        });
        handles.push(h);
    }
    thread::sleep(Duration::from_secs(1));
    debug!("{} clients is ready", handles.len());

    // all ready
    b.iter(|| {
        debug!("enter iter content");
        for _ in 0..NUM_CLIENT {
            clients_ctl.send(1).unwrap();
        }
        // wait until all clients finished
        barrier_end.wait();
        debug!("all clients finished")
    });

    // shutdown server
    shut_sdr.send(()).unwrap();
    loop {
        let stream = TcpStream::connect(SERVER_ADDR);
        if let Err(e) = stream {
            error!("failed to connnect to server, try again: {}", e);
            thread::sleep(Duration::from_secs(1));
            continue;
        }
        Client::new(stream.unwrap())
            .get("ping".to_owned())
            .unwrap_err();
        debug!("waiting for server to exited");
        server_handle.join().unwrap();
        break;
    }
    debug!("server exited already. terminate clients now");

    // terminate clients thread
    for _ in 0..NUM_CLIENT {
        clients_ctl.send(2).unwrap();
    }
    debug!("terminateing clients thread");
    for handle in handles.drain(..) {
        handle.join().unwrap();
    }
    debug!("all clients exited");
}

fn client_write_heavy(k: String) {
    for _ in 0..5 {
        let stream = TcpStream::connect(SERVER_ADDR);
        match stream {
            Err(e) => error!("client failed to connect server {}", e),
            Ok(stream) => {
                let mut cli = Client::new(stream);
                if let Err(e) = cli.set(k.to_owned(), k.to_owned()) {
                    error!("failed to set {}", e);
                }
            }
        }
    }
}

fn client_read_heavy(k: String) {
    let stream = TcpStream::connect(SERVER_ADDR);
    match stream {
        Err(e) => error!("client failed to connect server {}", e),
        Ok(stream) => {
            let mut cli = Client::new(stream);
            if let Err(e) = cli.set(k.to_owned(), k.to_owned()) {
                error!("failed to set {}", e);
            }
        }
    }
    for _ in 0..4 {
        let stream = TcpStream::connect(SERVER_ADDR);
        match stream {
            Err(e) => error!("client failed to connect server {}", e),
            Ok(stream) => {
                let mut cli = Client::new(stream);
                match cli.get(k.to_owned()) {
                    Err(e) => {
                        error!("failed to get {}", e);
                    }
                    Ok(mut ret) => {
                        ret.pop();
                        if ret != k {
                            error!("expect: {}\ngot: {}", k, ret);
                        }
                    }
                }
            }
        }
    }
}

pub fn engine_benchmark(c: &mut Criterion) {
    let mut rng = thread_rng();
    let mut data = Vec::new();
    for _ in 0..100 {
        let key_len: usize = rng.gen_range(1, 100000);
        let key: String = rng
            .sample_iter(&Alphanumeric)
            .take(key_len)
            .map(char::from)
            .collect();
        let val_len: usize = rng.gen_range(1, 100000);
        let val: String = rng
            .sample_iter(&Alphanumeric)
            .take(val_len)
            .map(char::from)
            .collect();
        data.push((key, val));
    }
    let mut read_seq = Vec::new();
    for _ in 0..1000 {
        let i = rng.gen_range(0, 100);
        let (k, v) = data.get(i).unwrap();
        read_seq.push((k.clone(), v.clone()));
    }
    let data_2 = data.clone();
    let read_seq_2 = read_seq.clone();
    engine_sled_bench(c, data_2, read_seq_2);
    engine_kvs_bench(c, data, read_seq);
}

fn engine_kvs_bench(
    c: &mut Criterion,
    data: Vec<(String, String)>,
    read_seq: Vec<(String, String)>,
) {
    let kvs_dir = TempDir::new().unwrap();
    let kvs_store = KvStore::open(&kvs_dir.path()).unwrap();
    let cp = kvs_store.clone();
    c.bench_function("kvs write", move |b| {
        b.iter(|| {
            for (k, v) in &data {
                cp.set(k.clone(), v.clone()).unwrap();
            }
        })
    });
    c.bench_function("kvs read", move |b| {
        b.iter(|| {
            for (k, v) in &read_seq {
                assert_eq!(kvs_store.get(k.clone()).unwrap().unwrap(), *v);
            }
        })
    });
}

fn engine_sled_bench(
    c: &mut Criterion,
    data: Vec<(String, String)>,
    read_seq: Vec<(String, String)>,
) {
    let sled_dir = TempDir::new().unwrap();
    let sled_store = SledKvEngine::open(&sled_dir.path()).unwrap();
    let cp = sled_store.clone();
    c.bench_function("sled write", move |b| {
        b.iter(|| {
            for (k, v) in &data {
                cp.set(k.clone(), v.clone()).unwrap();
            }
        })
    });
    c.bench_function("sled read", move |b| {
        b.iter(|| {
            for (k, v) in &read_seq {
                assert_eq!(sled_store.get(k.clone()).unwrap().unwrap(), *v);
            }
        })
    });
}

criterion_group! {
    name = benches;
    // This can be any expression that returns a `Criterion` object.
    config = Criterion::default().significance_level(0.1).sample_size(30);
    targets = engine_benchmark
}
criterion_main!(benches);
