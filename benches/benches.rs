use criterion::{criterion_group, criterion_main, Criterion};
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
use tempfile::TempDir;
use tracing::info;
use tracing_subscriber;

pub fn thread_pool_bench(c: &mut Criterion) {
    const NUM_CLIENT: usize = 5;
    tracing_subscriber::fmt().init();
    let inputs = &[1, 2, 4, 6, 8, 10, 12, 14, 16];
    let mut rng = thread_rng();
    let mut data: Vec<String> = Vec::new();
    for _ in 0..NUM_CLIENT {
        let key: String = rng
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        data.push(key);
    }
    c.bench_function_over_inputs(
        "write-heavey",
        move |b, &&num| {
            info!("bench for {} thread pool", num);
            // start server
            let (shut_sdr, shut_rcv) = channel::bounded(1);
            let dir = TempDir::new().unwrap();
            let server_handle = thread::spawn(move || {
                server::run(
                    "127.0.0.1:4000",
                    KvStore::open(dir.path()).unwrap(),
                    SharedQueueThreadPool::new(num).unwrap(),
                    shut_rcv,
                )
                .unwrap();
            });

            // prepare client thread
            let mut handles = Vec::new();
            let (clients_ctl, cmd_input) = channel::bounded(0);
            let cmd_input = Arc::new(Mutex::new(cmd_input));
            let barrier_end = Arc::new(Barrier::new(NUM_CLIENT + 1));
            for k in &data {
                let k = k.to_owned();
                let ba_end = barrier_end.clone();
                let cmd_input = cmd_input.clone();
                let h = thread::spawn(move || loop {
                    let mut cli = Client::new(TcpStream::connect("127.0.0.1:4000").unwrap());
                    let cmd = cmd_input.lock().unwrap().recv().unwrap();
                    if cmd == 1 {
                        cli.set(k.to_owned(), "zh".to_owned())
                            .expect("failed to set");
                    } else {
                        break;
                    }
                    ba_end.wait();
                });
                handles.push(h);
            }
            info!("{} clients is ready", handles.len());

            // all ready
            b.iter(|| {
                info!("enter iter content");
                for _ in 0..NUM_CLIENT {
                    clients_ctl.send(1).unwrap();
                }
                info!("running !!!");
                // wait until all clients finished
                barrier_end.wait();
                info!("all clients finished")
            });

            // shutdown server
            shut_sdr.send(()).unwrap();
            Client::new(TcpStream::connect("127.0.0.1:4000").unwrap())
                .get("ping".to_owned())
                .unwrap_err();
            info!("waiting for server to exited");
            server_handle.join().unwrap();
            info!("server exited already. terminate clients now");

            // terminate clients thread
            for _ in 0..NUM_CLIENT {
                clients_ctl.send(2).unwrap();
            }
            info!("terminateing clients thread");
            for handle in handles.drain(..) {
                handle.join().unwrap();
            }
            info!("all clients exited");
        },
        inputs,
    );
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

criterion_group!(benches, thread_pool_bench);
criterion_main!(benches);
