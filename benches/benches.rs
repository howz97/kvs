use criterion::{criterion_group, criterion_main, Criterion};
use kvs::my_engine::KvStore;
use kvs::sled_engine::SledKvEngine;
use kvs::KvsEngine;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use tempfile::TempDir;

pub fn thread_pool_bench(c: &mut Criterion) {
    let inputs = &[1, 2, 4, 6, 8, 10, 12, 14, 16];

    c.bench_function_over_inputs(
        "example",
        |b, &&num| {
            // do setup here
            b.iter(|| {
                // important measured work goes here
            });
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

criterion_group!(benches, engine_benchmark);
criterion_main!(benches);
