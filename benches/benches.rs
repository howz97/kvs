use criterion::{criterion_group, criterion_main, Criterion};
use kvs::my_engine::KvStore;
use kvs::sled_engine::SledKvEngine;
use kvs::KvsEngine;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use tempfile::TempDir;

pub fn criterion_benchmark(c: &mut Criterion) {
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

    let kvs_dir = TempDir::new().unwrap();
    let mut kvs_store = KvStore::open(&kvs_dir.path()).unwrap();
    c.bench_function("kvs write", |b| {
        b.iter(|| {
            for (k, v) in &data {
                kvs_store.set(k.clone(), v.clone()).unwrap();
            }
        })
    });
    c.bench_function("kvs read", |b| {
        b.iter(|| {
            for (k, v) in &read_seq {
                assert_eq!(kvs_store.get(k.clone()).unwrap().unwrap(), *v);
            }
        })
    });

    let sled_dir = TempDir::new().unwrap();
    let mut sled_store = SledKvEngine::open(&sled_dir.path()).unwrap();
    c.bench_function("sled write", |b| {
        b.iter(|| {
            for (k, v) in &data {
                sled_store.set(k.clone(), v.clone()).unwrap();
            }
        })
    });
    c.bench_function("sled read", |b| {
        b.iter(|| {
            for (k, v) in &read_seq {
                assert_eq!(sled_store.get(k.clone()).unwrap().unwrap(), *v);
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
