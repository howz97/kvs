use criterion::{black_box, criterion_group, criterion_main, Criterion};
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
            for _ in 0..10 {
                for (k, _) in &data {
                    kvs_store.get(k.clone()).unwrap();
                }
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
            for _ in 0..10 {
                for (k, _) in &data {
                    sled_store.get(k.clone()).unwrap();
                }
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
