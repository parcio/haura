use std::sync::atomic::AtomicU8;

use bincode::serialized_size;
use criterion::{criterion_group, criterion_main, Criterion};
use serde::Serialize;

#[derive(Serialize)]
pub struct Dummy<T> {
    a: u32,
    b: usize,
    #[serde(skip)]
    c: AtomicU8,
    #[serde(skip)]
    d: AtomicU8,
    e: Vec<T>,
    f: Vec<T>,
}

static DUMMY_NODE: Dummy<()> = Dummy {
    a: 0,
    b: 0,
    c: AtomicU8::new(0),
    d: AtomicU8::new(0),
    e: vec![],
    f: vec![],
};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("serialized_size_internal_node", |b| {
        b.iter(|| serialized_size(&DUMMY_NODE).unwrap() as usize)
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
