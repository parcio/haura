use betree_storage_stack::cache::{Cache, ClockCache};
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};

fn get_and_pin(b: &mut Bencher) {
    let mut c = ClockCache::new(5);
    c.insert(5, 5, 1);
    b.iter(|| {
        black_box(c.get(&5, true));
    });
}

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("get and pin", get_and_pin);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
