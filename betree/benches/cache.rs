use betree_storage_stack::{
    cache::{Cache, ClockCache},
    compression::DecompressionTag,
    database::{DatasetId, Generation},
    storage_pool::DiskOffset,
    vdev::Block,
};
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};

fn get_and_pin(b: &mut Bencher) {
    let five = DiskOffset::new(0, 0, Block(5));
    let mut c = ClockCache::new(5);
    c.insert(five, five, 1);
    b.iter(|| {
        black_box(c.get(&five, true));
    });
}

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("get and pin", get_and_pin);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
