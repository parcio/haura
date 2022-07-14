use betree_storage_stack::allocator::{SegmentAllocator, SEGMENT_SIZE_BYTES};
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};

fn allocate(b: &mut Bencher) {
    let mut a = SegmentAllocator::new([0; SEGMENT_SIZE_BYTES]);
    b.iter(|| {
        black_box(a.allocate(10));
    });
}

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("allocate", allocate);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
