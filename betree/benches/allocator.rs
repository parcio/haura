use betree_storage_stack::allocator::{
    Allocator, BestFitSimple, FirstFit, NextFit, SegmentAllocator, WorstFitSimple,
    SEGMENT_SIZE_BYTES,
};
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use rand::distributions::{Distribution, Uniform};
use rand::rngs::StdRng;
use rand::SeedableRng;
use zipf::ZipfDistribution;

#[derive(Clone)]
enum SizeDistribution {
    Uniform(Uniform<usize>),
    Zipfian(ZipfDistribution),
}

fn bench_allocator<A: Allocator>(
    b: &mut Bencher,
    dist: SizeDistribution,
    alloc_ratio: f64,
    min_size: usize,
    max_size: usize,
) {
    let mut allocator = A::new([0; SEGMENT_SIZE_BYTES]);
    let mut rng = StdRng::seed_from_u64(42);
    let mut allocated = Vec::new();

    b.iter(|| {
        if rand::random::<f64>() < alloc_ratio {
            // Allocation path
            let size = match &dist {
                SizeDistribution::Uniform(u) => u.sample(&mut rng),
                SizeDistribution::Zipfian(z) => {
                    (z.sample(&mut rng) as usize).clamp(min_size, max_size)
                }
            } as u32;
            if let Some(offset) = allocator.allocate(size) {
                allocated.push((offset, size));
            }
        } else if !allocated.is_empty() {
            // Deallocation path
            let idx = rand::random::<usize>() % allocated.len();
            let (offset, size) = allocated.swap_remove(idx);
            allocator.deallocate(offset, size);
        }
    });
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let min_size = 64;
    let max_size = 4096;
    let zipfian_exponent = 0.99;

    let distributions = [
        (
            "uniform",
            SizeDistribution::Uniform(Uniform::new(min_size, max_size)),
        ),
        (
            "zipfian",
            SizeDistribution::Zipfian(
                ZipfDistribution::new(max_size - min_size, zipfian_exponent).expect(""),
            ),
        ),
    ];

    for (dist_name, dist) in distributions {
        let mut group = c.benchmark_group(dist_name);
        group.bench_function("first_fit", |b| {
            bench_allocator::<FirstFit>(b, dist.clone(), 0.8, min_size, max_size)
        });
        group.bench_function("next_fit", |b| {
            bench_allocator::<NextFit>(b, dist.clone(), 0.8, min_size, max_size)
        });
        group.bench_function("best_fit", |b| {
            bench_allocator::<BestFitSimple>(b, dist.clone(), 0.8, min_size, max_size)
        });
        group.bench_function("worst_fit", |b| {
            bench_allocator::<WorstFitSimple>(b, dist.clone(), 0.8, min_size, max_size)
        });
        group.bench_function("segment", |b| {
            bench_allocator::<SegmentAllocator>(b, dist.clone(), 0.8, min_size, max_size)
        });
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

