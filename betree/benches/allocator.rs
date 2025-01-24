use betree_storage_stack::allocator::{
    self, Allocator, BestFitSimple, FirstFit, FirstFitList, NextFit, NextFitList, SegmentAllocator,
    WorstFitSimple, SEGMENT_SIZE_BYTES,
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
    let mut allocated = Vec::new();

    let mut rng = StdRng::seed_from_u64(42);
    let mut sample_size = || -> u32 {
        match &dist {
            SizeDistribution::Uniform(u) => return black_box(u.sample(&mut rng)) as u32,
            SizeDistribution::Zipfian(z) => {
                return (black_box(z.sample(&mut rng)) as usize).clamp(min_size, max_size) as u32
            }
        }
    };

    b.iter(|| {
        if rand::random::<f64>() < alloc_ratio {
            // Allocation path
            let size = sample_size();
            if let Some(offset) = black_box(allocator.allocate(size)) {
                allocated.push((offset, size));
            }
        } else if !allocated.is_empty() {
            // Deallocation path
            let idx = rand::random::<usize>() % allocated.len();
            let (offset, size) = allocated.swap_remove(idx);
            black_box(allocator.deallocate(offset, size));
        }
    });
}

// In Haura, allocators are not continuously active in memory. Instead, they are loaded from disk
// when needed. This benchmark simulates this behavior by creating a new allocator instance for each
// iteration. Also deallocations are buffered and applied during sync operations, not immediately to
// the allocator. Here, we simulate the sync operation by directly modifying the underlying bitmap
// data after the allocator has performed allocations, mimicking the delayed deallocation process.
fn bench_allocator_with_sync<A: Allocator>(
    b: &mut Bencher,
    dist: SizeDistribution,
    allocations: u64,
    deallocations: u64,
    min_size: usize,
    max_size: usize,
) {
    let data = [0; SEGMENT_SIZE_BYTES];
    let mut allocated = Vec::new();

    let mut rng = StdRng::seed_from_u64(42);
    let mut sample_size = || -> u32 {
        match &dist {
            SizeDistribution::Uniform(u) => return black_box(u.sample(&mut rng)) as u32,
            SizeDistribution::Zipfian(z) => {
                return (black_box(z.sample(&mut rng)) as usize).clamp(min_size, max_size) as u32
            }
        }
    };

    b.iter(|| {
        let mut allocator = A::new(data);
        for _ in 0..allocations {
            let size = sample_size();
            if let Some(offset) = black_box(allocator.allocate(size)) {
                allocated.push((offset, size));
            }
        }

        // Simulates the deferred deallocations
        let bitmap = allocator.data();
        for _ in 0..deallocations {
            if allocated.is_empty() {
                break;
            }
            let idx = rand::random::<usize>() % allocated.len();
            let (offset, size) = allocated.swap_remove(idx);

            let start = offset as usize;
            let end = (offset + size) as usize;
            let range = &mut bitmap[start..end];
            range.fill(false);
        }
        // At the end of the iteration, the allocator goes out of scope, simulating it being
        // unloaded from memory. In the next iteration, a new allocator will be created and loaded
        // with the modified bitmap data.
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

    for (dist_name, dist) in distributions.clone() {
        let mut group = c.benchmark_group(dist_name);
        group.bench_function("first_fit", |b| {
            bench_allocator::<FirstFit>(b, dist.clone(), 0.8, min_size, max_size)
        });
        group.bench_function("first_fit_list", |b| {
            bench_allocator::<FirstFitList>(b, dist.clone(), 0.8, min_size, max_size)
        });
        group.bench_function("next_fit", |b| {
            bench_allocator::<NextFit>(b, dist.clone(), 0.8, min_size, max_size)
        });
        group.bench_function("next_fit_list", |b| {
            bench_allocator::<NextFitList>(b, dist.clone(), 0.8, min_size, max_size)
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

    let alloc_dealloc_ratios = [
        (100, 50),
        (500, 250),
        (1000, 500),
        (5000, 2500),
        (10000, 5000),
    ];

    for (dist_name, dist) in distributions {
        for (allocations, deallocations) in alloc_dealloc_ratios {
            let group_name = format!("{}_sync_{}_{}", dist_name, allocations, deallocations);
            let mut group = c.benchmark_group(group_name);
            group.bench_function("first_fit", |b| {
                bench_allocator_with_sync::<FirstFit>(
                    b,
                    dist.clone(),
                    allocations,
                    deallocations,
                    min_size,
                    max_size,
                )
            });
            group.bench_function("first_fit_list", |b| {
                bench_allocator_with_sync::<FirstFitList>(
                    b,
                    dist.clone(),
                    allocations,
                    deallocations,
                    min_size,
                    max_size,
                )
            });
            group.bench_function("next_fit", |b| {
                bench_allocator_with_sync::<NextFit>(
                    b,
                    dist.clone(),
                    allocations,
                    deallocations,
                    min_size,
                    max_size,
                )
            });
            group.bench_function("next_fit_list", |b| {
                bench_allocator_with_sync::<NextFitList>(
                    b,
                    dist.clone(),
                    allocations,
                    deallocations,
                    min_size,
                    max_size,
                )
            });
            group.bench_function("best_fit", |b| {
                bench_allocator_with_sync::<BestFitSimple>(
                    b,
                    dist.clone(),
                    allocations,
                    deallocations,
                    min_size,
                    max_size,
                )
            });
            group.bench_function("worst_fit", |b| {
                bench_allocator_with_sync::<WorstFitSimple>(
                    b,
                    dist.clone(),
                    allocations,
                    deallocations,
                    min_size,
                    max_size,
                )
            });
            group.bench_function("segment", |b| {
                bench_allocator_with_sync::<SegmentAllocator>(
                    b,
                    dist.clone(),
                    allocations,
                    deallocations,
                    min_size,
                    max_size,
                )
            });
            group.finish();
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
