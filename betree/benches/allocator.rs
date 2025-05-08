use std::time::{Duration, Instant};

use betree_storage_stack::allocator::{
    self, Allocator, ApproximateBestFitTree, BestFitList, BestFitScan, BestFitTree, FirstFitList,
    FirstFitScan, FirstFitTree, HybridAllocator, NextFitList, NextFitScan, SegmentAllocator,
    WorstFitList, WorstFitScan, WorstFitTree, SEGMENT_SIZE_BYTES, SEGMENT_SIZE_LOG_2,
};
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use rand::{
    distributions::{Distribution, Uniform},
    rngs::StdRng,
    SeedableRng,
};
use zipf::ZipfDistribution;

#[derive(Clone)]
enum SizeDistribution {
    Uniform(Uniform<usize>),
    Zipfian(ZipfDistribution),
}

// Define a type alias for our benchmark function to make it less verbose
type BenchmarkFn = Box<dyn Fn(&mut Bencher, SizeDistribution, u64, u64, usize, usize)>;

// Macro to generate allocator benchmark entries
macro_rules! allocator_benchmark {
    ($name:expr, $allocator_type:ty, $bench_function:ident) => {
        (
            $name,
            Box::new(|b, dist, allocations, deallocations, min_size, max_size| {
                $bench_function::<$allocator_type>(
                    b,
                    dist,
                    allocations,
                    deallocations,
                    min_size,
                    max_size,
                )
            }),
        )
    };
}

// Macro to generate allocator benchmark entries with name derived from type
macro_rules! generate_allocator_benchmarks {
    ($bench_function:ident, $($allocator_type:ty),*) => {
        vec![
            $(
                allocator_benchmark!(stringify!($allocator_type), $allocator_type, $bench_function),
            )*
        ]
    };
}

// In Haura, allocators are not continuously active in memory. Instead, they are loaded from disk
// when needed. This benchmark simulates this behavior by creating a new allocator instance for each
// iteration. Also deallocations are buffered and applied during sync operations, not immediately to
// the allocator. Here, we simulate the sync operation by directly modifying the underlying bitmap
// data after the allocator has performed allocations, mimicking the delayed deallocation process.
fn bench_alloc<A: Allocator>(
    b: &mut Bencher,
    dist: SizeDistribution,
    allocations: u64,
    deallocations: u64,
    min_size: usize,
    max_size: usize,
) {
    let data = [0; SEGMENT_SIZE_BYTES];
    let mut allocated = Vec::new();
    allocated.reserve(allocations as usize);

    let mut rng = StdRng::seed_from_u64(42);
    let mut sample_size = || -> u32 {
        match &dist {
            SizeDistribution::Uniform(u) => return black_box(u.sample(&mut rng)) as u32,
            SizeDistribution::Zipfian(z) => {
                let rank = black_box(z.sample(&mut rng)) as usize;
                return (min_size + (rank - 1)) as u32;
            }
        }
    };

    b.iter_custom(|iters| {
        let mut total_allocation_time = Duration::new(0, 0);

        for _ in 0..iters {
            allocated.clear();
            let mut allocator = A::new(data);

            let start = Instant::now();
            for _ in 0..allocations {
                let size = sample_size();
                if let Some(offset) = black_box(allocator.allocate(size)) {
                    allocated.push((offset, size));
                }
            }
            total_allocation_time += start.elapsed();

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
        }
        Duration::from_nanos((total_allocation_time.as_nanos() / allocations as u128) as u64)
    });
}

fn bench_new<A: Allocator>(
    b: &mut Bencher,
    dist: SizeDistribution,
    allocations: u64,
    deallocations: u64,
    min_size: usize,
    max_size: usize,
) {
    let data = [0; SEGMENT_SIZE_BYTES];
    let mut allocated = Vec::new();
    allocated.reserve(allocations as usize);

    let mut rng = StdRng::seed_from_u64(42);
    let mut sample_size = || -> u32 {
        match &dist {
            SizeDistribution::Uniform(u) => return black_box(u.sample(&mut rng)) as u32,
            SizeDistribution::Zipfian(z) => {
                let rank = black_box(z.sample(&mut rng)) as usize;
                return (min_size + (rank - 1)) as u32; // Linear mapping for Zipfian
            }
        }
    };

    b.iter_custom(|iters| {
        let mut total_allocation_time = Duration::new(0, 0);

        for _ in 0..iters {
            allocated.clear();
            let start = Instant::now();
            let mut allocator = A::new(data);
            total_allocation_time += start.elapsed();

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
        }
        total_allocation_time
    });
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let min_size = 128;
    let max_size = 1024;
    let zipfian_exponent = 0.99;

    let distributions = [
        //(
        //    "uniform",
        //    SizeDistribution::Uniform(Uniform::new(min_size, max_size + 1)),
        //),
        (
            "zipfian",
            SizeDistribution::Zipfian(
                ZipfDistribution::new(max_size - min_size + 1, zipfian_exponent).expect(""),
            ),
        ),
    ];

    let allocations = 2_u64.pow(SEGMENT_SIZE_LOG_2 as u32 - 10);
    let deallocations = allocations / 2;

    // Define the allocators to benchmark for allocation
    #[rustfmt::skip]
    let allocator_benchmarks_alloc: Vec<(&'static str, BenchmarkFn)> = generate_allocator_benchmarks!(
        bench_alloc,
        FirstFitScan,
        FirstFitList,
        FirstFitTree,
        NextFitScan,
        NextFitList,
        BestFitScan,
        BestFitList,
        ApproximateBestFitTree,
        BestFitTree,
        WorstFitScan,
        WorstFitList,
        WorstFitTree,
        SegmentAllocator
    );

    for (dist_name, dist) in distributions.clone() {
        let group_name = format!("allocator_alloc_{}_{}", dist_name, SEGMENT_SIZE_LOG_2);
        let mut group = c.benchmark_group(group_name);
        for (bench_name, bench_func) in &allocator_benchmarks_alloc {
            group.bench_function(*bench_name, |b| {
                bench_func(
                    b,
                    dist.clone(),
                    allocations,
                    deallocations,
                    min_size,
                    max_size,
                )
            });
        }
        group.finish();
    }

    // Define the allocators to benchmark for 'new' function time
    let allocator_benchmarks_new: Vec<(&'static str, BenchmarkFn)> = generate_allocator_benchmarks!(
        bench_new,
        FirstFitScan,
        FirstFitList,
        FirstFitTree,
        NextFitScan,
        NextFitList,
        BestFitScan,
        BestFitList,
        ApproximateBestFitTree,
        BestFitTree,
        WorstFitScan,
        WorstFitList,
        WorstFitTree,
        SegmentAllocator
    );

    for (dist_name, dist) in distributions.clone() {
        let group_name = format!("allocator_new_{}_{}", dist_name, SEGMENT_SIZE_LOG_2);
        let mut group = c.benchmark_group(group_name);
        for (bench_name, bench_func) in &allocator_benchmarks_new {
            group.bench_function(*bench_name, |b| {
                bench_func(
                    b,
                    dist.clone(),
                    allocations,
                    deallocations,
                    min_size,
                    max_size,
                )
            });
        }
        group.finish();
    }
}

criterion_group! {
    name = benches;
    // This can be any expression that returns a `Criterion` object.
    config = Criterion::default().sample_size(500).measurement_time(Duration::new(600, 0)).warm_up_time(Duration::new(10, 0));
    targets = criterion_benchmark
}
criterion_main!(benches);
