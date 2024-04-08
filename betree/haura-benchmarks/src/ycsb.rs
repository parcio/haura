//! Benchmarks based on the YCSB-{A,B,C,D,E} workloads.
//!
//! Link: https://web.archive.org/web/20170809211159id_/http://www.cs.toronto.edu/~delara/courses/csc2231/papers/cooper.pdf

use betree_perf::KvClient;
use rand::{Rng, SeedableRng};

// Default in YCSB, 10 x 100 bytes field in one struct.
const ENTRY_SIZE: usize = 1000;
// Default of YCSB
const ZIPF_EXP: f64 = 0.99;

/// A - Update heavy
/// Operations: Read: 50%, Update 50%
/// Distribution: Zipfian
/// Application example: Session store recording recent actions in a user session
pub fn A() {}

use rand::distributions::Distribution;
use std::io::Write;

/// C - Read heavy
/// Operations: Read 100%
/// Distribution: Zipfian
/// Application example: User profile cache, where profiles are constructed elsewhere (e.g., Hadoop)
pub fn C(mut client: KvClient, size: u64) {
    println!("Running YCSB Workload C");
    println!("Filling KV store...");
    let keys = client.fill_entries(size / ENTRY_SIZE as u64, ENTRY_SIZE as u32);
    println!("Creating distribution...");
    let dist = zipf::ZipfDistribution::new(keys.len(), ZIPF_EXP).unwrap();
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!("ycsb_c.csv"))
        .unwrap();
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"latency_ns,op\n").unwrap();

    let threads = 8;
    for workers in 1..=threads {
        println!("Running benchmark with {workers} threads...");
        let mut rng = client.rng().clone();
        let mut total = 0;

        let threads = (0..workers)
            .map(|_| std::sync::mpsc::channel::<std::time::Instant>())
            .enumerate()
            .map(|(id, (tx, rx))| {
                let keys = keys.clone();
                let ds = client.ds.clone();
                (
                    std::thread::spawn(move || {
                        let mut rng = rand_xoshiro::Xoshiro256Plus::seed_from_u64(id as u64);
                        let dist = zipf::ZipfDistribution::new(keys.len(), ZIPF_EXP).unwrap();
                        let mut total = 0;
                        while let Ok(start) = rx.recv() {
                            while start.elapsed().as_secs() < 50 {
                                for _ in 0..100 {
                                    ds.get(&keys[dist.sample(&mut rng) - 1][..])
                                        .unwrap()
                                        .unwrap();
                                    total += 1;
                                }
                            }
                        }
                        total
                    }),
                    tx,
                )
            })
            .collect::<Vec<_>>();
        let start = std::time::Instant::now();
        for (t, tx) in threads.iter() {
            tx.send(start).unwrap();
        }
        let mut total = 0;
        for (t, tx) in threads.into_iter() {
            drop(tx);
            total += t.join().unwrap();
        }
        let end = start.elapsed();
        println!("Achieved: {} ops/sec", total as f32 / end.as_secs_f32());
        println!("          {} ns avg", end.as_nanos() / total);
    }
}
