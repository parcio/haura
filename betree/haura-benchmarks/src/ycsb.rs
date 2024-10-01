//! Benchmarks based on the YCSB-{A,B,C,D,E,F} workloads.
//!
//! Link: https://web.archive.org/web/20170809211159id_/http://www.cs.toronto.edu/~delara/courses/csc2231/papers/cooper.pdf
//!
//! +----------+--------------+------------------+-------------------------------------------------+
//! | Workload | Operations   | Record selection | Application example                             |
//! +----------+--------------+------------------+-------------------------------------------------+
//! | A        | Read: 50%    | Zipfian          | Session store recording recent actions in a user|
//! |          | Update: 50%  |                  | session                                         |
//! +----------+--------------+------------------+-------------------------------------------------+
//! | B        | Read: 95%    | Zipfian          | Photo tagging; add a tag is an update, but most |
//! |          | Update: 5%   |                  | operations are to read tags                     |
//! +----------+--------------+------------------+-------------------------------------------------+
//! | C        | Read: 100%   | Zipfian          | User profile cache, where profiles are          |
//! |          |              |                  | constructed elsewhere (e.g., Hadoop)            |
//! +----------+--------------+------------------+-------------------------------------------------+
//! | D        | Read: 95%    | Latest           | User status updates; people want to read the    |
//! |          | Insert: 5%   |                  | latest statuses                                 |
//! +----------+--------------+------------------+-------------------------------------------------+
//! | E        | Scan: 95%    | Zipfian/Uniform* | Threaded conversations, where each scan is for  |
//! |          | Insert: 5%   |                  | the posts in a given thread (assumed to be      |
//! |          |              |                  | clustered by thread id)                         |
//! +----------+--------------+------------------+-------------------------------------------------+
//! | F        | Read-modify- | Varies           | Read a record, modify it, and write it back     |
//! |          | write        |                  |                                                 |
//! +----------+--------------+------------------+-------------------------------------------------+

use betree_perf::KvClient;
use rand::distributions::Distribution;
use rand::prelude::SliceRandom;
use rand::{Rng, SeedableRng};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

// Default in YCSB, 10 x 100 bytes field in one struct.
const ENTRY_SIZE: usize = 1000;
// Default of YCSB
const ZIPF_EXP: f64 = 0.99;

/// A - Update heavy
/// Operations: Read 50%, Update 50%
/// Distribution: Zipfian
/// Application example: Session store recording recent actions in a user session
pub fn a(mut client: KvClient, size: u64, threads: usize, runtime: u64) {
    println!("Running YCSB Workload A");
    println!("Filling KV store...");
    let mut keys = client.fill_entries(size / ENTRY_SIZE as u64, ENTRY_SIZE as u32);
    keys.shuffle(client.rng());
    println!("Creating distribution...");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!("ycsb_a.csv"))
        .unwrap();
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"threads,ops,time_ns\n").unwrap();

    for workers in 1..=threads {
        println!("Running benchmark with {workers} threads...");
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
                        let value = vec![0u8; ENTRY_SIZE];
                        while let Ok(start) = rx.recv() {
                            while start.elapsed().as_secs() < runtime {
                                for _ in 0..100 {
                                    let k = &keys[dist.sample(&mut rng) - 1][..];
                                    if rng.gen_bool(0.5) {
                                        ds.get(k).unwrap().unwrap();
                                    } else {
                                        ds.upsert(k.to_vec(), &value, 0).unwrap();
                                    }
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
        client.db.read().drop_cache().unwrap();
        let start = std::time::Instant::now();
        for (_t, tx) in threads.iter() {
            tx.send(start).unwrap();
        }
        let mut total = 0;
        for (t, tx) in threads.into_iter() {
            drop(tx);
            total += t.join().unwrap();
        }
        let end = start.elapsed();
        w.write_fmt(format_args!("{workers},{total},{}\n", end.as_nanos()))
            .unwrap();
        w.flush().unwrap();
        println!("Achieved: {} ops/sec", total as f32 / end.as_secs_f32());
        println!("          {} ns avg", end.as_nanos() / total);
    }
}

/// B - Read heavy
/// Operations: Read 95%, Update 5%
/// Distribution: Zipfian
/// Application example: Photo tagging; add a tag is an update, but most operations are to read
/// tags
pub fn b(mut client: KvClient, size: u64, threads: usize, runtime: u64) {
    println!("Running YCSB Workload B");
    println!("Filling KV store...");
    let mut keys = client.fill_entries(size / ENTRY_SIZE as u64, ENTRY_SIZE as u32);
    keys.shuffle(client.rng());
    println!("Creating distribution...");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!("ycsb_b.csv"))
        .unwrap();
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"threads,ops,time_ns\n").unwrap();

    for workers in 1..=threads {
        println!("Running benchmark with {workers} threads...");
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
                        let value = vec![0u8; ENTRY_SIZE];
                        while let Ok(start) = rx.recv() {
                            while start.elapsed().as_secs() < runtime {
                                for _ in 0..100 {
                                    let k = &keys[dist.sample(&mut rng) - 1][..];
                                    if rng.gen_bool(0.95) {
                                        // 95% reads
                                        ds.get(k).unwrap().unwrap();
                                    } else {
                                        // 5% updates
                                        ds.upsert(k.to_vec(), &value, 0).unwrap();
                                    }
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
        client.db.read().drop_cache().unwrap();
        let start = std::time::Instant::now();
        for (_t, tx) in threads.iter() {
            tx.send(start).unwrap();
        }
        let mut total = 0;
        for (t, tx) in threads.into_iter() {
            drop(tx);
            total += t.join().unwrap();
        }
        let end = start.elapsed();
        w.write_fmt(format_args!("{workers},{total},{}\n", end.as_nanos()))
            .unwrap();
        w.flush().unwrap();
        println!("Achieved: {} ops/sec", total as f32 / end.as_secs_f32());
        println!("          {} ns avg", end.as_nanos() / total);
    }
}

/// C - Read heavy
/// Operations: Read 100%
/// Distribution: Zipfian
/// Access Size: 1000 bytes
pub fn c(mut client: KvClient, size: u64, threads: usize, runtime: u64) {
    println!("Running YCSB Workload C");
    println!("Filling KV store...");
    let mut keys = client.fill_entries(size / ENTRY_SIZE as u64, ENTRY_SIZE as u32);
    keys.shuffle(client.rng());
    println!("Creating distribution...");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!("ycsb_c.csv"))
        .unwrap();
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"threads,ops,time_ns\n").unwrap();

    for workers in 1..=threads {
        println!("Running benchmark with {workers} threads...");
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
                            while start.elapsed().as_secs() < runtime {
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
        client.db.read().drop_cache().unwrap();
        let start = std::time::Instant::now();
        for (_t, tx) in threads.iter() {
            tx.send(start).unwrap();
        }
        let mut total = 0;
        for (t, tx) in threads.into_iter() {
            drop(tx);
            total += t.join().unwrap();
        }
        let end = start.elapsed();
        w.write_fmt(format_args!("{workers},{total},{}\n", end.as_nanos()))
            .unwrap();
        w.flush().unwrap();
        println!("Achieved: {} ops/sec", total as f32 / end.as_secs_f32());
        println!("          {} ns avg", end.as_nanos() / total);
    }
}

/// D - Read latest
/// Operations: Read 95%, Insert 5%
/// Distribution: Latest
/// Application example: User status updates; people want to read the latest statuses
pub fn d(mut client: KvClient, size: u64, threads: usize, runtime: u64) {
    println!("Running YCSB Workload D");
    println!("Filling KV store...");
    // Reserve 20% extra space for new insertions
    let initial_size = size / ENTRY_SIZE as u64;
    let total_size = initial_size + (initial_size / 5);

    // Only fill initial portion
    let mut keys = client.fill_entries(initial_size, ENTRY_SIZE as u32);

    // Fill rest of keys
    for idx in initial_size..total_size {
        let k = (idx as u64).to_be_bytes();
        keys.push(k);
    }

    // Do not shuffle because we want to have the most recent keys at the back
    // keys.shuffle(client.rng());

    println!("Creating distribution...");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!("ycsb_d.csv"))
        .unwrap();
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"threads,ops,time_ns\n").unwrap();

    // Create thread-safe current_size
    let current_size = Arc::new(AtomicUsize::new(initial_size as usize));

    for workers in 1..=threads {
        println!("Running benchmark with {workers} threads...");
        let threads = (0..workers)
            .map(|_| std::sync::mpsc::channel::<std::time::Instant>())
            .enumerate()
            .map(|(id, (tx, rx))| {
                let keys = keys.clone();
                let ds = client.ds.clone();
                let current_size = Arc::clone(&current_size);
                (
                    std::thread::spawn(move || {
                        let mut rng = rand_xoshiro::Xoshiro256Plus::seed_from_u64(id as u64);
                        let mut total = 0;
                        let value = vec![0u8; ENTRY_SIZE];

                        while let Ok(start) = rx.recv() {
                            while start.elapsed().as_secs() < runtime {
                                for _ in 0..100 {
                                    if rng.gen_bool(0.95) {
                                        // 95% reads using skewed latest distribution
                                        let max = current_size.load(AtomicOrdering::Relaxed);
                                        // Generate zipfian value and subtract from max to favor recent items
                                        let dist =
                                            zipf::ZipfDistribution::new(max, ZIPF_EXP).unwrap();
                                        let offset = dist.sample(&mut rng);
                                        let idx = max.saturating_sub(offset);
                                        ds.get(&keys[idx][..]).unwrap();
                                    } else {
                                        // 5% inserts of new records
                                        let current = current_size.load(AtomicOrdering::Relaxed);
                                        if current < keys.len() {
                                            ds.insert(keys[current].to_vec(), &value).unwrap();
                                            current_size.fetch_add(1, AtomicOrdering::Relaxed);
                                        }
                                    }
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
        client.db.read().drop_cache().unwrap();
        let start = std::time::Instant::now();
        for (_t, tx) in threads.iter() {
            tx.send(start).unwrap();
        }
        let mut total = 0;
        for (t, tx) in threads.into_iter() {
            drop(tx);
            total += t.join().unwrap();
        }
        let end = start.elapsed();
        w.write_fmt(format_args!("{workers},{total},{}\n", end.as_nanos()))
            .unwrap();
        w.flush().unwrap();
        println!("Achieved: {} ops/sec", total as f32 / end.as_secs_f32());
        println!("          {} ns avg", end.as_nanos() / total);
    }
}

/// E - Short Ranges
/// Operations: Scan 95%, Insert 5%
/// Distribution: Zipfian for first key, Uniform for length
/// Application example: Threaded conversations, where each scan is for the posts in a given thread
/// (assumed to be clustered by thread id)
pub fn e(mut client: KvClient, size: u64, threads: usize, runtime: u64) {
    println!("Running YCSB Workload E");
    println!("Filling KV store...");
    // Reserve 20% extra space for new insertions
    let initial_size = size / ENTRY_SIZE as u64;
    let total_size = initial_size + (initial_size / 5);

    // Only fill initial portion
    let mut keys = client.fill_entries(initial_size, ENTRY_SIZE as u32);

    // Fill rest of keys for potential inserts
    for idx in initial_size..total_size {
        let k = (idx as u64).to_be_bytes();
        keys.push(k);
    }

    println!("Creating distribution...");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!("ycsb_e.csv"))
        .unwrap();
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"threads,ops,time_ns\n").unwrap();

    // Thread-safe current size tracking
    let current_size = Arc::new(AtomicUsize::new(initial_size as usize));

    for workers in 1..=threads {
        println!("Running benchmark with {workers} threads...");
        let threads = (0..workers)
            .map(|_| std::sync::mpsc::channel::<std::time::Instant>())
            .enumerate()
            .map(|(id, (tx, rx))| {
                let keys = keys.clone();
                let ds = client.ds.clone();
                let current_size = Arc::clone(&current_size);
                (
                    std::thread::spawn(move || {
                        let mut rng = rand_xoshiro::Xoshiro256Plus::seed_from_u64(id as u64);
                        let mut total = 0;
                        let value = vec![0u8; ENTRY_SIZE];

                        while let Ok(start) = rx.recv() {
                            while start.elapsed().as_secs() < runtime {
                                for _ in 0..100 {
                                    if rng.gen_bool(0.95) {
                                        // 95% scans
                                        let max = current_size.load(AtomicOrdering::Relaxed);
                                        // Get start key using zipfian
                                        let dist =
                                            zipf::ZipfDistribution::new(max, ZIPF_EXP).unwrap();
                                        let start_idx = dist.sample(&mut rng) - 1;

                                        // Uniform random scan length between 1 and 100
                                        let scan_length = rng.gen_range(1..=100);
                                        let end_idx = (start_idx + scan_length).min(max - 1);

                                        // Perform the range scan
                                        let start_key = &keys[start_idx][..];
                                        let end_key = &keys[end_idx][..];
                                        // Consume the iterator to actually perform the scan
                                        for _entry in ds.range(start_key..end_key).unwrap() {}
                                    } else {
                                        // 5% inserts of new records
                                        let current = current_size.load(AtomicOrdering::Relaxed);
                                        if current < keys.len() {
                                            ds.insert(keys[current].to_vec(), &value).unwrap();
                                            current_size.fetch_add(1, AtomicOrdering::Relaxed);
                                        }
                                    }
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
        client.db.read().drop_cache().unwrap();
        let start = std::time::Instant::now();
        for (_t, tx) in threads.iter() {
            tx.send(start).unwrap();
        }
        let mut total = 0;
        for (t, tx) in threads.into_iter() {
            drop(tx);
            total += t.join().unwrap();
        }
        let end = start.elapsed();
        w.write_fmt(format_args!("{workers},{total},{}\n", end.as_nanos()))
            .unwrap();
        w.flush().unwrap();
        println!("Achieved: {} ops/sec", total as f32 / end.as_secs_f32());
        println!("          {} ns avg", end.as_nanos() / total);
    }
}

/// F - Read-modify-write
/// Operations: Read 50%, RMW 50%
/// Distribution: Zipfian
/// Application example: user database, where user records are read and modified by the user or to
/// record user activity
pub fn f(mut client: KvClient, size: u64, threads: usize, runtime: u64) {
    println!("Running YCSB Workload F");
    println!("Filling KV store...");
    let mut keys = client.fill_entries(size / ENTRY_SIZE as u64, ENTRY_SIZE as u32);
    keys.shuffle(client.rng());
    println!("Creating distribution...");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!("ycsb_f.csv"))
        .unwrap();
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"threads,ops,time_ns\n").unwrap();

    for workers in 1..=threads {
        println!("Running benchmark with {workers} threads...");
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
                        let value = vec![0u8; ENTRY_SIZE];
                        while let Ok(start) = rx.recv() {
                            while start.elapsed().as_secs() < runtime {
                                for _ in 0..100 {
                                    let k = &keys[dist.sample(&mut rng) - 1][..];
                                    if rng.gen_bool(0.5) {
                                        // 50% reads
                                        ds.get(k).unwrap().unwrap();
                                    } else {
                                        // 50% read-modify-write
                                        let _existing = ds.get(k).unwrap().unwrap();
                                        // Modify the value (in real workloads this would be an actual modification)
                                        ds.upsert(k.to_vec(), &value, 0).unwrap();
                                    }
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
        client.db.read().drop_cache().unwrap();
        let start = std::time::Instant::now();
        for (_t, tx) in threads.iter() {
            tx.send(start).unwrap();
        }
        let mut total = 0;
        for (t, tx) in threads.into_iter() {
            drop(tx);
            total += t.join().unwrap();
        }
        let end = start.elapsed();
        w.write_fmt(format_args!("{workers},{total},{}\n", end.as_nanos()))
            .unwrap();
        w.flush().unwrap();
        println!("Achieved: {} ops/sec", total as f32 / end.as_secs_f32());
        println!("          {} ns avg", end.as_nanos() / total);
    }
}
