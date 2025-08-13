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
//! +----------+--------------+------------------+-------------------------------------------------+
//! | G        | Read         | Uniform          |                                                 |
//! |          |              |                  |                                                 |
//! +----------+--------------+------------------+-------------------------------------------------+
//! +----------+--------------+------------------+-------------------------------------------------+
//! | H        | Update       | Uniform          |                                                 |
//! |          |              |                  |                                                 |
//! +----------+--------------+------------------+-------------------------------------------------+
//! +----------+--------------+------------------+-------------------------------------------------+
//! | I        | Delete       | Uniform          |                                                 |
//! |          |              |                  |                                                 |
//! +----------+--------------+------------------+-------------------------------------------------+

use betree_perf::KvClient;
use rand::distributions::Distribution;
use rand::prelude::SliceRandom;
use rand::{Rng, SeedableRng};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;



// Default in YCSB, 10 x 100 bytes field in one struct.
// const ENTRY_SIZE: usize = 30*1000; // Now passed as parameter
// Default of YCSB
const ZIPF_EXP: f64 = 0.99;

/// A - Update heavy
/// Operations: Read 50%, Update 50%
/// Distribution: Zipfian
/// Application example: Session store recording recent actions in a user session
pub fn a(mut client: KvClient, size: u64, workers: usize, runtime: u64, data_source: &str, data_type: &str, data_path: &str, entry_size: usize) {
    println!("Running YCSB Workload A {} {} {}",size, workers, runtime);
    println!("Filling KV store...");
    
    let mut keys = match data_source {
        "file" => {
            client.fill_entries_from_path(data_path, entry_size as u32)
        }
        _ => {
            // Default to generated data
            client.fill_entries_with_data_type(size / entry_size as u64, entry_size as u32, data_type)
        }
    };
    
    keys.shuffle(client.rng());
    println!("Creating distribution...");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!("ycsb_a.csv"))
        .unwrap();
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"threads,ops,time_ns\n").unwrap();

    //for workers in 1..=threads {
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
                        let mut value = vec![0u8; entry_size];
                        let k = &keys[dist.sample(&mut rng) - 1][..];
                        value = ds.get(k).unwrap().unwrap().to_vec();
                        while let Ok(start) = rx.recv() {
                            while start.elapsed().as_secs() < runtime {
                                for _ in 0..100 {
                                    let k = &keys[dist.sample(&mut rng) - 1][..];
                                    if rng.gen_bool(0.5) {
                                        value = ds.get(k).unwrap().unwrap().to_vec();
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
    //}
}

/// B - Read heavy
/// Operations: Read 95%, Update 5%
/// Distribution: Zipfian
/// Application example: Photo tagging; add a tag is an update, but most operations are to read
/// tags
pub fn b(mut client: KvClient, size: u64, workers: usize, runtime: u64, data_source: &str, data_type: &str, data_path: &str, entry_size: usize) {
    println!("Running YCSB Workload B");
    println!("Filling KV store...");
    
    let mut keys = match data_source {
        "file" => {
            client.fill_entries_from_path(data_path, entry_size as u32)
        }
        _ => {
            // Default to generated data
            client.fill_entries_with_data_type(size / entry_size as u64, entry_size as u32, data_type)
        }
    };
    
    keys.shuffle(client.rng());
    println!("Creating distribution...");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!("ycsb_b.csv"))
        .unwrap();
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"threads,ops,time_ns\n").unwrap();

    //for workers in 1..=threads {
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
                        let mut value = vec![0u8; entry_size];
                        let k = &keys[dist.sample(&mut rng) - 1][..];
                        value = ds.get(k).unwrap().unwrap().to_vec();
                        while let Ok(start) = rx.recv() {
                            while start.elapsed().as_secs() < runtime {
                                for _ in 0..100 {
                                    let k = &keys[dist.sample(&mut rng) - 1][..];
                                    if rng.gen_bool(0.95) {
                                        // 95% reads
                                        value = ds.get(k).unwrap().unwrap().to_vec();
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
    //}
}

/// C - Read heavy
/// Operations: Read 100%
/// Distribution: Zipfian
/// Access Size: 1000 bytes
pub fn c(mut client: KvClient, size: u64, threads: usize, runtime: u64, data_source: &str, data_type: &str, data_path: &str, entry_size: usize) {
    println!("Running YCSB Workload C");
    println!("Filling KV store...");
    
    let mut keys = match data_source {
        "file" => {
            client.fill_entries_from_path(data_path, entry_size as u32)
        }
        _ => {
            // Default to generated data
            client.fill_entries_with_data_type(size / entry_size as u64, entry_size as u32, data_type)
        }
    };
    
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
pub fn d(mut client: KvClient, size: u64, threads: usize, runtime: u64, data_source: &str, data_type: &str, data_path: &str, entry_size: usize) {
    println!("Running YCSB Workload D");
    println!("Filling KV store...");
    // Reserve 20% extra space for new insertions
    
    // Only fill initial portion
    let mut keys = match data_source {
        "file" => {
            client.fill_entries_from_path(data_path, entry_size as u32)
        }
        _ => {
            // Default to generated data
            client.fill_entries_with_data_type(size / entry_size as u64, entry_size as u32, data_type)
        }
    };

    let initial_size =  (keys.len() as f64 * 0.05) as usize;
    let total_size = keys.len();
    println!("{} {}", initial_size, total_size);
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
                        let mut value = vec![0u8; entry_size];

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
                                        //value = ds.get(&keys[idx][..]).unwrap();
                                        if let Some(bytes) = ds.get(&keys[idx][..]).unwrap() {
                                            value = bytes.to_vec();
                                        } else {
                                            // Handle the case where the key isn't found, maybe assign a default or log an error
                                        }
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
pub fn e(mut client: KvClient, size: u64, workers: usize, runtime: u64, data_source: &str, data_type: &str, data_path: &str, entry_size: usize) {
    println!("Running YCSB Workload E");
    println!("Filling KV store...");
    // Reserve 20% extra space for new insertions
    //let initial_size = size / entry_size as u64;
    //let total_size = initial_size + (initial_size / 5);

    // Only fill initial portion
    let mut keys = match data_source {
        "file" => {
            client.fill_entries_from_path(data_path, entry_size as u32)
        }
        _ => {
            // Default to generated data
            client.fill_entries_with_data_type(size / entry_size as u64, entry_size as u32, data_type)
        }
    };
  let initial_size =  keys.len();
    // let total_size = initial_size + (keys.len() as f64 * 0.05) as usize;
    // println!("{} {}", initial_size, total_size);
    // // Fill rest of keys for potential inserts
    // for idx in initial_size..total_size {
    //     let k = keys[idx - initial_size];
    //     keys.push(k);
    // }

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

    //for workers in 1..=threads {
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
                        let mut value = vec![0u8; entry_size];
                        let k = &keys[0][..];
                        value = ds.get(k).unwrap().unwrap().to_vec();
                        while let Ok(start) = rx.recv() {
                            while start.elapsed().as_secs() < runtime {
                                for _ in 0..100 {
                                    if rng.gen_bool(0.95) {
                                        // 95% scans
                                        let max = current_size.load(AtomicOrdering::Relaxed);
let dist = zipf::ZipfDistribution::new(max, ZIPF_EXP).unwrap();
let mut start_idx = dist.sample(&mut rng).saturating_sub(1);

let scan_length = rng.gen_range(1..=100);
let mut end_idx = (start_idx + scan_length).min(max.saturating_sub(1));

// Ensure valid bounds
if start_idx >= keys.len() || end_idx >= keys.len() || start_idx >= end_idx {
    continue; // skip invalid range
}

let start_key = &keys[start_idx][..];
let end_key = &keys[end_idx][..];
                                        // Consume the iterator to actually perform the scan
                                        for _entry in ds.range(start_key..end_key).unwrap() {
                                            if let Ok((_k, _v)) = _entry {
                                                value = _v.to_vec();
                                                break; // exit after the first one
                                            }
                                        }
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
    //}
}

/// F - Read-modify-write
/// Operations: Read 50%, RMW 50%
/// Distribution: Zipfian
/// Application example: user database, where user records are read and modified by the user or to
/// record user activity
pub fn f(mut client: KvClient, size: u64, threads: usize, runtime: u64, data_source: &str, data_type: &str, data_path: &str, entry_size: usize) {
    println!("Running YCSB Workload F");
    println!("Filling KV store...");
    
    let mut keys = match data_source {
        "file" => {
            client.fill_entries_from_path(data_path, entry_size as u32)
        }
        _ => {
            // Default to generated data
            client.fill_entries_with_data_type(size / entry_size as u64, entry_size as u32, data_type)
        }
    };
    
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
                        let mut value = vec![0u8; entry_size];
                        while let Ok(start) = rx.recv() {
                            while start.elapsed().as_secs() < runtime {
                                for _ in 0..100 {
                                    let k = &keys[dist.sample(&mut rng) - 1][..];
                                    if rng.gen_bool(0.5) {
                                        // 50% reads
                                        value = ds.get(k).unwrap().unwrap().to_vec();
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

use rand_xoshiro::Xoshiro256Plus;

pub fn g(mut client: KvClient, size: u64, workers: usize, runtime: u64, data_source: &str, data_type: &str, data_path: &str, entry_size: usize) {
    println!("Running YCSB Workload G");
    println!("Filling KV store...");
    
    let mut keys = match data_source {
        "file" => {
            client.fill_entries_from_path(data_path, entry_size as u32)
        }
        _ => {
            // Default to generated data
            client.fill_entries_with_data_type(size / entry_size as u64, entry_size as u32, data_type)
        }
    };
    
    keys.shuffle(client.rng());

    // Estimate entries per 128KB leaf: value(1000B) + key(8B) + overhead(~8B)
    let approx_entry_size = entry_size + 8 + 8;
    let entries_per_leaf = (1024*1024) / approx_entry_size; // ≈ 128 entries
    println!("Estimated entries per leaf: {entries_per_leaf}");

    // Pick one key per estimated leaf node
    let mut leaf_sampled_keys = Vec::new();
    for i in (0..keys.len()).step_by(entries_per_leaf) {
        leaf_sampled_keys.push(keys[i]);
    }

    //client.ds.flush().unwrap();

    // Shuffle the reduced key set
    //leaf_sampled_keys.shuffle(client.rng());
    println!("Sampled {} keys from {} total keys", leaf_sampled_keys.len(), keys.len());

    println!("Creating distribution...");
    
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open("ycsb_g.csv")
        .unwrap();
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"threads,ops,time_ns\n").unwrap();


    //for workers in [1, 5, 10, 15, 20, 25] {
        let threads = (0..workers)
            .map(|_| std::sync::mpsc::channel::<std::time::Instant>())
            .enumerate()
            .map(|(id, (tx, rx))| {
                let _keys = keys.clone();
                let ds = client.ds.clone();
                (
                    std::thread::spawn(move || {
                        let mut rng = Xoshiro256Plus::seed_from_u64(id as u64);
                        let mut total = 0;
                        while let Ok(start) = rx.recv() {
                            while start.elapsed().as_secs() < 20 {
                                for _ in 0..500 {
                                    if let Some(k) = _keys.choose(&mut rng) {
                                        if let Some(value) = ds.get(*k).unwrap() {
                                            total += 1;
                                        }
                                    }
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
    //}
}

use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::Path;

pub fn read_folder_chunks<P: AsRef<Path>>(folder_path: P, chunk_size: usize) -> Vec<Vec<u8>> {
    let mut all_chunks = Vec::new();

    for entry in fs::read_dir(folder_path).expect("Failed to read folder") {
        let entry = entry.expect("Invalid entry");
        let path = entry.path();

        if path.is_file() {
            let file = File::open(&path).expect("Unable to open file");
            let mut reader = BufReader::new(file);

            loop {
                let mut buffer = vec![0u8; chunk_size];
                let bytes_read = reader.read(&mut buffer).expect("Read error");
                if bytes_read == 0 {
                    break;
                }

                buffer.truncate(bytes_read);
                all_chunks.push(buffer);
            }
        }
    }

    all_chunks
}


pub fn h(mut client: KvClient, size: u64, threads: usize, runtime: u64, data_source: &str, data_type: &str, data_path: &str, entry_size: usize) {
    println!("Running YCSB Workload H");
    println!("Filling KV store...");
    
    let mut keys = match data_source {
        "file" => {
            client.fill_entries_from_path(data_path, entry_size as u32)
        }
        _ => {
            // Default to generated data
            client.fill_entries_with_data_type(size / entry_size as u64, entry_size as u32, data_type)
        }
    };

    keys.shuffle(client.rng());
    println!("Creating distribution...");

    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open("ycsb_h.csv")
        .unwrap();
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"threads,ops,time_ns\n").unwrap();

    let corpus_chunks = read_folder_chunks("/home/skarim/Code/smash/haura/betree/haura-benchmarks/silesia_corpus", 1024);
    let chunk_data = std::sync::Arc::new(corpus_chunks); // Share across threads


    for workers in 1..=threads {
        let threads = (0..workers)
            .map(|_| std::sync::mpsc::channel::<std::time::Instant>())
            .enumerate()
            .map(|(id, (tx, rx))| {
                let keys = keys.clone();
                let ds = client.ds.clone();
                 let chunks = chunk_data.clone(); // shared chunk vector
                (
                    std::thread::spawn(move || {
                        use rand::seq::SliceRandom; // Add this if it's not already imported
                        let mut rng = rand_xoshiro::Xoshiro256Plus::seed_from_u64(id as u64);

                        let mut shuffled_keys = keys.clone(); // Clone to keep original list intact
                        shuffled_keys.shuffle(&mut rng);
                        let mut total = 0;
                        
                        let mut idx = 0;

                        while let Ok(start) = rx.recv() {
                            while start.elapsed().as_secs() < runtime {
                                for jdx in 0..1000 {
                                    let k = &shuffled_keys[jdx + idx];
                                    let chunk_idx = (jdx + idx) % chunks.len();
                                let value = &chunks[chunk_idx];
                                    ds.upsert(k.to_vec(), &value, 0).unwrap();  // **Only Write**
                                    total += 1;
                                }
                            }

                            if idx + 1000 >= shuffled_keys.len(){
                                idx = 0;
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


pub fn i(mut client: KvClient, size: u64, threads: usize, runtime: u64, data_source: &str, data_type: &str, data_path: &str, entry_size: usize) {
   println!("Running YCSB Workload I");
    println!("Filling KV store...");
    
    let mut keys = match data_source {
        "file" => {
            client.fill_entries_from_path(data_path, entry_size as u32)
        }
        _ => {
            // Default to generated data
            client.fill_entries_with_data_type(size / entry_size as u64, entry_size as u32, data_type)
        }
    };

    keys.shuffle(client.rng());
    println!("Creating distribution...");

    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open("ycsb_i.csv")
        .unwrap();
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"threads,ops,time_ns\n").unwrap();

    for workers in 1..=threads {
        let threads = (0..workers)
            .map(|_| std::sync::mpsc::channel::<std::time::Instant>())
            .enumerate()
            .map(|(id, (tx, rx))| {
                let keys = keys.clone();
                let ds = client.ds.clone();
                //let value = vec![0u8; entry_size];
                (
                    std::thread::spawn(move || {
                        use rand::seq::SliceRandom; // Add this if it's not already imported
                        let mut rng = rand_xoshiro::Xoshiro256Plus::seed_from_u64(id as u64);

                        let mut shuffled_keys = keys.clone(); // Clone to keep original list intact
                        shuffled_keys.shuffle(&mut rng);
                        let mut total = 0;
                        
                        let mut idx = 0;

                        while let Ok(start) = rx.recv() {
                            while start.elapsed().as_secs() < runtime {
                                for jdx in 0..1000 {
                                    let k = &shuffled_keys[jdx + idx];
                                    ds.delete(k.to_vec()).unwrap();  // **Only Write**
                                    total += 1;
                                }
                            }

                            if idx + 1000 >= shuffled_keys.len(){
                                idx = 0;
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
