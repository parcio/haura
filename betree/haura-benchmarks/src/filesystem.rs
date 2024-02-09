///!
use betree_perf::*;
use betree_storage_stack::vdev::Block;
use betree_storage_stack::StoragePreference;
use rand::{
    distributions::{DistIter, Slice},
    thread_rng, Rng,
};
use std::{error::Error, io::Write, ops::Range};

fn pref(foo: u8, size: Block<u64>, client: &Client) -> StoragePreference {
    let space = client.database.read().free_space_tier();
    match foo {
        0 if Block(space[0].free.0 - size.0) > Block((space[0].total.0 as f64 * 0.2) as u64) => {
            StoragePreference::FASTEST
        }
        1 if Block(space[1].free.0 - size.0) > Block((space[1].total.0 as f64 * 0.2) as u64) => {
            StoragePreference::FAST
        }
        2 if Block(space[2].free.0 - size.0) > Block((space[2].total.0 as f64 * 0.2) as u64) => {
            StoragePreference::SLOW
        }
        3.. => panic!(),
        _ => pref(foo + 1, size, client),
    }
}

// barely, seldom, often
const PROBS: [f64; 3] = [0.01, 0.2, 0.9];

// LANL size reference
const SIZES: [u64; 5] = [
    64 * 1000,
    256 * 1000,
    1 * 1000 * 1000,
    4 * 1000 * 1000,
    1 * 1000 * 1000 * 1000,
];
// Tuple describing the file distribution
const GROUPS_SPEC: [[usize; 5]; 3] = [
    [1022, 256, 1364, 1364, 24],
    [164, 40, 220, 220, 4],
    [12, 4, 16, 16, 2],
];

const TIERS: Range<u8> = 0..3;

pub fn run(mut client: Client) -> Result<(), Box<dyn Error>> {
    println!("running filesystem");
    println!("initialize state");
    let mut groups = vec![];
    let mut counter: u64 = 1;
    let start = std::time::Instant::now();
    for t_id in 0..3 {
        groups.push(vec![]);
        let objs = groups.last_mut().unwrap();
        for (count, size) in GROUPS_SPEC[t_id].iter().zip(SIZES.iter()) {
            for _ in 0..*count {
                let pref = pref(
                    client.rng.gen_range(TIERS),
                    Block::from_bytes(*size),
                    &client,
                );
                let key = format!("key{counter}").into_bytes();
                let (obj, _info) = client
                    .object_store
                    .open_or_create_object_with_pref(&key, pref)?;
                objs.push(key);
                counter += 1;
                let mut cursor = obj.cursor_with_pref(pref);
                with_random_bytes(&mut client.rng, *size, 8 * 1024 * 1024, |b| {
                    cursor.write_all(b)
                })?;
            }
        }
    }

    println!("sync db");
    client.sync().expect("Failed to sync database");

    println!("start conditioning");
    let mut buf = vec![0; 2 * 1024 * 1024 * 1024];
    let mut samplers: Vec<DistIter<_, _, _>> = groups
        .iter()
        .map(|ob| thread_rng().sample_iter(Slice::new(&ob).unwrap()))
        .collect();
    while start.elapsed().as_secs() < 1200 {
        // println!("Reading generation {run} of {RUNS}");
        for (id, prob) in PROBS.iter().enumerate() {
            if client.rng.gen_bool(*prob) {
                let obj = samplers[id].next().unwrap();
                let obj = client.object_store.open_object(obj)?.unwrap();
                obj.read_at(&mut buf, 0).map_err(|e| e.1)?;
            }
        }
    }
    // Allow for some cooldown and migration ending...
    std::thread::sleep(std::time::Duration::from_secs(30));
    println!("sync db");
    client.sync().expect("Failed to sync database");
    // pick certain files which we know are in range, here we pick 3x64KB, 3x256KB, 3x1MB, 3x4MB, 1x1GB
    // Read individual files multiple times to see the cache working?
    const SELECTION: [usize; 5] = [3, 3, 3, 3, 1];
    println!("start measuring");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open("filesystem_measurements.csv")?;
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"key,size,read_latency_ns,write_latency_ns,group\n")?;

    for (n, _) in GROUPS_SPEC.iter().enumerate() {
        for (idx, sel_num) in SELECTION.iter().enumerate() {
            let obj_num = GROUPS_SPEC[n][idx];
            let okstart = obj_key_start(n, idx);
            let okend = okstart + obj_num;
            for _ in 0..*sel_num {
                client.database.read().drop_cache()?;
                let obj_key = format!("key{}", client.rng.gen_range(okstart..=okend));
                let obj = client
                    .object_store
                    .open_object(obj_key.as_bytes())?
                    .expect("Known object could not be opened");
                let start = std::time::Instant::now();
                obj.read_at(&mut buf, 0).map_err(|e| e.1)?;
                let read_time = start.elapsed();
                let size = SIZES[idx];
                let mut cursor = obj.cursor();
                let start = std::time::Instant::now();
                with_random_bytes(&mut client.rng, size, 8 * 1024 * 1024, |b| {
                    cursor.write_all(b)
                })?;
                let write_time = start.elapsed();
                w.write_all(
                    format!(
                        "{obj_key},{size},{},{},{n}\n",
                        read_time.as_nanos(),
                        write_time.as_nanos()
                    )
                    .as_bytes(),
                )?;
                client.sync()?;
                client.database.read().drop_cache()?;
                std::thread::sleep(std::time::Duration::from_secs(20));
            }
        }
    }
    w.flush()?;
    Ok(())
}

fn obj_key_start(tier: usize, group: usize) -> usize {
    let mut tier_offset = 0;
    for idx in 0..tier {
        tier_offset += GROUPS_SPEC[idx].iter().sum::<usize>();
    }
    let group_offset = GROUPS_SPEC[tier].iter().take(group).sum::<usize>();
    tier_offset + group_offset
}
