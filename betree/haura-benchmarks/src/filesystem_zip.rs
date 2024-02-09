///!
use betree_perf::*;
use betree_storage_stack::vdev::Block;
use betree_storage_stack::StoragePreference;
use rand::{
    distributions::{DistIter, Slice},
    seq::SliceRandom,
    thread_rng, Rng,
};
use std::{
    error::Error,
    io::{Read, Write},
    ops::Range,
    path::Path,
};

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

const TIERS: Range<u8> = 0..3;

const GROUPS: [f32; 3] = [0.8, 0.15, 0.05];
const NUM_SAMPLE: usize = 50;

pub fn run(mut client: Client, zip_path: impl AsRef<Path>) -> Result<(), Box<dyn Error>> {
    println!("running filesystem");
    println!("initialize state");

    let file = std::fs::OpenOptions::new().read(true).open(zip_path)?;
    let mut zip = zip::ZipArchive::new(file).unwrap();

    // Create objects
    let start = std::time::Instant::now();

    // use expandable vector
    let mut buf = Vec::new();
    let file_names = zip
        .file_names()
        .map(|n| n.to_string())
        .collect::<Vec<String>>();
    let mut file_name_with_size = vec![];
    let file_num = file_names.len();
    for file in file_names.into_iter() {
        // Read each file and insert randomly as new object into the object store
        let mut zfile = zip.by_name(&file).unwrap();
        zfile.read_to_end(&mut buf)?;
        let size = zfile.compressed_size();
        let pref = pref(
            client.rng.gen_range(TIERS),
            Block::from_bytes(size),
            &client,
        );
        let (obj, _) = client
            .object_store
            .open_or_create_object_with_pref(file.as_bytes(), pref)?;
        obj.write_at_with_pref(&buf, 0, pref).map_err(|e| e.1)?;
        file_name_with_size.push((file, size));
        buf.clear();
    }

    // Create groups
    let mut groups: [Vec<(String, u64)>; 3] = [0; 3].map(|_| vec![]);
    let mut distributed = 0usize;
    file_name_with_size.shuffle(&mut client.rng);
    for (id, part) in GROUPS.iter().enumerate() {
        let num = (part * file_num as f32) as usize;
        groups[id] = file_name_with_size[distributed..(distributed + num)].to_vec();
        distributed += num;
    }

    println!("sync db");
    client.sync().expect("Failed to sync database");

    println!("start conditioning");
    let mut buf = vec![0; 5 * 1024 * 1024 * 1024];
    let mut samplers: Vec<DistIter<_, _, _>> = groups
        .iter()
        .map(|ob| thread_rng().sample_iter(Slice::new(&ob).unwrap()))
        .collect();
    while start.elapsed().as_secs() < 1200 {
        // println!("Reading generation {run} of {RUNS}");
        for (id, prob) in PROBS.iter().enumerate() {
            if client.rng.gen_bool(*prob) {
                let obj = samplers[id].next().unwrap();
                let obj = client.object_store.open_object(obj.0.as_bytes())?.unwrap();
                obj.read_at(&mut buf, 0).map_err(|e| e.1)?;
            }
        }
    }
    // Allow for some cooldown and migration ending...
    std::thread::sleep(std::time::Duration::from_secs(30));
    println!("sync db");
    client.sync().expect("Failed to sync database");
    // // pick certain files which we know are in range, here we pick 3x64KB, 3x256KB, 3x1MB, 3x4MB, 1x1GB
    // // Read individual files multiple times to see the cache working?
    // const SELECTION: [usize; 5] = [3, 3, 3, 3, 1];
    println!("start measuring");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open("filesystem_zip_measurements.csv")?;
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"key,size,read_latency_ns,write_latency_ns,group\n")?;

    for (idx, sampler) in groups.iter().enumerate() {
        for _ in 0..NUM_SAMPLE {
            client.database.read().drop_cache()?;
            let (obj_key, size) = sampler.choose(&mut client.rng).unwrap();
            let obj = client
                .object_store
                .open_object(obj_key.as_bytes())?
                .expect("Known object could not be opened");
            let start = std::time::Instant::now();
            obj.read_at(&mut buf, 0).map_err(|e| e.1)?;
            let read_time = start.elapsed();
            let mut cursor = obj.cursor();
            let start = std::time::Instant::now();
            with_random_bytes(&mut client.rng, *size, 8 * 1024 * 1024, |b| {
                cursor.write_all(b)
            })?;
            let write_time = start.elapsed();
            w.write_all(
                format!(
                    "{obj_key},{size},{},{},{idx}\n",
                    read_time.as_nanos(),
                    write_time.as_nanos()
                )
                .as_bytes(),
            )?;
            client.sync()?;
            client.database.read().drop_cache()?;
            std::thread::sleep(std::time::Duration::from_secs(5));
        }
    }
    w.flush()?;
    Ok(())
}
