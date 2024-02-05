///! This file implements a scientific workflow style writing first serial data onto a storage layer and then reading this data from storage in a somewhat
///! random but repeating pattern.
use betree_perf::*;
use betree_storage_stack::StoragePreference;
use rand::Rng;
use std::{
    error::Error,
    io::{Seek, Write},
};

const OBJ_NAME: &[u8] = b"important_research";

pub struct EvaluationConfig {
    pub runtime: u64,
    pub size: u64,
    pub samples: u64,
    pub min_size: u64,
    pub max_size: u64,
}

fn gen_positions(client: &mut Client, config: &EvaluationConfig) -> Vec<(u64, u64)> {
    let mut positions = vec![];
    for _ in 0..config.samples {
        let start = client.rng.gen_range(0..config.size);
        let length = client.rng.gen_range(config.min_size..config.max_size);
        positions.push((start, length.clamp(0, config.size.saturating_sub(start))));
    }
    positions
}

fn prepare_store(client: &mut Client, config: &EvaluationConfig) -> Result<(), Box<dyn Error>> {
    let start = std::time::Instant::now();
    let (obj, _info) = client
        .object_store
        .open_or_create_object_with_pref(b"important_research", StoragePreference::SLOW)?;
    let mut cursor = obj.cursor_with_pref(StoragePreference::SLOW);

    with_random_bytes(&mut client.rng, config.size, 8 * 1024 * 1024, |b| {
        cursor.write_all(b)
    })?;
    println!("Initial write took {}s", start.elapsed().as_secs());
    client.sync().expect("Failed to sync database");
    Ok(())
}

pub fn run_read_write(
    mut client: Client,
    config: EvaluationConfig,
    rw: f64,
    name: &str,
) -> Result<(), Box<dyn Error>> {
    println!("running scientific_evaluation");
    // Generate positions to read
    let positions = gen_positions(&mut client, &config);
    prepare_store(&mut client, &config)?;

    let (obj, _info) = client
        .object_store
        .open_object_with_info(OBJ_NAME)?
        .expect("Object was just created, but can't be opened!");
    let mut cursor = obj.cursor();

    let start = std::time::Instant::now();
    let mut buf = vec![0; config.max_size as usize];
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open("evaluation_{name}.csv")?;
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"offset,size,latency_ns,op\n")?;
    for (pos, len) in positions.iter().cycle() {
        // Read data as may be done in some evaluation where only parts of a
        // database file are read in.
        if client.rng.gen_bool(rw) {
            let t = std::time::Instant::now();
            obj.read_at(&mut buf[..*len as usize], *pos).unwrap();
            w.write_fmt(format_args!("{pos},{len},{},r\n", t.elapsed().as_nanos()))?;
        } else {
            cursor.seek(std::io::SeekFrom::Start(*pos))?;
            let t = std::time::Instant::now();
            with_random_bytes(&mut client.rng, *len, 8 * 1024 * 1024, |b| {
                cursor.write_all(b)
            })?;
            w.write_fmt(format_args!("{pos},{len},{},w\n", t.elapsed().as_nanos()))?;
        }
        if start.elapsed().as_secs() >= config.runtime {
            break;
        }
    }
    w.flush()?;
    Ok(())
}
