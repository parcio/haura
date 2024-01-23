///! This file implements a scientific workflow style writing first serial data onto a storage layer and then reading this data from storage in a somewhat
///! random but repeating pattern.
use betree_perf::*;
use betree_storage_stack::StoragePreference;
use rand::Rng;
use std::{error::Error, io::{Write, Seek}};

const OBJECT_SIZE: u64 = 25 * 1024 * 1024 * 1024;
const MIN_FETCH_SIZE: u64 = 1 * 1024;
const MAX_FETCH_SIZE: u64 = 12 * 1024 * 1024;
const N_POSITIONS: u64 = 1024; // E(sum N_POSITIONS_size) = 3GiB
const OBJ_NAME: &[u8] = b"important_research";

fn gen_positions(client: &mut Client) -> Vec<(u64, u64)> {
    let mut positions = vec![];
    for _ in 0..N_POSITIONS {
        let start = client.rng.gen_range(0..OBJECT_SIZE);
        let length = client.rng.gen_range(MIN_FETCH_SIZE..MAX_FETCH_SIZE);
        positions.push((
            start,
            length.clamp(0, OBJECT_SIZE.saturating_sub(start)),
        ));
    }
    positions
}

fn prepare_store(client: &mut Client) -> Result<(), Box<dyn Error>> {
    let start = std::time::Instant::now();
    let (obj, _info) = client
        .object_store
        .open_or_create_object_with_pref(b"important_research", StoragePreference::SLOW)?;
    let mut cursor = obj.cursor_with_pref(StoragePreference::SLOW);

    with_random_bytes(&mut client.rng, OBJECT_SIZE, 8 * 1024 * 1024, |b| {
        cursor.write_all(b)
    })?;
    println!("Initial write took {}s", start.elapsed().as_secs());
    client.sync().expect("Failed to sync database");
    Ok(())
}

pub fn run_read(mut client: Client, runtime: u64) -> Result<(), Box<dyn Error>> {
    println!("running scientific_evaluation");
    // Generate positions to read
    let positions = gen_positions(&mut client);
    prepare_store(&mut client)?;

    let (obj, _info) = client
        .object_store
        .open_object_with_info(OBJ_NAME)?
        .expect("Object was just created, but can't be opened!");

    let start = std::time::Instant::now();
    let mut buf = vec![0; MAX_FETCH_SIZE as usize];
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open("evaluation_read.csv")?;
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"offset,size,latency_ns\n")?;
    for (pos, len) in positions.iter().cycle() {
        // Read data as may be done in some evaluation where only parts of a
        // database file are read in.
        let t = std::time::Instant::now();
        obj.read_at(&mut buf[..*len as usize], *pos).unwrap();
        w.write_fmt(format_args!("{pos},{len},{}", t.elapsed().as_nanos()))?;
        if start.elapsed().as_secs() >= runtime {
            break;
        }
    }
    w.flush()?;
    Ok(())
}

pub fn run_read_write(mut client: Client, runtime: u64) -> Result<(), Box<dyn Error>> {
    println!("running scientific_evaluation");
    // Generate positions to read
    let positions = gen_positions(&mut client);
    prepare_store(&mut client)?;

    let (obj, _info) = client
        .object_store
        .open_object_with_info(OBJ_NAME)?
        .expect("Object was just created, but can't be opened!");
    let mut cursor = obj.cursor();

    let start = std::time::Instant::now();
    let mut buf = vec![0; MAX_FETCH_SIZE as usize];
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open("evaluation_read_write.csv")?;
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"offset,size,latency_ns,op\n")?;
    for (pos, len) in positions.iter().cycle() {
        // Read data as may be done in some evaluation where only parts of a
        // database file are read in.
        if client.rng.gen_bool(0.5) {
            let t = std::time::Instant::now();
            obj.read_at(&mut buf[..*len as usize], *pos).unwrap();
            w.write_fmt(format_args!("{pos},{len},{},r", t.elapsed().as_nanos()))?;
        } else {
            cursor.seek(std::io::SeekFrom::Start(*pos))?;
            let t = std::time::Instant::now();
            with_random_bytes(&mut client.rng, *len, 8 * 1024 * 1024, |b| {
                cursor.write_all(b)
            })?;
            w.write_fmt(format_args!("{pos},{len},{},w", t.elapsed().as_nanos()))?;
        }
        if start.elapsed().as_secs() >= runtime {
            break;
        }
    }
    w.flush()?;
    Ok(())
}
