///! This case implements a checkpoint like writing test in which multiple
///! objects are created on the preferred fastest speed and later migrated
///! downwards once they are no longer needed.
///!
///! A sync is performed after each object batch to ensure that data is safe
///! before continuing.
use betree_perf::*;
use betree_storage_stack::StoragePreference;
use rand::RngCore;
use std::{error::Error, io::Write};
use std::io::BufWriter;

pub fn run(mut client: Client) -> Result<(), Box<dyn Error>> {
    const N_OBJECTS: usize = 5;
    const OBJECT_SIZE_MIB: [u64; N_OBJECTS] = [256, 256, 1, 384, 128];
    const N_GENERATIONS: usize = 70;
    const MIN_WAIT_MS: u64 = 1500;
    const WAIT_RAND_RANGE: u64 = 400;
    println!("running checkpoints");


    let mut stats = BufWriter::new(std::fs::OpenOptions::new().create(true).write(true).open("checkpoints.csv")?);
    stats.write_all(b"generation,size_mib,object_num,time_ms\n")?;
    for gen in 0..N_GENERATIONS {
        let start = std::time::Instant::now();
        let mut accumulated_size = 0;
        for obj_id in 0..N_OBJECTS {
            let key = format!("{gen}_{obj_id}");
            println!("Creating {key}");
            let (obj, _info) = client
                .object_store
                .open_or_create_object_with_pref(key.as_bytes(), StoragePreference::FASTEST)?;
            // We definitely want to write on the fastest layer to minimize
            // waiting inbetween computation.
            let mut cursor = obj.cursor_with_pref(StoragePreference::FASTEST);
            accumulated_size+= OBJECT_SIZE_MIB[obj_id];
            with_random_bytes(
                &mut client.rng,
                OBJECT_SIZE_MIB[obj_id] * 1024 * 1024,
                8 * 1024 * 1024,
                |b| cursor.write_all(b),
            )?;
        }
        stats.write_all(format!("{gen},{accumulated_size},{N_OBJECTS},{}", start.elapsed().as_millis()).as_bytes())?;
        std::thread::sleep(std::time::Duration::from_millis(
            client.rng.next_u64() % WAIT_RAND_RANGE + MIN_WAIT_MS,
        ));
        client.sync().expect("Failed to sync database");
    }
    client.sync().expect("Failed to sync database");
    stats.flush()?;
    Ok(())
}
