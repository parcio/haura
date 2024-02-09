use betree_perf::*;
use betree_storage_stack::StoragePreference;
use std::{
    error::Error,
    io::{self, Seek, SeekFrom, Write},
};

pub fn run(client: &mut Client, part_count: u64, part_size: u64) -> Result<(), Box<dyn Error>> {
    println!("running switchover::run");

    // SLOWEST will fail in our config, so it serves as an early-failure mechanism for unset prefs
    let (obj, _info) = client
        .object_store
        .open_or_create_object_with_pref(b"obj", StoragePreference::SLOWEST)?;
    let mut cursor = obj.cursor();

    for oddness in [0, 1] {
        cursor.seek(SeekFrom::Start(0))?;

        for part_idx in 0..part_count {
            cursor.set_storage_preference(if part_idx % 2 == oddness {
                StoragePreference::FASTEST
            } else {
                StoragePreference::FAST
            });
            with_random_bytes(
                &mut client.rng,
                part_size,
                part_size.min(128 * 1024) as usize,
                |b| cursor.write_all(b),
            )?;
        }

        client.sync().expect("Failed to sync database");

        cursor.seek(SeekFrom::Start(0))?;
        io::copy(&mut cursor, &mut io::sink())?;
    }

    Ok(())
}
