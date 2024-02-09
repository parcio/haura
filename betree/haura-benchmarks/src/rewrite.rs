use betree_perf::*;
use betree_storage_stack::StoragePreference;
use std::{
    error::Error,
    io::{Seek, SeekFrom, Write},
};

pub fn run(
    client: &mut Client,
    object_size: u64,
    rewrite_count: u64,
) -> Result<(), Box<dyn Error>> {
    println!("running rewrite::run");

    // SLOWEST will fail in our config, so it serves as an early-failure mechanism for unset prefs
    let (obj, _info) = client
        .object_store
        .open_or_create_object_with_pref(b"obj", StoragePreference::FASTEST)?;
    let mut cursor = obj.cursor();

    for _rewrite in 0..rewrite_count {
        cursor.seek(SeekFrom::Start(0))?;

        with_random_bytes(
            &mut client.rng,
            object_size,
            object_size.min(128 * 1024) as usize,
            |b| cursor.write_all(b),
        )?;

        client.sync().expect("Failed to sync database");
    }

    Ok(())
}
