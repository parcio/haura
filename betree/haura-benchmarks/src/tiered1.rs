use betree_perf::*;
use betree_storage_stack::StoragePreference;
use std::{error::Error, io::Write};

pub fn run(mut client: Client) -> Result<(), Box<dyn Error>> {
    const N_OBJECTS: u64 = 1;
    const OBJECT_SIZE: u64 = 5 * 1024 * 1024 * 1024;
    println!("running tiered1");

    let os = &client.object_store;

    let mut objects = Vec::new();

    for obj_idx in 0..N_OBJECTS {
        let name = format!("foo-{}", obj_idx);
        println!("using {}", name);
        objects.push(name.clone());

        let (obj, _info) =
            os.open_or_create_object_with_pref(name.as_bytes(), StoragePreference::FASTEST)?;
        let mut cursor = obj.cursor_with_pref(StoragePreference::FAST);

        with_random_bytes(&mut client.rng, OBJECT_SIZE / 2, 8 * 1024 * 1024, |b| {
            cursor.write_all(b)
        })?;

        cursor.set_storage_preference(StoragePreference::FASTEST);

        with_random_bytes(&mut client.rng, OBJECT_SIZE / 2, 8 * 1024 * 1024, |b| {
            cursor.write_all(b)
        })?;
    }

    client.sync().expect("Failed to sync database");

    for obj_name in objects {
        let (obj, info) = os
            .open_object_with_info(obj_name.as_bytes())?
            .expect("Object was just created, but can't be opened!");

        assert_eq!(info.size, OBJECT_SIZE);
        let size = obj
            .read_all_chunks()?
            .map(|chunk| {
                if let Ok((_k, v)) = chunk {
                    v.len() as u64
                } else {
                    0
                }
            })
            .sum::<u64>();
        assert_eq!(info.size, size);
    }

    Ok(())
}
