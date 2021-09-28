use betree_storage_stack::{
    database::AccessMode,
    storage_pool::{LeafVdev, TierConfiguration, Vdev},
    Database, DatabaseConfiguration, Dataset, StoragePoolConfiguration, StoragePreference,
};

use std::{
    error::Error,
    io::{Seek, SeekFrom, Write},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn main() -> Result<()> {
    let mut db = Database::build(DatabaseConfiguration {
        storage: StoragePoolConfiguration {
            tiers: vec![TierConfiguration::new(vec![Vdev::Leaf(LeafVdev::Memory {
                mem: 32 * 1024 * 1024,
            })])],
            ..StoragePoolConfiguration::default()
        },
        access_mode: AccessMode::AlwaysCreateNew,
        ..DatabaseConfiguration::default()
    })?;

    let os = db.open_named_object_store(b"example", StoragePreference::NONE)?;

    let obj1 = os.create_object(&[10, 10, 10])?;
    obj1.write_at(&[42, 43, 44], 10);
    obj1.write_at(&[1, 2, 3, 4, 5], 500 * 128 * 1024);

    obj1.set_metadata(b"foo", b"bar")?;

    let obj2 = os.create_object(&[20, 20])?;
    let mut cursor = obj2.cursor();
    cursor.write_all(&[1, 4, 3, 2])?;
    cursor.write_all(&[5, 6, 7, 8])?;
    cursor.seek(SeekFrom::Start(1))?;
    cursor.write_all(&[2, 3, 4])?;

    db.close_object_store(os);

    println!("Metadata: ");
    print_dataset(&db.open_dataset(b"example\0meta")?)?;
    println!("\nData: ");
    print_dataset(&db.open_dataset(b"example\0data")?)?;

    Ok(())
}

fn print_dataset(ds: &Dataset<DatabaseConfiguration>) -> Result<()> {
    for (k, v) in ds.range::<_, &[u8]>(..)?.flatten() {
        println!("{:?} -> {:?}", &k[..], &v[..]);
    }

    Ok(())
}
