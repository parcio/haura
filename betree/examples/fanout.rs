use betree_storage_stack::{
    compression::CompressionConfiguration,
    database::AccessMode,
    storage_pool::{LeafVdev, TierConfiguration, Vdev},
    Database, DatabaseConfiguration, StoragePoolConfiguration, StoragePreference,
};

use std::{error::Error, process};

#[cfg(features = "internal-api")]
fn main() -> Result<(), Box<dyn Error>> {
    let mut db = Database::build(DatabaseConfiguration {
        storage: StoragePoolConfiguration {
            tiers: vec![TierConfiguration::new(vec![Vdev::Leaf(LeafVdev::Memory(
                12 * 1024 * 1024 * 1024,
            ))])],
            ..StoragePoolConfiguration::default()
        },
        access_mode: AccessMode::AlwaysCreateNew,
        ..DatabaseConfiguration::default()
    })?;

    db.create_dataset(b"d", StoragePreference::NONE)?;
    let ds = db.open_dataset(b"d", StoragePreference::NONE)?;

    let mut key = vec![42; 512 * 1024];
    for k in 0..10000 {
        key[..4].copy_from_slice(&u32::to_be_bytes(k)[..]);
        ds.insert(&key[..], &[])?;
    }

    ds.debug_for_each();

    Ok(())
}

#[cfg(not(features = "internal-api"))]
fn main() {
    eprintln!("Feature 'internal-api' must be activated for this example");
    process::exit(1);
}
