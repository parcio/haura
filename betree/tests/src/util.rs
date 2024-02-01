use super::test_db;
use betree_storage_stack::{tree::StorageKind, Database, Dataset};
use rand::RngCore;

pub fn random_db(tier: u32, mb_per_tier: u32, kind: StorageKind) -> (Database, Dataset) {
    let mut db = test_db(tier, mb_per_tier);
    let ds = db.open_or_create_dataset_on(b"hey", kind).unwrap();
    let mut key = vec![0u8; 64];
    let mut val = vec![0u8; 4096];
    let mut rng = rand::thread_rng();
    for _ in 0..20000 {
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut val);
        ds.insert(key.clone(), val.as_slice()).unwrap();
    }
    db.sync().unwrap();
    (db, ds)
}
