use super::test_db;
use betree_storage_stack::{tree::StorageKind, Database, Dataset};
use rand::RngCore;

pub fn random_db(tier: u32, mb_per_tier: u32, kind: StorageKind) -> (Database, Dataset, u32) {
    let mut db = test_db(tier, mb_per_tier);
    dbg!(&kind);
    let ds = db.open_or_create_dataset_on(b"hey", kind).unwrap();
    let mut key = vec![0u8; 64];
    let mut val = vec![0u8; 1024];
    let mut rng = rand::thread_rng();
    let ks = (tier as f32 * (mb_per_tier as u64 * 1024 * 1024) as f32 * 0.8) as u32 / 1086;
    for idx in 1..ks {
        let k = format!("{idx}");
        rng.fill_bytes(&mut val);
        ds.insert(&(idx as u64).to_be_bytes()[..], val.as_slice())
            .unwrap();
    }
    db.sync().unwrap();
    (db, ds, ks)
}
