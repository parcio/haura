use super::test_db;
use betree_storage_stack::{tree::StorageKind, Database, Dataset};
use rand::{
    rngs::{StdRng, ThreadRng},
    seq::SliceRandom,
    RngCore,
};

pub fn random_db(tier: u32, mb_per_tier: u32, kind: StorageKind) -> (Database, Dataset, u32) {
    let mut db = test_db(tier, mb_per_tier, kind);
    let ds = db.open_or_create_dataset(b"hey").unwrap();
    let mut val = vec![0u8; 1024];
    let mut rng: StdRng = rand::SeedableRng::seed_from_u64(1337);
    let ks = (tier as f32 * (mb_per_tier as u64 * 1024 * 1024) as f32 * 0.4) as u32 / 1086;
    let mut foo: Vec<u64> = (1..ks as u64).collect();
    foo.shuffle(&mut rng);
    for (it, idx) in foo.iter().enumerate() {
        rng.fill_bytes(&mut val);
        ds.insert(&idx.to_be_bytes()[..], val.as_slice()).unwrap();
        if it % 10000 == 0 {
            db.sync().unwrap();
        }
    }
    db.sync().unwrap();
    (db, ds, ks)
}
