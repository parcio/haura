#![no_main]
use libfuzzer_sys::fuzz_target;
use betree_storage_stack::database::{ Database, InMemoryConfiguration };
use betree_fuzz::KvOp;


fuzz_target!(|ops: Vec<KvOp>| {
    println!("{:?}", ops);

    let config = InMemoryConfiguration {
        data_size: 64 * 1024 * 1024,
        cache_size: 8 * 1024 * 1024
    };
    let mut db = Database::build(config).unwrap();
    db.create_dataset(b"data").unwrap();
    let ds = db.open_dataset(b"data").unwrap();

    use betree_fuzz::KvOp::*;
    for op in ops {
        match op {
            Get(key) => { let _ = ds.get(key); },
            Range(from, to) => { let _ = ds.range(from..to); },
            RangeDelete(from, to) => { let _ = ds.range_delete(from..to); },
            Insert(key, data) => { let _ = ds.insert(key, &data); },
            Upsert(key, data, offset) => { let _ = ds.upsert(key, &data, offset); },
            Delete(key) => { let _ = ds.delete(key); }
        }
    }
});
