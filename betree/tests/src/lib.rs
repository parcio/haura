#![allow(dead_code)]

use betree_storage_stack::{
    compression::CompressionConfiguration,
    database::AccessMode,
    object::{ObjectHandle, ObjectStore},
    storage_pool::{LeafVdev, TierConfiguration, Vdev},
    Database, DatabaseConfiguration, StoragePoolConfiguration, StoragePreference,
};

use std::{io::{BufReader, Read}, env};

use rand::{Rng, SeedableRng, prelude::ThreadRng};
use rand_xoshiro::Xoshiro256PlusPlus;

use insta::assert_json_snapshot;
use serde_json::json;

fn test_db(tiers: u32, mb_per_tier: u32) -> Database<DatabaseConfiguration> {
    let tier_size = mb_per_tier as usize * 1024 * 1024;
    let cfg = DatabaseConfiguration {
        storage: StoragePoolConfiguration {
            tiers: (0..tiers)
                .map(|_| TierConfiguration {
                    top_level_vdevs: vec![Vdev::Leaf(LeafVdev::Memory { mem: tier_size })],
                })
                .collect(),
            ..Default::default()
        },
        compression: CompressionConfiguration::None,
        access_mode: AccessMode::AlwaysCreateNew,
        ..Default::default()
    };

    Database::build(cfg).expect("Database initialisation failed")
}

// List of sizes for each tier is attached
// It is assumed len(that mb_per_tier) = tiers
fn test_db_uneven(tiers: usize, mb_per_tier: &[u32]) -> Database<DatabaseConfiguration> {
    let cfg = DatabaseConfiguration {
        storage: StoragePoolConfiguration {
            tiers: (0..tiers)
                .map(|idx| TierConfiguration {
                    top_level_vdevs: vec![Vdev::Leaf(LeafVdev::Memory { mem: mb_per_tier[idx] as usize * TO_MEBIBYTE })],
                })
                .collect(),
            ..Default::default()
        },
        compression: CompressionConfiguration::None,
        access_mode: AccessMode::AlwaysCreateNew,
        alloc_strategy: [vec![0], vec![1], vec![2], vec![3]],
        ..Default::default()
    };

    Database::build(cfg).expect("Database initialisation failed")
}

struct TestDriver {
    name: String,
    database: Database<DatabaseConfiguration>,
    object_store: ObjectStore<DatabaseConfiguration>,
    rng: Xoshiro256PlusPlus,
}

impl TestDriver {
    fn setup(test_name: &str, tiers: u32, mb_per_tier: u32) -> TestDriver {
        let mut database = test_db(tiers, mb_per_tier);

        TestDriver {
            name: String::from(test_name),
            rng: Xoshiro256PlusPlus::seed_from_u64(42),
            object_store: database
                .open_named_object_store(b"test", StoragePreference::FASTEST)
                .expect("Failed to open default object store"),
            database,
        }
    }

    // TODO: It would be nice if this could generate diffs to previous state,
    // and assert those. Diffs are easier to review and perhaps closer to what
    // we want to test.
    fn checkpoint(&mut self, name: &str) {
        self.database.sync().expect("Failed to sync database");

        assert_json_snapshot!(
            format!("{}/{}", self.name, name),
            json!({
                "shape/data":
                    self.object_store
                        .data_tree()
                        .tree_dump()
                        .expect("Failed to create data tree dump"),
                "keys/data":
                    self.object_store
                        .data_tree()
                        .range::<_, &[u8]>(..)
                        .expect("Failed to query data keys")
                        .map(|res| res.map(|(k, _v)| k))
                        .collect::<Result<Vec<_>, _>>()
                        .expect("Failed to gather data keys"),
                "keys/meta":
                    self.object_store
                        .meta_tree()
                        .range::<_, &[u8]>(..)
                        .expect("Failed to query meta keys")
                        .map(|res| res.map(|(k, _v)| k))
                        .collect::<Result<Vec<_>, _>>()
                        .expect("Failed to gather meta keys")
            })
        );
    }

    fn open(&self, obj_name: &[u8]) -> ObjectHandle<DatabaseConfiguration> {
        self.object_store
            .open_or_create_object(obj_name)
            .expect("Unable to open object")
    }

    fn insert_random_at(
        &mut self,
        object_name: &[u8],
        pref: StoragePreference,
        block_size: u64,
        times: u64,
        offset: u64,
    ) {
        let obj = self
            .object_store
            .open_or_create_object(object_name)
            .expect("Unable to create object");

        let mut buf = vec![0; block_size as usize];
        let pref = pref.or(StoragePreference::FASTEST);

        for i in 0..times {
            self.rng
                .try_fill(&mut buf[..])
                .expect("Couldn't fill with random data");
            obj.write_at_with_pref(&buf, offset + i * block_size as u64, pref)
                .expect("Failed to write buf");
        }
    }

    fn insert_random(
        &mut self,
        object_name: &[u8],
        pref: StoragePreference,
        block_size: u64,
        times: u64,
    ) {
        self.insert_random_at(object_name, pref, block_size, times, 0);
    }

    fn delete(&self, object_name: &[u8]) {
        if let Ok(Some(obj)) = self.object_store.open_object(object_name) {
            obj.delete().expect("Failed to delete object");
        }
    }

    fn read_for_length(&self, object_name: &[u8]) -> u64 {
        let obj = self
            .object_store
            .open_or_create_object(object_name)
            .expect("Unable to create object");

        BufReader::new(obj.cursor()).bytes().count() as u64
    }
}

#[test]
fn insert_single() {
    let mut driver = TestDriver::setup("insert single", 1, 256);

    driver.checkpoint("empty tree");
    driver.insert_random(b"foo", StoragePreference::NONE, 8192, 2000);
    driver.checkpoint("inserted foo");

    for _ in 1..=3 {
        driver.insert_random(b"foo", StoragePreference::NONE, 8192, 2000);
        // intentionally same key as above, to assert that tree structures is not changed by
        // object rewrites of the same size
        driver.checkpoint("inserted foo");
    }

    driver.insert_random(b"foo", StoragePreference::NONE, 8192, 4000);
    driver.checkpoint("rewrote foo, but larger");

    driver.delete(b"foo");
    driver.checkpoint("deleted foo");
    driver.insert_random(b"bar", StoragePreference::NONE, 8192, 3000);
    driver.checkpoint("inserted bar");
}

#[test]
fn delete_single() {
    let mut driver = TestDriver::setup("delete single", 1, 1024);

    driver.checkpoint("empty tree");
    driver.insert_random(b"something", StoragePreference::NONE, 8192 * 8, 2000);
    driver.checkpoint("inserted something");
    driver.delete(b"something");
    driver.checkpoint("deleted something");
}

#[test]
fn downgrade() {
    let mut driver = TestDriver::setup("downgrade", 2, 256);

    driver.checkpoint("empty tree");
    driver.insert_random(b"foo", StoragePreference::FASTEST, 8192, 2000);
    driver.checkpoint("fastest pref");
    driver.insert_random(b"foo", StoragePreference::FAST, 8192, 2100);
    driver.checkpoint("fast pref");
}

#[test]
fn sparse() {
    let mut driver = TestDriver::setup("sparse", 1, 1024);
    let chunk_size = 128 * 1024;

    driver.checkpoint("empty tree");
    driver.insert_random_at(
        b"foo",
        StoragePreference::FASTEST,
        chunk_size,
        200,
        300 * chunk_size,
    );
    driver.checkpoint("sparse write 1");
    driver.insert_random_at(
        b"foo",
        StoragePreference::FASTEST,
        chunk_size,
        300,
        800 * chunk_size,
    );
    driver.checkpoint("sparse write 2");

    assert_eq!(driver.read_for_length(b"foo"), (800 + 300) * chunk_size);
}

#[test]
fn rename() {
    let mut driver = TestDriver::setup("rename", 1, 512);

    driver.checkpoint("empty tree");
    driver.insert_random(b"foo", StoragePreference::FASTEST, 128 * 1024, 20);
    driver.checkpoint("inserted foo");

    {
        let obj = driver.open(b"foo");
        obj.set_metadata(b"quux", b"bar").unwrap();
        obj.set_metadata(b"some other key", b"some other value")
            .unwrap();
    }
    driver.checkpoint("inserted metadata");

    {
        let mut obj = driver.open(b"foo");
        obj.rename(b"not foo").unwrap();
    }
    driver.checkpoint("renamed foo to not foo");

    {
        let obj = driver.open(b"not foo");
        obj.delete_metadata(b"quux").unwrap();
        obj.set_metadata(b"new quux", b"bar").unwrap();
        obj.write_at(&[42], 128 * 1024 * 19283).unwrap();
    }
    driver.checkpoint("changed (meta)data after renaming");
}

const TO_MEBIBYTE: usize = 1024 * 1024;

// @jwuensche:
// This test seems to trigger particulary unregular behavior and may be failing depending on various factors
// We repeat this test here to trigger this potential behavior
fn write_flaky(tier_size_mb: u32, write_size_mb: usize) {
    for _ in 0..3 {
        let mut db = test_db(1, tier_size_mb);
        let os = db.open_named_object_store(b"test", StoragePreference::FASTEST).expect("Oh no! Could not open object store");
        let buf = vec![42_u8; write_size_mb * TO_MEBIBYTE];
        {
            let obj = os.open_or_create_object(b"hewo").expect("Oh no! Could not open object!");
            obj.write_at(&buf, 0).expect(format!("Writing of {} MiB into {} MiB storage failed", write_size_mb, tier_size_mb).as_str());
        }
        db.close_object_store(os);
        db.sync().expect(format!("Sync failed ({}MB of {}MB)", write_size_mb, tier_size_mb).as_str());
    }
}

use rstest::{rstest, fixture};

#[rstest]
#[case::a(16)]
#[case::b(32)]
#[case::c(64)]
#[case::d(128)]
#[case::e(256)]
#[case::f(512)]
#[case::g(1024)]
#[case::h(2048)]
fn write_block(#[case] tier_size_mb: u32) {
    let mut write_size = 1;
    while write_size < tier_size_mb {
        write_flaky(tier_size_mb, write_size as usize);
        write_size *= 2;
    }
    // @jwuensche: This is too errorprone at the moment will add a different test for this as it muddies the results from this one
    // write_flaky(tier_size_mb, (tier_size_mb - 1) as usize, format!("write_{}mb_{}mb", tier_size_mb, write_size).as_str());
}

#[rstest]
#[case::a(1024, 0.1)]
#[case::b(1024, 0.2)]
#[case::c(1024, 0.3)]
#[case::d(1024, 0.4)]
#[case::e(1024, 0.5)]
#[case::f(1024, 0.6)]
#[case::g(1024, 0.7)]
#[case::h(1024, 0.8)]
#[case::i(1024, 0.9)]
#[case::j(1024, 0.91)]
#[timeout(std::time::Duration::from_secs(60))]
fn write_full(#[case] tier_size_mb: u32, #[case] par_space: f32) {
    // @jwuensche: This test can lead to busy locks, the timeout prevents the tests from completely locking up
    // If 60 seconds are overstepped it is highly unlikely that the test will ever finish
    write_flaky(tier_size_mb, (tier_size_mb as f32 * par_space) as usize)
}

#[rstest]
// Fullness if buffer growth atleast x1.1
#[case::a(1024, 0.93)]
#[case::b(1024, 0.95)]
#[case::c(1024, 0.97)]
#[case::d(1024, 0.99)]
// Over-over fill
#[case::e(1024, 1.1)]
#[case::f(1024, 1.2)]
#[case::g(1024, 1.5)]
#[timeout(std::time::Duration::from_secs(60))]
fn write_overfull(#[case] tier_size_mb: u32, #[case] par_space: f32) {
    // env_logger::init();
    let mut db = test_db(1, tier_size_mb);
    let os = db.open_named_object_store(b"test", StoragePreference::FASTEST).expect("Oh no! Could not open object store");
    let buf = vec![42_u8; (tier_size_mb as f32 * par_space) as usize * TO_MEBIBYTE];
    {
        let obj = os.open_or_create_object(b"hewo").expect("Oh no! Could not open object!");
        obj.write_at(&buf, 0).expect(format!("Writing of {} MiB into {} MiB storage failed (Growth Factor 1.1)", tier_size_mb as f32 * par_space * 1.1, tier_size_mb).as_str());
    }
    db.close_object_store(os);
    // NOTE: Test multiple times if the error persist as it should in this case
    db.sync().expect_err(format!("Sync succeeded ({}MB of {}MB)", tier_size_mb as f32 * par_space * 1.1, tier_size_mb).as_str());
    db.sync().expect_err(format!("Sync succeeded ({}MB of {}MB)", tier_size_mb as f32 * par_space * 1.1, tier_size_mb).as_str());
    db.sync().expect_err(format!("Sync succeeded ({}MB of {}MB)", tier_size_mb as f32 * par_space * 1.1, tier_size_mb).as_str());
    // NOTE: If the sync errors are not corrected this will deadlock here on the final drop. Test with timeout.
}

#[fixture]
fn rng() -> ThreadRng {
    rand::thread_rng()
}

#[rstest]
#[case::a(128)]
#[case::b(512)]
#[case::c(1024)]
#[case::d(2048)]
fn write_sequence(#[case] tier_size_mb: u32) {
    let mut rng = rand::thread_rng();
    let mut db = test_db(1, tier_size_mb);
    let os = db.open_named_object_store(b"test", StoragePreference::FASTEST).expect("Oh no! Could not open object store");
    let mut accumulated: usize = 0;
    let max_chunk = tier_size_mb as f32 * 0.1;
    let mut idx: u8 = 1;
    let buf = vec![42; max_chunk as usize * TO_MEBIBYTE];
    while accumulated < (tier_size_mb as f32 * 0.9) as usize {
        let obj = os.open_or_create_object(&idx.to_le_bytes()).expect("oh no! could not open object!");
        let size = (max_chunk * rng.gen::<f32>()) as usize;
        obj.write_at(&buf[0..size], 0).expect("could not write");
        accumulated += size;
        idx+=1;
        db.sync().expect("Could not sync database");
    }
}

use rand::prelude::SliceRandom;

#[rstest]
#[case::a(128)]
#[case::b(512)]
#[case::c(1024)]
#[case::d(2048)]
fn write_delete_sequence(#[case] tier_size_mb: u32, mut rng: ThreadRng) {
    let mut db = test_db(1, tier_size_mb);
    let os = db.open_named_object_store(b"test", StoragePreference::FASTEST).expect("Oh no! Could not open object store");
    let mut accumulated: usize = 0;
    let max_chunk = tier_size_mb as f32 * 0.1;
    let mut idx: u8 = 1;
    let buf = vec![42; max_chunk as usize * TO_MEBIBYTE];
    let mut inserted: Vec<u8> = vec![];
    while accumulated < (tier_size_mb as f32 * 0.9) as usize {
        match rng.gen_bool(0.55) {
            true => {
                println!("Add object");
                let obj = os.open_or_create_object(&idx.to_le_bytes()).expect("oh no! could not open object!");
                let size = (max_chunk * rng.gen::<f32>()) as usize;
                obj.write_at(&buf[0..size], 0).expect("could not write");
                inserted.push(idx);
                accumulated += size;

            },
            false => {
                if inserted.len() > 0 {
                    println!("Delete object");
                    let key = inserted.choose(&mut rng).expect("Could not choose an element").clone();
                    let foo = os.open_object(&key.to_le_bytes()).expect("Key is not contained").expect("No content beyond object");
                    foo.delete().expect("Could not delete");
                    inserted.retain(|e| *e != key);
                }
            },
        }
        idx += 1;
        db.sync().expect("Could not sync database");
    }
}


#[rstest]
#[case::a(512,200)]
#[case::b(1024,400)]
#[case::c(2048,700)]
#[case::d(2048,800)]
#[case::e(2048,900)]
#[case::f(2048,1000)]
// @jwuensche
// The size s_1 of the tier should be in relation to the buffer size s_2
// s_1 < 3*s_2 && s_1 > 2*s_2
fn write_delete_essential_size(#[case] tier_size_mb: u32, #[case] buf_size: usize) {
    let mut db = test_db(1, tier_size_mb);
    let os = db.open_named_object_store(b"test", StoragePreference::FASTEST).expect("Oh no! Could not open object store");
    let buf = vec![42; buf_size * TO_MEBIBYTE];
    let buf2 = vec![24; buf_size * TO_MEBIBYTE];
    {
        let obj = os.open_or_create_object(b"test").expect("Could not create object");
        obj.write_at(&buf, 0).expect(&format!("Could not write buffer of size {}MB", buf_size));
    }
    db.sync().expect("Could not sync database");
    {
        let obj = os.open_or_create_object(b"test2").expect("Could not create object");
        obj.write_at(&buf, 0).expect(&format!("Could not write buffer of size {}MB", buf_size));
    }
    db.sync().expect("Could not sync database");
    {
        let obj = os.open_or_create_object(b"test2").expect("Could not create object");
        obj.delete().expect("Could not delete");
    }
    db.sync().expect("Could not sync database");
    {
        let obj = os.open_or_create_object(b"test3").expect("Could not create object");
        obj.write_at(&buf2, 0).expect(&format!("Could not write buffer of size {}MB", buf_size));
    }
    db.sync().expect("Could not sync database");
    {
        let obj = os.open_or_create_object(b"test").expect("Could not create object");
        let obj2 = os.open_or_create_object(b"test3").expect("Could not create object");
        let mut read = vec![0; buf_size * TO_MEBIBYTE];
        obj.read_at(&mut read, 0).expect("Could not read first key");
        assert_eq!(buf, read);
        obj2.read_at(&mut read, 0).expect("Could not read first key");
        assert_eq!(buf2, read);
    }
    db.sync().expect("Could not sync database");
}

#[rstest]
#[case::a(2048, 600)]
#[case::b(2048, 700)]
// @jwuensche:
// This test really provides a measure of convenience.
// It will not be logical to any application that writing over the same buffer space runs out of space.
// We should include some measure to handle these cases.
// -> Space Accounting!
fn overwrite_buffer(#[case] tier_size_mb: u32, #[case] buf_size: usize) {
    let mut db = test_db(1, tier_size_mb);
    let os = db.open_named_object_store(b"test", StoragePreference::FASTEST).expect("Oh no! Could not open object store");
    let buf = vec![42; buf_size * TO_MEBIBYTE];
    {
        let obj = os.open_or_create_object(b"test").expect("Could not create object");
        obj.write_at(&buf, 0).expect(&format!("Could not write buffer of size {}MB at offset {}", buf_size, 0));
        db.sync().expect("Could not sync database");
        obj.write_at(&buf, buf.len() as u64).expect(&format!("Could not write buffer of size {}MB at offset {}", buf_size, buf.len()));
        db.sync().expect("Could not sync database");
        obj.write_at(&buf, buf.len() as u64).expect(&format!("Could not write buffer of size {}MB at offset {}", buf_size, buf.len()));
    }
    db.sync().expect("Could not sync database");
}

#[rstest]
#[case::a(2048)]
fn write_sequence_random_fill(#[case] tier_size_mb: u32, mut rng: ThreadRng) {
    let mut db = test_db(1, tier_size_mb);
    let os = db.open_named_object_store(b"test", StoragePreference::FASTEST).expect("Oh no! Could not open object store");
    let mut accumulated: usize = 0;
    let max_chunk = tier_size_mb as f32 * 0.1;
    let mut idx: u8 = 1;
    let buf = vec![42; max_chunk as usize * TO_MEBIBYTE];
    while accumulated < (tier_size_mb as f32 * 0.9) as usize {
        let obj = os.open_or_create_object(&idx.to_le_bytes()).expect("oh no! could not open object!");
        let size = (max_chunk * rng.gen::<f32>()) as usize;
        obj.write_at(&buf[0..size], 0).expect("could not write");
        accumulated += size;
        idx+=1;
        db.sync().expect("Could not sync database");
    }
    let obj = os.open_or_create_object(b"finaldrop").expect("oh no! could not open object!");
    obj.write_at(&buf, 0).expect("hello");
    db.sync().expect("Could not sync database");
}

#[rstest]
#[case::a(32)]
#[case::b(128)]
#[case::c(512)]
#[case::d(2048)]
fn migrate_down(#[case] tier_size_mb: u32) {
    let mut db = test_db(2, tier_size_mb);
    let os = db.open_named_object_store(b"test", StoragePreference::FASTEST).expect("Oh no! Could not open object store");
    let obj = os.open_or_create_object(b"foobar").expect("oh no! could not open object!");
    let buf = vec![42; (tier_size_mb as f32 * 0.7) as usize * TO_MEBIBYTE];
    obj.write_at(&buf, 0).expect("Could not write to newly created object");
    db.sync().expect("Could not sync database");
    obj.migrate(StoragePreference::FAST).unwrap();
}

#[rstest]
#[case::a(32)]
#[case::b(128)]
#[case::c(512)]
#[case::d(2048)]
fn migrate_up(#[case] tier_size_mb: u32) {
    let mut db = test_db(2, tier_size_mb);
    let os = db.open_named_object_store(b"test", StoragePreference::FAST).expect("Oh no! Could not open object store");
    let obj = os.open_or_create_object(b"foobar").expect("oh no! could not open object!");
    let buf = vec![42; (tier_size_mb as f32 * 0.7) as usize * TO_MEBIBYTE];
    obj.write_at(&buf, 0).expect("Could not write to newly created object");
    db.sync().expect("Could not sync database");
    obj.migrate(StoragePreference::FASTEST).unwrap();
}

#[rstest]
#[case::a(32, 8)]
#[case::b(128, 32)]
#[case::c(512, 128)]
#[case::d(2048, 512)]
// The most commmon case for this should be moving upwards to faster storage.
// Therefore this is teted here, the other direction should however behave exactly the same.
fn migrate_invalid_size(#[case] tier_size_mb: u32, #[case] buffer_size: u32) {
    let mut db = test_db_uneven(2, &[buffer_size, tier_size_mb]);
    let os = db.open_named_object_store(b"test", StoragePreference::FAST).expect("Oh no! Could not open object store");
    let obj = os.open_or_create_object(b"foobar").expect("oh no! could not open object!");
    let buf = vec![42; (tier_size_mb as f32 * 0.9) as usize * TO_MEBIBYTE];
    obj.write_at(&buf, 0).expect("Could not write to newly created object");
    db.sync().expect("Could not sync database");
    // The slowest tier is not defined in this default configuration
    obj.migrate(StoragePreference::FASTEST);//.expect_err("Should not succeed");
    db.sync().expect_err("Too large data synced");
}

#[rstest]
#[case::a(32)]
#[case::b(128)]
#[case::c(512)]
#[case::d(2048)]
fn migrate_invalid_tier(#[case] tier_size_mb: u32) {
    let mut db = test_db(2, tier_size_mb);
    let os = db.open_named_object_store(b"test", StoragePreference::FASTEST).expect("Oh no! Could not open object store");
    let obj = os.open_or_create_object(b"foobar").expect("oh no! could not open object!");
    let buf = vec![42; (tier_size_mb as f32 * 0.6) as usize * TO_MEBIBYTE];
    obj.write_at(&buf, 0).expect("Could not write to newly created object");
    db.sync().expect("Could not sync database");
    // The slowest tier is not defined in this default configuration
    obj.migrate(StoragePreference::SLOWEST).expect_err("Accepted invalid tier");
}

#[rstest]
#[case::a(32)]
#[case::b(128)]
#[case::c(512)]
#[case::d(2048)]
// @jwuensche: This case should not raise any errors and should just allow silent dropping of the operation.
fn migrate_nochange(#[case] tier_size_mb: u32, mut rng: ThreadRng) {
    let mut db = test_db(2, tier_size_mb);
    let os = db.open_named_object_store(b"test", StoragePreference::FASTEST).expect("Oh no! Could not open object store");
    let obj = os.open_or_create_object(b"foobar").expect("oh no! could not open object!");
    let mut buf = vec![42u8; (tier_size_mb as f32 * 0.8) as usize * TO_MEBIBYTE];
    rng.try_fill(buf.as_mut_slice()).expect("Could not fill buffer");
    obj.write_at(&buf, 0).expect("Could not write to newly created object");
    db.sync().expect("Could not sync database");
    obj.migrate(StoragePreference::FASTEST).unwrap();
}

#[rstest]
fn space_accounting_smoke() {
    // env_logger::init();
    let mut db = test_db(2, 64);
    let before = db.free_space_tier();
    let os = db.open_named_object_store(b"test", StoragePreference::FASTEST).expect("Oh no! Could not open object store");
    let obj = os.open_or_create_object(b"foobar").expect("oh no! could not open object!");
    let buf = vec![42u8; 2 * TO_MEBIBYTE];
    obj.write_at(&buf, 0).expect("Could not write to newly created object");
    db.sync().expect("Could not sync database");
    let after = db.free_space_tier();

    // Size - superblocks blocks
    let expected_free_size_before = (64 * TO_MEBIBYTE as u64) / 4096 - 2;
    //let expected_free_size_after = expected_free_size_before - (32 * TO_MEBIBYTE as u64 / 4096);
    println!("{:?}", before);
    println!("{:?}", after);
    assert_eq!(before[0].free.as_u64(), expected_free_size_before);
    assert_ne!(before[0].free, after[0].free);
    // assert_eq!(after[0].free.as_u64(), expected_free_size_after);
}
