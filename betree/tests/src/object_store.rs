use betree_storage_stack::{Database, StoragePreference};

use super::{configs, test_db, TO_MEBIBYTE};

#[test]
// Open and close the default object store and test if the objects are preserved
fn default_object_store_object_persists() {
    let mut db = test_db(2, 64, Default::default());
    let os = db.open_object_store().unwrap();
    let obj = os.open_or_create_object(b"hewo").unwrap();
    obj.write_at(&[1, 2, 3], 0).unwrap();
    obj.close().unwrap();
    db.close_object_store(os);
    let os = db.open_object_store().unwrap();
    let obj = os.open_object(b"hewo").unwrap().unwrap();
    let mut buf = vec![0; 3];
    obj.read_at(&mut buf, 0).unwrap();
    assert_eq!(buf, [1, 2, 3]);
    let other = db
        .open_named_object_store(b"uwu", StoragePreference::NONE)
        .unwrap();
    assert!(other.open_object(b"hewo").unwrap().is_none());
}

#[test]
// Open and close the default object store and test if the objects are preserved
fn object_store_object_persists() {
    let mut db = test_db(2, 64, Default::default());
    let os = db
        .open_named_object_store(b"uwu", StoragePreference::NONE)
        .unwrap();
    let obj = os.open_or_create_object(b"hewo").unwrap();
    obj.write_at(&[1, 2, 3], 0).unwrap();
    obj.close().unwrap();
    db.close_object_store(os);
    let os = db
        .open_named_object_store(b"uwu", StoragePreference::NONE)
        .unwrap();
    let obj = os.open_object(b"hewo").unwrap().unwrap();
    let mut buf = vec![0; 3];
    obj.read_at(&mut buf, 0).unwrap();
    assert_eq!(buf, [1, 2, 3]);
    let other = db.open_object_store().unwrap();
    assert!(other.open_object(b"hewo").unwrap().is_none());
}

#[test]
fn object_store_iter() {
    let mut db = test_db(2, 64, Default::default());
    let os = db.open_object_store().unwrap();
    db.close_object_store(os);
    let os = db
        .open_named_object_store(b"uwu", StoragePreference::NONE)
        .unwrap();
    db.close_object_store(os);
    let os = db
        .open_named_object_store(b"snek", StoragePreference::NONE)
        .unwrap();
    db.close_object_store(os);
    let mut osl = db.iter_object_stores_pub().unwrap();
    assert_eq!(osl.next().unwrap().unwrap().as_u64(), 1);
    assert_eq!(osl.next().unwrap().unwrap().as_u64(), 2);
    assert_eq!(osl.next().unwrap().unwrap().as_u64(), 3);
}

#[test]
fn object_store_object_iter() {
    let mut db = test_db(2, 64, Default::default());
    let os = db.open_object_store().unwrap();
    let _ = os.open_or_create_object(b"hewo").unwrap();
    let _ = os.open_or_create_object(b"uwu").unwrap();
    let mut objs = os.iter_objects().unwrap();
    assert_eq!(objs.next().unwrap().0, b"hewo");
    assert_eq!(objs.next().unwrap().0, b"uwu");
}

#[test]
fn object_store_reinit_from_iterator() {
    // Test opening of multiple stores by their names.
    // Test if the default store name '0' gets skipped.
    let mut db = test_db(2, 64, Default::default());
    let os = db
        .open_named_object_store(b"foo", StoragePreference::NONE)
        .unwrap();
    db.close_object_store(os);
    let os = db
        .open_named_object_store(b"bar", StoragePreference::NONE)
        .unwrap();
    db.close_object_store(os);
    let os = db.open_object_store().unwrap();
    db.close_object_store(os);
    assert!(db.iter_object_store_names().unwrap().count() == 2);
    for name in db.iter_object_store_names().unwrap() {
        let _ = db
            .open_named_object_store(&name, StoragePreference::NONE)
            .unwrap();
    }
}

#[test]
fn object_store_access_pattern() {
    // Any objects with a certain access pattern should be stored on the fitting
    // tier initially.
    // 0 - RandomRW
    // 1 - SeqRW
    let mut db = Database::build(configs::access_specific_config()).unwrap();
    let os = db.open_object_store().unwrap();
    let (obj, _) = os
        .create_object_with_access_type(
            b"foo",
            betree_storage_stack::PreferredAccessType::SequentialReadWrite,
        )
        .unwrap();
    let dat = vec![42; 32 * TO_MEBIBYTE];
    obj.write_at(&dat[..16 * TO_MEBIBYTE], 0).unwrap();
    db.sync().unwrap();
    assert!(db.free_space_tier()[0].free > db.free_space_tier()[1].free);
    let (obj, _) = os
        .create_object_with_access_type(
            b"foo",
            betree_storage_stack::PreferredAccessType::RandomReadWrite,
        )
        .unwrap();
    obj.write_at(&dat, 0).unwrap();
    db.sync().unwrap();
    assert!(db.free_space_tier()[0].free < db.free_space_tier()[1].free);
}

#[test]
fn object_store_reinit_from_id() {
    let mut db = test_db(2, 64, Default::default());
    let os = db.open_object_store().unwrap();
    db.close_object_store(os);
    let mut osl = db.iter_object_stores_pub().unwrap();
    let _ = db
        .internal_open_object_store_with_id(osl.next().unwrap().unwrap())
        .unwrap();
}
