use betree_storage_stack::StoragePreference;

use super::test_db;

#[test]
// Open and close the default object store and test if the objects are preserved
fn default_object_store_object_persists() {
        let mut db = test_db(2, 64);
        let os = db.open_object_store().unwrap();
        let obj = os.open_or_create_object(b"hewo").unwrap();
        obj.write_at(&[1,2,3], 0).unwrap();
        obj.close().unwrap();
        db.close_object_store(os);
        let os = db.open_object_store().unwrap();
        let obj = os.open_object(b"hewo").unwrap().unwrap();
        let mut buf = vec![0; 3];
        obj.read_at(&mut buf, 0).unwrap();
        assert_eq!(buf, [1,2,3]);
        let other = db.open_named_object_store(b"uwu", StoragePreference::NONE).unwrap();
        assert!(other.open_object(b"hewo").unwrap().is_none());
}

#[test]
// Open and close the default object store and test if the objects are preserved
fn object_store_object_persists() {
        let mut db = test_db(2, 64);
        let os = db.open_named_object_store(b"uwu", StoragePreference::NONE).unwrap();
        let obj = os.open_or_create_object(b"hewo").unwrap();
        obj.write_at(&[1,2,3], 0).unwrap();
        obj.close().unwrap();
        db.close_object_store(os);
        let os = db.open_named_object_store(b"uwu", StoragePreference::NONE).unwrap();
        let obj = os.open_object(b"hewo").unwrap().unwrap();
        let mut buf = vec![0; 3];
        obj.read_at(&mut buf, 0).unwrap();
        assert_eq!(buf, [1,2,3]);
        let other = db.open_object_store().unwrap();
        assert!(other.open_object(b"hewo").unwrap().is_none());
}

#[test]
fn object_store_iter() {
        let mut db = test_db(2, 64);
        let os = db.open_object_store().unwrap();
        db.close_object_store(os);
        let os = db.open_named_object_store(b"uwu", StoragePreference::NONE).unwrap();
        db.close_object_store(os);
        let os = db.open_named_object_store(b"snek", StoragePreference::NONE).unwrap();
        db.close_object_store(os);
        let mut osl = db.iter_object_stores_pub().unwrap();
        assert_eq!(osl.next().unwrap().unwrap().as_u64(), 1);
        assert_eq!(osl.next().unwrap().unwrap().as_u64(), 2);
        assert_eq!(osl.next().unwrap().unwrap().as_u64(), 3);
}

#[test]
fn object_store_object_iter() {
    let mut db = test_db(2, 64);
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
    let mut db = test_db(2, 64);
    let os = db.open_named_object_store(b"foo", StoragePreference::NONE).unwrap();
    db.close_object_store(os);
    let os = db.open_named_object_store(b"bar", StoragePreference::NONE).unwrap();
    db.close_object_store(os);
    let os = db.open_object_store().unwrap();
    db.close_object_store(os);
    assert!(db.iter_object_store_names().unwrap().count() == 2);
    for name in db.iter_object_store_names().unwrap() {
        let _ = db.open_named_object_store(&name, StoragePreference::NONE).unwrap();
    }
}
