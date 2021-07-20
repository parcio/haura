use betree_storage_stack::{
    database::{Database, DatabaseConfiguration, AccessMode},
    storage_pool::{
        StoragePoolConfiguration,
        TierConfiguration,
        Vdev, LeafVdev
    }
};

#[derive(Debug)]
pub struct ValidKey(Vec<u8>);

impl<'a> arbitrary::Arbitrary<'a> for ValidKey {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // don't allow empty keys
        let len = u.arbitrary_len::<usize>()? + 1;
        let mut key = Vec::with_capacity(len);

        for _ in 0..len {
            // prevent interior zeroes
            key.push(u.arbitrary::<u8>()?.saturating_add(1));
        }

        Ok(ValidKey(key))
    }
}

#[derive(arbitrary::Arbitrary, Debug)]
pub enum KvOp {
    Get(ValidKey),
    Range(ValidKey, ValidKey),
    RangeDelete(ValidKey, ValidKey),
    Insert(ValidKey, Vec<u8>),
    Upsert(ValidKey, Vec<u8>, u32),
    Delete(ValidKey),
}

fn setup_db(data_mb: u32) -> Database<DatabaseConfiguration> {
    let config = DatabaseConfiguration {
        storage: StoragePoolConfiguration {
            tiers: vec![
                TierConfiguration {
                    top_level_vdevs: vec![
                        Vdev::Leaf(LeafVdev::Memory { mem: data_mb as usize * 1024 * 1024 })
                    ]
                }
            ],
            thread_pool_size: Some(1),
            ..Default::default()
        },
        access_mode: AccessMode::AlwaysCreateNew,
        sync_interval_ms: None,
        cache_size: 8 * 1024 * 1024,
        ..Default::default()
    };

    Database::build(config).unwrap()
}

pub fn run_kv_ops(ops: &[KvOp]) {
    let mut db = setup_db(8);
    db.create_dataset(b"data").unwrap();
    let ds = db.open_dataset(b"data").unwrap();

    for op in ops {
        use crate::KvOp::*;
        match op {
            Get(ValidKey(key)) => {
                ds.get(&key[..]).unwrap();
            }
            Range(ValidKey(from), ValidKey(to)) => {
                if let Ok(mut range) = ds.range(&from[..]..&to[..]) {
                    while let Some(_) = range.next() {}
                }
            }
            RangeDelete(ValidKey(from), ValidKey(to)) => {
                let _ = ds.range_delete(&from[..]..&to[..]);
            }
            Insert(ValidKey(key), data) => {
                let _ = ds.insert(&key[..], &data);
            }
            Upsert(ValidKey(key), data, offset) => {
                let _ = ds.upsert(&key[..], &data, *offset);
            }
            Delete(ValidKey(key)) => {
                let _ = ds.delete(&key[..]);
            }
        }
    }
}

#[derive(arbitrary::Arbitrary, Debug)]
pub enum ObjOp {
    Write(ValidKey, Vec<u8>, u64),
    Read(ValidKey, u32, u64),
    Delete(ValidKey)
}

pub fn run_obj_ops(ops: &[ObjOp]) {
    let mut db = setup_db(64);
    let os = db.open_object_store().unwrap();

    for op in ops {
        use crate::ObjOp::*;
        match op {
            Write(ValidKey(key), data, offset) => {
                let (obj, _info) = os.open_or_create_object(key).unwrap();
                let _ = obj.write_at(data, *offset);
            },
            Read(ValidKey(key), len, offset) => {
                let len = (*len).min(64 * 1024 * 1024);
                let mut buf = vec![0; len as usize];
                let (obj, _info) = os.open_or_create_object(key).unwrap();
                let _ = obj.read_at(&mut buf, *offset);
            }
            Delete(ValidKey(key)) => {
                if let Ok(Some((obj, _info))) = os.open_object(key) {
                    let _ = obj.delete();
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::*;

    #[test]
    fn known_kv_failures() {
        use crate::KvOp::*;
        let known_failures: &[Vec<KvOp>] = &[
            vec![Upsert(vec![211, 211, 211, 211, 96], vec![], 167772160)],
            vec![Delete(vec![
                255, 255, 255, 255, 255, 211, 211, 211, 96, 0, 0, 214, 255, 254, 255, 249, 210,
                255, 207, 0,
            ])],

            {
                let max_operation_size = 8388608;
                let mut buf = vec![0; max_operation_size + 1];
                let key = b"test-object-rw".to_vec();

                vec![
                    Upsert(key.clone(), vec![0], 0),
                    Upsert(key.clone(), buf[..max_operation_size - 1].to_vec(), 0),
                    Upsert(key.clone(), buf[..max_operation_size].to_vec(), 0),
                    Upsert(key.clone(), buf[..max_operation_size + 1].to_vec(), 0)
                ]
            }
        ];

        for failure in known_failures {
            run_kv_ops(&failure);
        }
    }

    #[test]
    fn known_obj_failures() {
        use crate::ObjOp::*;
        let known_failures: &[Vec<ObjOp>] = &[
            {
                let max_operation_size = 8388608;
                let mut buf = vec![0; max_operation_size + 1];
                let key = b"test-object-rw".to_vec();

                vec![
                    Write(key.clone(), vec![0], 0),
                    Write(key.clone(), buf[..max_operation_size - 1].to_vec(), 0),
                    Write(key.clone(), buf[..max_operation_size].to_vec(), 0),
                    Write(key.clone(), buf[..max_operation_size + 1].to_vec(), 0),
                    Delete(key.clone()),
                    Read(key.clone(), max_operation_size as u64, 0)
                ]
            },
        ];

        for failure in known_failures {
            run_obj_ops(&failure);
        }
    }
}
