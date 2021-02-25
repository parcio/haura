use betree_storage_stack::database::{Database, InMemoryConfiguration};


#[derive(arbitrary::Arbitrary, Debug)]
pub enum KvOp {
    Get(Vec<u8>),
    Range(Vec<u8>, Vec<u8>),
    RangeDelete(Vec<u8>, Vec<u8>),
    Insert(Vec<u8>, Vec<u8>),
    Upsert(Vec<u8>, Vec<u8>, u32),
    Delete(Vec<u8>),
}

pub fn run_kv_ops(ops: &[KvOp]) {
    let config = InMemoryConfiguration {
        data_size: 8 * 1024 * 1024,
        cache_size: 8 * 1024 * 1024,
    };
    let mut db = Database::build(config).unwrap();
    db.create_dataset(b"data").unwrap();
    let ds = db.open_dataset(b"data").unwrap();

    for op in ops {
        use crate::KvOp::*;
        // eprintln!("{:?}", op);
        match op {
            Get(key) => {
                ds.get(&key[..]).unwrap();
            }
            Range(from, to) => {
                if let Ok(mut range) = ds.range(&from[..]..&to[..]) {
                    while let Some(_) = range.next() {}
                }
            }
            RangeDelete(from, to) => {
                let _ = ds.range_delete(&from[..]..&to[..]);
            }
            Insert(key, data) => {
                let _ = ds.insert(&key[..], &data);
            }
            Upsert(key, data, offset) => {
                let _ = ds.upsert(&key[..], &data, *offset);
            }
            Delete(key) => {
                let _ = ds.delete(&key[..]);
            }
        }
    }
}

#[derive(arbitrary::Arbitrary, Debug)]
pub enum ObjOp {
    Write(Vec<u8>, Vec<u8>, u64),
    Read(Vec<u8>, u64, u64),
    Delete(Vec<u8>)
}

pub fn run_obj_ops(ops: &[ObjOp]) {
    let config = InMemoryConfiguration {
        data_size: 128 * 1024 * 1024,
        cache_size: 8 * 1024 * 1024,
    };
    let mut db = Database::build(config).unwrap();
    let mut os = db.open_object_store().unwrap();

    for op in ops {
        use crate::ObjOp::*;
        // eprintln!("{:?}", op);
        match op {
            Write(key, data, offset) => {
                let mut obj = os.open_or_create_object(key).unwrap();
                obj.write_at(data, *offset);
            },
            Read(key, len, offset) => {
                let mut buf = vec![0; *len as usize];
                let mut obj = os.open_or_create_object(key).unwrap();
                obj.read_at(&mut buf, *offset);
            }
            Delete(key) => {
                if let Ok(Some(obj)) = os.open_object(key) {
                    obj.delete();
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
