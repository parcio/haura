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
    use betree_storage_stack::database::{Database, InMemoryConfiguration};

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

#[cfg(test)]
mod test {
    use crate::{
        run_kv_ops,
        KvOp::{self, *},
    };

    #[test]
    fn known_failures() {
        let known_failures: &[Vec<KvOp>] = &[
            vec![Upsert(vec![211, 211, 211, 211, 96], vec![], 167772160)],
            vec![Delete(vec![
                255, 255, 255, 255, 255, 211, 211, 211, 96, 0, 0, 214, 255, 254, 255, 249, 210,
                255, 207, 0,
            ])],
        ];

        for failure in known_failures {
            run_kv_ops(&failure);
        }
    }
}
