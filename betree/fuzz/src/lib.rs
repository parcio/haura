#[derive(arbitrary::Arbitrary, Debug)]
pub enum KvOp {
    Get(Vec<u8>),
    Range(Vec<u8>, Vec<u8>),
    RangeDelete(Vec<u8>, Vec<u8>),
    Insert(Vec<u8>, Vec<u8>),
    Upsert(Vec<u8>, Vec<u8>, u32),
    Delete(Vec<u8>)
}
