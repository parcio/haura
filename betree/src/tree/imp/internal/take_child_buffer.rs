use crate::cow_bytes::CowBytes;

pub(in crate::tree::imp) struct MergeChildResult<NP> {
    pub(in crate::tree::imp) pivot_key: CowBytes,
    pub(in crate::tree::imp) old_np: NP,
    pub(in crate::tree::imp) size_delta: isize,
}
