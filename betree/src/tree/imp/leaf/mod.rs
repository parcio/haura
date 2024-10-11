//! Various impl of a "leaf" type node.

use crate::cow_bytes::CowBytes;

/// Case-dependent outcome of a rebalance operation.
#[derive(Debug)]
pub(super) enum FillUpResult {
    Rebalanced {
        pivot_key: CowBytes,
        size_delta: isize,
    },
    Merged {
        size_delta: isize,
    },
}

pub(crate) mod leaf;
pub(crate) mod packed;

pub(super) use leaf::LeafNode;
pub(super) use packed::PackedMap;
