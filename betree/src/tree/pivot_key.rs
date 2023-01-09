//! A node identification key.
//!
//! See [PivotKey] for more documentation.
use crate::{
    cow_bytes::CowBytes,
    database::DatasetId,
};

/// An identifier for an arbitrary node.
///
/// The access is part direction, part position of the relation from the
/// *parent* node. For the root this position is arbitrary and carries no pivot.
/// The dataset id determines the identity of the tree in which the given bytes
/// can be found. This makes the [PivotKey] globally unique and suitable to
/// accesss and identify certain nodes within a tree.
///
/// Furthermore, a key does not have to exist as keys can be changed or deleted
/// over the lifetime of a tree; take care to deal with missing information.
///
/// ```text
/// px - Pivot Element
///
///         p1      p2      p3
/// ┌───────┬───────┬───────┬───────┐
/// │       │       │       │       │
/// │  Left │ Right │ Right │ Right │
/// │  (p1) │ (p1)  │ (p2)  │ (p3)  │
/// └───┬───┴───┬───┴───┬───┴───┬───┘
///     │       │       │       │
///     ▼       ▼       ▼       ▼
/// ```
///
/// TODO: The key is not unique atm, multiple keys can point to same node, but
/// this relation is atleast surjective.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PivotKey {
    Left(CowBytes, DatasetId),
    Right(CowBytes, DatasetId),
    Root(DatasetId),
}

impl PivotKey {
    /// Get the dataset id of this node key.
    pub fn d_id(&self) -> DatasetId {
        match self {
            Self::Left(_, d_id) | Self::Right(_, d_id) | Self::Root(d_id) => *d_id,
        }
    }

    /// Get the pivot bytes of this node key.
    ///
    /// If the return value is none, the indexed node is the *root* of the tree.
    pub fn bytes(&self) -> Option<CowBytes> {
        match self {
            // Cheap CowBytes clone
            Self::Left(p, _) | Self::Right(p,_) => Some(p.clone()),
            Self::Root(_) => None,
        }
    }

    /// Wether or not the key indexes a root node.
    pub fn is_root(&self) -> bool {
        self.bytes().is_none()
    }

    /// Wether or not the direction from the pivot is left.
    ///
    /// See [PivotKey] for a more detailed instruction.
    pub fn is_left(&self) -> bool {
        match self {
            Self::Left(..) => true,
            _ => false,
        }
    }

    /// Wether or not the direction from the pivot is right.
    ///
    /// See [PivotKey] for a more detailed instruction.
    pub fn is_right(&self) -> bool {
        match self {
            Self::Right(..) => true,
            _ => false,
        }
    }
}
