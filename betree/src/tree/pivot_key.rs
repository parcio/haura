//! A node identification key.
//!
//! See [PivotKey] for more documentation.
use serde::Serialize;

use crate::{cow_bytes::CowBytes, database::DatasetId};

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
/// │ Left  │       │       │       │
/// │ Outer │ Right │ Right │ Right │
/// │ (p1)  │ (p1)  │ (p2)  │ (p3)  │
/// └───┬───┴───┬───┴───┬───┴───┬───┘
///     │       │       │       │
///     ▼       ▼       ▼       ▼
/// ```
#[derive(Hash, Clone, Debug, PartialEq, Eq, Serialize)]
pub enum PivotKey {
    /// Left most child of this node. Left of `.0`.
    LeftOuter(CowBytes, DatasetId),
    /// Right child of `.0`.
    Right(CowBytes, DatasetId),
    /// Root of the given tree.
    Root(DatasetId),
}

impl PivotKey {
    /// Get the dataset id of this node key.
    pub fn d_id(&self) -> DatasetId {
        match self {
            Self::LeftOuter(_, d_id) | Self::Right(_, d_id) | Self::Root(d_id) => *d_id,
        }
    }

    /// Get the pivot bytes of this node key.
    ///
    /// If the return value is none, the indexed node is the *root* of the tree.
    pub fn bytes(&self) -> Option<CowBytes> {
        match self {
            // Cheap CowBytes clone
            Self::LeftOuter(p, _) | Self::Right(p, _) => Some(p.clone()),
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
            Self::LeftOuter(..) => true,
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

/// A PivotKey without indication of global tree location.
///
/// This enum is useful to transport information from node local operations to
/// the tree layer to avoid passing around [DatasetId]s continuously.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum LocalPivotKey {
    LeftOuter(CowBytes),
    Right(CowBytes),
    Root(),
}

impl LocalPivotKey {
    pub fn to_global(self, d_id: DatasetId) -> PivotKey {
        match self {
            LocalPivotKey::LeftOuter(p) => PivotKey::LeftOuter(p, d_id),
            LocalPivotKey::Right(p) => PivotKey::Right(p, d_id),
            LocalPivotKey::Root() => PivotKey::Root(d_id),
        }
    }
}
