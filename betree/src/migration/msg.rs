use crate::{
    cow_bytes::CowBytes,
    database::DatasetId,
    object::{ObjectId, ObjectInfo, ObjectStore, ObjectStoreId},
    storage_pool::DiskOffset,
    tree::PivotKey,
    vdev::Block,
    StoragePreference,
};
use std::{
    fmt::Display,
    time::{Duration, SystemTime},
};

#[derive(Clone)]
/// Message emitted by the [crate::data_management::Dml] which
/// contains storage operation information about the elements of the internal representation,
/// the nodes of the B-epsilon tree.
///
/// For each message it is of great importance to observe the attached [OpInfo]
/// as information about the transitional node identification are stored there
/// which are not stored in any manner outside of the migration policy.
pub enum DmlMsg {
    // The three base operations of our store.
    // Largely relevant for clearing, and frequency determination in LRU, LFU,
    // FIFO and other policies
    /// A fetch operation from disk has been performed.
    Fetch(OpInfo),
    /// A write operation to disk has been performed.
    Write(OpInfo),
    /// A node has been completely removed from the storage stack and can no
    /// longer be referenced.
    Remove(OpInfo),
    // /// Initial message at the beginning of an session.
    // Discover(DiskOffset),
}

use serde::Serialize;

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub struct GlobalObjectId(ObjectStoreId, ObjectId);

impl GlobalObjectId {
    pub fn build(os_id: ObjectStoreId, id: ObjectId) -> Self {
        Self(os_id, id)
    }

    pub(crate) fn store_key(&self) -> &ObjectStoreId {
        &self.0
    }
}

impl Serialize for GlobalObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let key = format!("{}-{}", self.0, self.1);
        serializer.serialize_str(&key)
    }
}

impl Display for GlobalObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}-{}", self.0, self.1))
    }
}

/// This is has been designed to be it's own separate message to allow for
/// separate handling of objects and nodes and to prevent recursive type
/// definitions on [DatabaseBuilder].
///
/// The object migration must happen on a _best_ estimation basis. An object
/// might be distributed on multiple storage tiers and must not adhere to the
/// storage preference given.
#[derive(Clone)]
pub enum DatabaseMsg {
    // Relevant for Promotion and/or Demotion
    DatasetOpen(DatasetId),
    DatasetClose(DatasetId),

    /// Announce and deliver an accessible copy of active object stores.
    ObjectstoreOpen(ObjectStoreId, ObjectStore),
    ObjectstoreClose(ObjectStoreId),

    /// Informs of openend object, adjoint with extra information for access.
    ObjectOpen(GlobalObjectId, ObjectInfo, CowBytes),
    /// Informs of closed object, adjoint with extra information for access.
    ObjectClose(GlobalObjectId, ObjectInfo),
    /// Frequency information about read and write operations on an object.
    ObjectRead(GlobalObjectId, Duration),
    /// Report the written storage class with the new size of the object.
    ObjectWrite(GlobalObjectId, u64, StoragePreference, Duration),
    /// Notification if a manual migration took place.
    ObjectMigrate(GlobalObjectId, StoragePreference),
    /// Notification similar to [Self::ObjectOpen] but with different semantics.
    ObjectDiscover(GlobalObjectId, ObjectInfo, CowBytes),
}

impl DmlMsg {
    pub fn fetch(offset: DiskOffset, size: Block<u32>, pivot_key: PivotKey) -> Self {
        Self::Fetch(OpInfo {
            offset,
            size,
            time: SystemTime::now(),
            pivot_key,
        })
    }

    pub fn write(offset: DiskOffset, size: Block<u32>, pivot_key: PivotKey) -> Self {
        Self::Write(OpInfo {
            offset,
            size,
            time: SystemTime::now(),
            pivot_key,
        })
    }

    pub fn remove(offset: DiskOffset, size: Block<u32>, pivot_key: PivotKey) -> Self {
        Self::Remove(OpInfo {
            offset,
            size,
            time: SystemTime::now(),
            pivot_key,
        })
    }
}

// NOTE: This is a short discussion on how the migration of complete Nodes should work.
// This was created as a design discussion and therefore preserved here as a comment.
//
// The information we receive comes from the fetching and eviction phase of the
// DML. Problematic here is the combination of user hinting and system
// migration. If we assume that we want to migrate one complete node to a
// different storage layer, or would like to atleast, all contained entries may
// not contain any higher preference. Therefore we would need to modify the
// existing storage preference of keys or encounter a blocked node which may
// never be moved below the current level. We may skip this node and proceed
// with the next chosen in the current level or forcibly move this node.
//
// The implementation of an assigned storage preference, chosen by the automatic
// migration policy, would facilitate an additional member of the Leaf struct
// which carries a system-storage-preference, which acts as an Upper-Bound
// storage boundary in contrast to the Lower-Bound of the user.
//
//  The combination of the upper and lower bound of storage preferences can then
//  be structured as the following problem:
//  ( > equals faster than, => equals the result on the combination of the
//  preference as proposed in the leaf)
//
//      > Equal to normal Storage Preferences
//      1. System Preference > User Preference => System Preference
//      2. System Preference = User Preference => System Preference
//
//      > The following line differentiated the solutions argued below and have
//      > their own advantages
//      3. System Preference < User Preference => User Preference (Preference Honouring)
//      3. System Preference < User Preference => System Preference (Preference Abandonment)
//
//      > Equal to normal Storage Preferences
//      4. System None < User Preference => User Preference
//      5. System None = User Preference => User Preference
//      6. System None > User Preference => User Preference
//
// - Preference Honouring:
//  Less intrusive and more true to the user hinting. We would need to depend on
//  the user to make sane choices here and manually move data they deem not
//  critical to lower levels (High effort for user). Users depend on the storage
//  preference right now to define which level of storage they want to fulfill
//  at least (Lower Bound). The comparison between two values will always prefer
//  the higher.  On systems with filled disk this will, depending on the
//  fallback configuration, either lead to allocations in different tiers (Down
//  and Up!) or to errors when writing.  As nodes are allocated on the slowest
//  desirable free tier, this will be the case in almost all auto migrations
//  which demote the current node.  Honouring the preferences will not ease any
//  situation as it will likely end up in the same storage tier as it is
//  inhabiting now.
//
//  Notable in this context is that the fallback policy already does not honour
//  the actual storage preference in trade-off to system stability. Though the
//  treatment of these situations is agreed to by the user via a different
//  contract in the DatabaseConfiguration.
//
// - Preference Abandonment:
//  An alternative to honouring the user preference is the abandonment of
//  contracts in the demotion case. Since we perform demotions in configurations
//  where a large number of blocks are already allocated and future accesses
//  might encounter full disks on their desired storage tiers it can be argued
//  to ignore the initial user estimation in favor of current storage tier
//  preferences of the user.

#[derive(Clone)]
/// All metadata necessary to classify an IO operation.
pub struct OpInfo {
    /// The temporal identifier of a node. If `previous_offset` is `Some` this
    /// node has been rewritten to a new location due to copy on write.
    pub(crate) offset: DiskOffset,
    /// The previous offset of the node written. If `None` the node has been
    /// newly created and never been written before.
    pub(crate) pivot_key: PivotKey,
    /// The size of the nodes in blocks. Relevant for weighting of operations
    /// and space restrictions.
    pub(crate) size: Block<u32>,
    // FIXME: As the dataset id is deeply burried in type definitions and
    // generics specified in the DMU we need to extract this, from the database
    // to be passed on to this message type. A bit annoying.
    // /// The dataset which this node belongs to. May be used to exclude datasets from migrations.
    // pub(crate) dataset_id: DatasetId,
    /// The time at which an operation has occurred.
    pub(crate) time: SystemTime,
}
