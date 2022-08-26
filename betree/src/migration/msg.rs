use crate::{
    database::{DatabaseBuilder, Dataset, DatasetId},
    object::{ObjectId, ObjectInfo, ObjectStore, ObjectStoreId},
    storage_pool::DiskOffset,
    vdev::Block,
    StoragePreference,
};
use std::time::SystemTime;

#[derive(Clone)]
pub enum DmlMsg {
    // The three base operations of our store.
    // Largely relevant for clearing, and frequency determination in LRU, LFU,
    // FIFO and other policies
    Fetch(OpInfo),
    Write(OpInfo),
    Remove(OpInfo),

    // Initial message at the beginning of an session
    // For effectiveness it is advised to discover all nodes in every dataset intially.
    Discover(DiskOffset),
}

#[derive(Hash, PartialEq, Eq, Clone)]
pub struct ObjectKey(ObjectStoreId, ObjectId);

impl ObjectKey {
    pub fn build(os_id: ObjectStoreId, id: ObjectId) -> Self {
        Self(os_id, id)
    }

    pub(crate) fn store_key(&self) -> &ObjectStoreId {
        &self.0
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
pub enum DatabaseMsg<Config: DatabaseBuilder + Clone> {
    // Relevant for Promotion and/or Demotion
    DatasetOpen(DatasetId),
    DatasetClose(DatasetId),

    /// Announce and deliver an accessible copy of active object stores.
    ObjectstoreOpen(ObjectStoreId, ObjectStore<Config>),
    ObjectstoreClose(ObjectStoreId),

    /// Informs of openend object, adjoint with extra information for access.
    ObjectOpen(ObjectKey, ObjectInfo),
    /// Informs of closed object, adjoint with extra information for access.
    ObjectClose(ObjectKey, ObjectInfo),
    /// Frequency information about read and write operations on an object.
    ObjectRead(ObjectKey),
    /// Report the written storage class with the new size of the object.
    ObjectWrite(ObjectKey, u64, StoragePreference),
    /// Notification if a manual migration took place.
    ObjectMigrate(ObjectKey, StoragePreference),
}

pub trait ConstructReport {
    fn build_fetch(info: OpInfo) -> Self;
    fn build_write(info: OpInfo) -> Self;
    fn fetch(offset: DiskOffset, size: Block<u32>) -> Self;
    fn write(offset: DiskOffset, size: Block<u32>, previous_offset: Option<DiskOffset>) -> Self;
    fn remove(offset: DiskOffset, size: Block<u32>) -> Self;
}

impl ConstructReport for DmlMsg {
    fn build_fetch(info: OpInfo) -> Self {
        Self::Fetch(info)
    }

    fn build_write(info: OpInfo) -> Self {
        Self::Write(info)
    }

    fn fetch(offset: DiskOffset, size: Block<u32>) -> Self {
        Self::build_fetch(OpInfo {
            offset,
            size,
            time: SystemTime::now(),
            previous_offset: None,
        })
    }

    fn write(offset: DiskOffset, size: Block<u32>, previous_offset: Option<DiskOffset>) -> Self {
        Self::build_write(OpInfo {
            offset,
            size,
            time: SystemTime::now(),
            previous_offset,
        })
    }

    fn remove(offset: DiskOffset, size: Block<u32>) -> Self {
        Self::Remove(OpInfo {
            offset,
            size,
            time: SystemTime::now(),
            previous_offset: None,
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
pub struct OpInfo {
    pub(crate) offset: DiskOffset,
    // If None, new entry
    pub(crate) previous_offset: Option<DiskOffset>,
    // Retrieved from [ObjectPointer]
    pub(crate) size: Block<u32>,
    // Maybe include this again?
    // pub(crate) dataset_id: DatasetId,
    pub(crate) time: SystemTime,
}
