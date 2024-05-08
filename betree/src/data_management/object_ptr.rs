use std::num::NonZeroU32;

use super::HasStoragePreference;
use crate::{
    compression::DecompressionTag,
    database::{DatasetId, Generation},
    size::StaticSize,
    storage_pool::DiskOffset,
    vdev::Block,
    StoragePreference,
};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
#[archive(check_bytes)]
/// A pointer to an on-disk serialized object.
pub struct ObjectPointer<D> {
    pub(super) decompression_tag: DecompressionTag,
    pub(super) checksum: D,
    pub(super) offset: DiskOffset,
    pub(super) size: Block<u32>,
    pub(super) metadata_size: Option<NonZeroU32>,
    pub(super) info: DatasetId,
    pub(super) generation: Generation,
}

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
#[archive(check_bytes)]
pub enum NodeType {
    // FIXME: Replace with adjustal block size. 256 bytes for NVM.
    Memory { m_size: Block<u32> },
    Block,
}

impl<D> HasStoragePreference for ObjectPointer<D> {
    fn current_preference(&self) -> Option<StoragePreference> {
        Some(self.correct_preference())
    }

    fn recalculate(&self) -> StoragePreference {
        self.correct_preference()
    }

    fn correct_preference(&self) -> StoragePreference {
        StoragePreference::new(self.offset.storage_class())
    }

    /// There is no support in encoding storage preference right now.
    fn system_storage_preference(&self) -> StoragePreference {
        unimplemented!()
    }

    fn set_system_storage_preference(&mut self, _pref: StoragePreference) {
        unimplemented!()
    }
}

impl<D: StaticSize> StaticSize for ObjectPointer<D> {
    fn static_size() -> usize {
        <DecompressionTag as StaticSize>::static_size()
            + D::static_size()
            + DatasetId::static_size()
            + Generation::static_size()
            + <DiskOffset as StaticSize>::static_size()
            + Block::<u32>::static_size()
            + Block::<u32>::static_size()
            + std::mem::size_of::<usize>()
    }
}

impl<D> ObjectPointer<D> {
    /// Get the decompression tag.
    pub fn decompression_tag(&self) -> DecompressionTag {
        self.decompression_tag
    }
    /// Get a reference to the checksum of the target.
    pub fn checksum(&self) -> &D {
        &self.checksum
    }
    /// Get the disk location this object is stored at.
    pub fn offset(&self) -> DiskOffset {
        self.offset
    }

    /// Whether a node needs all data initially or a skeleton size can be deconstructed.
    /// FIXME: This needs to load data in large blocks right now.
    pub fn can_be_loaded_partial(&self) -> Option<Block<u32>> {
        self.metadata_size
            .map(|size| Block::round_up_from_bytes(size.get()))
    }

    /// Get the size in blocks of the serialized object.
    pub fn size(&self) -> Block<u32> {
        self.size
    }
    /// Get the generation this object reference is belonging to. Relevant for
    /// dataset snapshots.
    pub fn generation(&self) -> Generation {
        self.generation
    }
    /// Get the id of the dataset this object is part of.
    pub fn info(&self) -> DatasetId {
        self.info
    }
}
