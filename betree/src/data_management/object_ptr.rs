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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
/// A pointer to an on-disk serialized object.
pub struct ObjectPointer<D> {
    pub(super) decompression_tag: DecompressionTag,
    pub(super) checksum: D,
    pub(super) offset: DiskOffset,
    pub(super) size: Block<u32>,
    pub(super) info: DatasetId,
    pub(super) generation: Generation,
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

    // There is no support in encoding storage preference right now.

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
            + 4
    }
}

impl<D> ObjectPointer<D> {
    pub fn decompression_tag(&self) -> DecompressionTag {
        self.decompression_tag
    }
    pub fn checksum(&self) -> &D {
        &self.checksum
    }
    pub fn offset(&self) -> DiskOffset {
        self.offset
    }
    pub fn size(&self) -> Block<u32> {
        self.size
    }
    pub fn generation(&self) -> Generation {
        self.generation
    }
    pub fn info(&self) -> DatasetId {
        self.info
    }
}
