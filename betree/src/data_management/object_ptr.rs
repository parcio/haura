use super::{errors::*, CopyOnWriteEvent, DmlBase, HasStoragePreference, Object, PodType};
use crate::{
    allocator::{Action, SegmentAllocator, SegmentId},
    buffer::Buf,
    cache::{AddSize, Cache, ChangeKeyError, RemoveError},
    checksum::{Builder, Checksum, State},
    compression::{CompressionBuilder, DecompressionTag},
    data_management::CopyOnWriteReason,
    database::{DatasetId, Generation, Handler, Object as FixObject},
    migration::ConstructReport,
    size::{Size, SizeMut, StaticSize},
    storage_pool::{DiskOffset, StoragePoolLayer, NUM_STORAGE_CLASSES},
    vdev::{Block, BLOCK_SIZE},
    StoragePreference,
};
use serde::{
    de::DeserializeOwned, ser::Error as SerError, Deserialize, Deserializer, Serialize, Serializer,
};


#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
/// A pointer to an on-disk serialized object.
pub struct ObjectPointer<D> {
    decompression_tag: DecompressionTag,
    checksum: D,
    offset: DiskOffset,
    size: Block<u32>,
    info: DatasetId,
    generation: Generation,
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

impl<D> From<ObjectPointer<D>> for ObjectRef<ObjectPointer<D>> {
    fn from(ptr: ObjectPointer<D>) -> Self {
        ObjectRef::Unmodified(ptr)
    }
}

impl<D> ObjectPointer<D> {
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
