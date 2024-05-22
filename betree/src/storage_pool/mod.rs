//! This module provides the `StoragePoolLayer`
//! which manages vdevs and features a write-back queue with read-write
//! ordering.

use crate::{
    buffer::Buf,
    checksum::Checksum,
    vdev::{Block, Error as VdevError, Result as VdevResult},
};
use futures::{executor::block_on, prelude::*, TryFuture};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt;

pub mod errors;
pub use self::errors::*;
use errors::Result as StoragePoolResult;

// TODO read-only storage pool layer?
// TODO Clone necessary?

/// A `StoragePoolLayer` which manages vdevs and features a write-back queue
pub trait StoragePoolLayer: Clone + Send + Sync + 'static {
    /// The checksum type used for verifying the data integrity.
    type Checksum: Checksum;

    /// A serializable configuration type for this `StoragePoolLayer` object.
    type Configuration: Serialize + DeserializeOwned + fmt::Debug;

    /// A serializable type to be returned by the `metrics` function,
    /// describing interesting statistics of this layer.
    /// Can be (), if none are available.
    type Metrics: Serialize;

    /// Constructs a new object using the given `Configuration`.
    fn new(configuration: &Self::Configuration) -> StoragePoolResult<Self>;

    /// Reads `size` blocks from the given `offset`.
    fn read(
        &self,
        size: Block<u32>,
        offset: DiskOffset,
        checksum: Self::Checksum,
    ) -> VdevResult<Buf> {
        block_on(self.read_async(size, offset, checksum)?.into_future())
    }

    /// Extract a slice from a memory region.
    fn slice(&self, offset: DiskOffset, start: usize, end: usize) -> VdevResult<&'static [u8]> {
        block_on(self.get_slice(offset, start, end)?.into_future())
    }

    /// A future yielding a reference to a byte range. This is valid as long as
    /// the underlying memory is present.
    type SliceAsync: TryFuture<Ok = &'static [u8], Error = VdevError> + Send;

    /// Fetch a reference to a slice from the specified disk block. This is only
    /// valid when used on memory represented vdevs.
    fn get_slice(
        &self,
        offset: DiskOffset,
        start: usize,
        end: usize,
    ) -> VdevResult<Self::SliceAsync>;

    /// Future returned by `read_async`.
    type ReadAsync: TryFuture<Ok = Buf, Error = VdevError> + Send;

    /// Reads `size` blocks asynchronously from the given `offset`.
    fn read_async(
        &self,
        size: Block<u32>,
        offset: DiskOffset,
        checksum: Self::Checksum,
    ) -> VdevResult<Self::ReadAsync>;

    /// Issues a write request that might happen in the background.
    fn begin_write(&self, data: Buf, offset: DiskOffset) -> VdevResult<()>;

    /// Writes the given `data` at `offset` for every `LeafVdev`.
    fn write_raw(&self, data: Buf, offset: Block<u64>) -> VdevResult<()>;

    /// Reads `size` blocks from  the given `offset` for every `LeafVdev`.
    fn read_raw(&self, size: Block<u32>, offset: Block<u64>) -> VdevResult<Vec<Buf>>;

    /// Returns the actual size of a data block for a specific `Vdev`
    /// which may be larger due to parity data.
    fn actual_size(&self, storage_class: u8, disk_id: u16, size: Block<u32>) -> Block<u32>;

    /// Returns the size for a specific `Vdev`.
    fn size_in_blocks(&self, storage_class: u8, disk_id: u16) -> Block<u64>;

    /// Return the number of leaf vdevs for a specific `Vdev`.
    fn num_disks(&self, storage_class: u8, disk_id: u16) -> usize;

    /// Returns the effective free size for a specific `Vdev`.
    fn effective_free_size(
        &self,
        storage_class: u8,
        disk_id: u16,
        free_size: Block<u64>,
    ) -> Block<u64>;

    /// Returns the number of `Vdev`s.
    fn disk_count(&self, storage_class: u8) -> u16;

    /// Returns the number of storage classes.
    fn storage_class_count(&self) -> u8;

    /// Flushes the write-back queue and the underlying storage backend.
    fn flush(&self) -> VdevResult<()>;

    /// Gather layer-specific metrics.
    fn metrics(&self) -> Self::Metrics;

    /// Return a fitting [StoragePreference] to the given [PreferredAccessType].
    fn access_type_preference(&self, t: PreferredAccessType) -> StoragePreference;
}

mod disk_offset;
pub use self::disk_offset::{DiskOffset, GlobalDiskId, LocalDiskId};

pub mod configuration;
pub use self::configuration::{
    LeafVdev, PreferredAccessType, StoragePoolConfiguration, TierConfiguration, Vdev,
};

mod unit;
pub use self::unit::StoragePoolUnit;

mod storage_preference;
pub(crate) use storage_preference::AtomicSystemStoragePreference;
pub use storage_preference::{AtomicStoragePreference, StoragePreference};

/// The amount of storage classes.
pub const NUM_STORAGE_CLASSES: usize = 4;
