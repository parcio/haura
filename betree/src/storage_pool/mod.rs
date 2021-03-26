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
use std::{fmt, io};

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
}

mod disk_offset;
pub use self::disk_offset::DiskOffset;

pub mod configuration;
pub use self::configuration::{StoragePoolConfiguration, TierConfiguration};

mod unit;
pub use self::unit::StoragePoolUnit;

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(transparent)]
pub struct StoragePreference(u8);
impl StoragePreference {
    pub const NONE: Self = Self(u8::MAX);
    pub const FASTEST: Self = Self(0);
    pub const FAST: Self = Self(1);
    pub const SLOW: Self = Self(2);
    pub const SLOWEST: Self = Self(3);

    pub fn new(class: u8) -> Self {
        Self(class)
    }

    pub fn or(self, other: Self) -> Self {
        if self == Self::NONE {
            other
        } else {
            self
        }
    }

    pub fn choose_faster(a: Self, b: Self) -> Self {
        // Only works if NONE stays large than any actual class
        Self(a.0.min(b.0))
    }

    pub fn preferred_class(self) -> Option<u8> {
        if self == Self::NONE {
            None
        } else {
            Some(self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn pref_choose_faster() {
        use super::StoragePreference as S;
        assert_eq!(S::choose_faster(S::SLOWEST, S::FASTEST), S::FASTEST);
        assert_eq!(S::choose_faster(S::FASTEST, S::NONE), S::FASTEST);
        assert_eq!(S::choose_faster(S::NONE, S::SLOWEST), S::SLOWEST);
    }
}
