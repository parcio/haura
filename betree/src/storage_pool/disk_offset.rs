use crate::{size::StaticSize, vdev::Block};
use serde::{Deserialize, Serialize};
use std::{fmt, mem};

/// 2-bit storage class, 10-bit disk ID, 52-bit block offset (see
/// [`BLOCK_SIZE`](../vdev/constant.BLOCK_SIZE.html))
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive(check_bytes)]
pub struct DiskOffset(u64);

impl std::fmt::Display for DiskOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "Offset({},{},{})",
            self.storage_class(),
            self.disk_id(),
            self.block_offset()
        ))
    }
}

const MASK_STORAGE_CLASS: u64 = ((1 << 2) - 1) << (10 + 52);
const MASK_DISK_ID: u64 = ((1 << 10) - 1) << 52;
const MASK_OFFSET: u64 = (1 << 52) - 1;
const MASK_CLASS_DISK_ID_COMBINED: u64 = ((1 << 12) - 1) << 52;

/// An identifier containing the class id and disk id. Uniquely identifies a
/// disk over all storage devices.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalDiskId(pub u16);

impl GlobalDiskId {
    /// Expose the internal representation of the identifier.
    pub fn as_u16(&self) -> u16 {
        self.0
    }
}

/// A class specific disk identifier. Only unique within a set class and only
/// valid for the original class.
pub struct LocalDiskId(u16);

impl DiskOffset {
    /// Constructs a new `DiskOffset`.
    /// The given `block_offset` may not be larger than (1 << 52) - 1.
    pub fn new(storage_class: u8, disk_id: u16, block_offset: Block<u64>) -> Self {
        let block_offset = block_offset.as_u64();
        assert_eq!(
            block_offset & !MASK_OFFSET,
            0,
            "the block offset is too large"
        );
        DiskOffset(((storage_class as u64) << (52 + 10)) | ((disk_id as u64) << 52) | block_offset)
    }
    /// Returns the 2-bit storage class.
    pub fn storage_class(&self) -> u8 {
        ((self.0 & MASK_STORAGE_CLASS) >> (52 + 10)) as u8
    }
    /// Returns the 10-bit disk ID.
    pub fn disk_id(&self) -> u16 {
        ((self.0 & MASK_DISK_ID) >> 52) as u16
    }
    /// Returns the 12-bit storage class with attached disk ID.
    pub fn class_disk_id(&self) -> GlobalDiskId {
        GlobalDiskId(((self.0 & MASK_CLASS_DISK_ID_COMBINED) >> 52) as u16)
    }
    /// Returns the block offset.
    pub fn block_offset(&self) -> Block<u64> {
        Block(self.0 & MASK_OFFSET)
    }
    /// Returns this object as `u64`.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
    /// Constructs a disk offset from the given `u64`.
    pub fn from_u64(x: u64) -> Self {
        DiskOffset(x)
    }

    // Glue together a class identifier with a class depdendent disk_id.
    pub fn construct_disk_id(class: u8, disk_id: u16) -> GlobalDiskId {
        GlobalDiskId(((class as u16) << 10) | disk_id)
    }
}

impl StaticSize for DiskOffset {
    fn static_size() -> usize {
        mem::size_of::<u64>()
    }
}

impl fmt::Debug for DiskOffset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DiskOffset")
            .field("storage_class", &self.storage_class())
            .field("disk_id", &self.disk_id())
            .field("block_offset", &self.block_offset())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks() {
        assert_eq!(!0u64, MASK_STORAGE_CLASS | MASK_DISK_ID | MASK_OFFSET);
        assert_eq!(0, MASK_STORAGE_CLASS & MASK_DISK_ID);
        assert_eq!(0, MASK_DISK_ID & MASK_OFFSET);
        assert_eq!(0, MASK_STORAGE_CLASS & MASK_OFFSET);
    }

    #[test]
    fn round_trip() {
        let o = DiskOffset::new(1, 42, Block::from_bytes(4096 * 189631));
        assert_eq!(o.storage_class(), 1);
        assert_eq!(o.disk_id(), 42);
        assert_eq!(o.block_offset().to_bytes(), 4096 * 189631);
    }
}
