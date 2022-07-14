//! This module provides `SegmentAllocator` and `SegmentId` for bitmap
//! allocation of 1GiB segments.

use crate::{cow_bytes::CowBytes, storage_pool::DiskOffset, vdev::Block};
use bitvec::prelude::*;
use byteorder::{BigEndian, ByteOrder};

/// 256KiB, so that `vdev::BLOCK_SIZE * SEGMENT_SIZE == 1GiB`
pub const SEGMENT_SIZE: usize = 1 << SEGMENT_SIZE_LOG_2;
/// Number of bytes required to store a segments allocation bitmap
pub const SEGMENT_SIZE_BYTES: usize = SEGMENT_SIZE / 8;
const SEGMENT_SIZE_LOG_2: usize = 18;
const SEGMENT_SIZE_MASK: usize = SEGMENT_SIZE - 1;

/// Simple first-fit bitmap allocator
pub struct SegmentAllocator {
    data: BitArr!(for SEGMENT_SIZE, in Lsb0, u8),
}

impl SegmentAllocator {
    /// Constructs a new `SegmentAllocator` given the segment allocation bitmap.
    /// The `bitmap` must have a length of `SEGMENT_SIZE`.
    pub fn new(bitmap: [u8; SEGMENT_SIZE_BYTES]) -> Self {
        SegmentAllocator {
            data: BitArray::new(bitmap),
        }
    }

    /// Allocates a block of the given `size`.
    /// Returns `None` if the allocation request cannot be satisfied.
    pub fn allocate(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return Some(0);
        }
        let offset = {
            let mut idx = 0;
            loop {
                loop {
                    if idx + size > SEGMENT_SIZE as u32 {
                        return None;
                    }
                    if !self.data[idx as usize] {
                        break;
                    }
                    idx += 1;
                }

                let start_idx = (idx + 1) as usize;
                let end_idx = (idx + size) as usize;
                if let Some(first_alloc_idx) = self.data[start_idx..end_idx].first_one() {
                    idx = (idx + 1) + first_alloc_idx as u32 + 1;
                } else {
                    break idx as u32;
                }
            }
        };
        self.mark(offset, size, Action::Allocate);
        Some(offset)
    }

    /// Allocates a block of the given `size` at `offset`.
    /// Returns `false` if the allocation request cannot be satisfied.
    pub fn allocate_at(&mut self, size: u32, offset: u32) -> bool {
        if size == 0 {
            return true;
        }
        if offset + size > SEGMENT_SIZE as u32 {
            return false;
        }

        let start_idx = offset as usize;
        let end_idx = (offset + size) as usize;
        if self.data[start_idx..end_idx].any() {
            return false;
        }
        self.mark(offset, size, Action::Allocate);
        true
    }

    /// Deallocates the allocated block.
    pub fn deallocate(&mut self, offset: u32, size: u32) {
        log::debug!(
            "Marked a block range {{ offset: {}, size: {} }} for deallocation",
            offset,
            size
        );
        self.mark(offset, size, Action::Deallocate);
    }

    fn mark(&mut self, offset: u32, size: u32, action: Action) {
        let start_idx = offset as usize;
        let end_idx = (offset + size) as usize;
        let range = &mut self.data[start_idx..end_idx];

        match action {
            // Is allocation, so range must be free
            Action::Allocate => debug_assert!(!range.any()),
            // Is deallocation, so range must be previously used
            Action::Deallocate => debug_assert!(range.all()),
        }

        range.set_all(action.as_bool());
    }
}

// TODO better wording
/// Allocation action
#[derive(Clone, Copy)]
pub enum Action {
    /// Deallocate an allocated block.
    Deallocate,
    /// Allocate a deallocated block.
    Allocate,
}

impl Action {
    /// Returns 1 if allocation and 0 if deallocation.
    pub fn as_bool(self) -> bool {
        match self {
            Action::Deallocate => false,
            Action::Allocate => true,
        }
    }
}

/// Identifier for 1GiB segments of a `StoragePool`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentId(pub u64);

impl SegmentId {
    /// Returns the corresponding segment of the given disk offset.
    pub fn get(offset: DiskOffset) -> Self {
        SegmentId(offset.as_u64() & !(SEGMENT_SIZE_MASK as u64))
    }

    /// Returns the block offset into the segment.
    pub fn get_block_offset(offset: DiskOffset) -> u32 {
        offset.as_u64() as u32 & SEGMENT_SIZE_MASK as u32
    }

    /// Returns the disk offset at the start of this segment.
    pub fn as_disk_offset(&self) -> DiskOffset {
        DiskOffset::from_u64(self.0)
    }

    /// Returns the disk offset of the block in this segment at the given
    /// offset.
    pub fn disk_offset(&self, segment_offset: u32) -> DiskOffset {
        DiskOffset::from_u64(self.0 + u64::from(segment_offset))
    }

    /// Returns the key of this segment for messages and queries.
    pub fn key(&self, key_prefix: &[u8]) -> CowBytes {
        // Shave off the two lower bytes because they are always null.
        let mut segment_key = [0; 8];
        BigEndian::write_u64(&mut segment_key[..], self.0);
        assert_eq!(&segment_key[6..], &[0, 0]);

        let mut key = CowBytes::new();
        key.push_slice(key_prefix);
        key.push_slice(&segment_key[..6]);
        key
    }

    /// Returns the ID of the disk that belongs to this segment.
    pub fn disk_id(&self) -> u16 {
        self.as_disk_offset().disk_id()
    }

    /// Returns the next segment ID.
    /// Wraps around at the end of the disk.
    pub fn next(&self, disk_size: Block<u64>) -> SegmentId {
        let disk_offset = self.as_disk_offset();
        if disk_offset.block_offset().as_u64() + SEGMENT_SIZE as u64 >= disk_size.as_u64() {
            SegmentId::get(DiskOffset::new(
                disk_offset.storage_class(),
                disk_offset.disk_id(),
                Block(0),
            ))
        } else {
            SegmentId(self.0 + SEGMENT_SIZE as u64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_id() {
        let offset = DiskOffset::new(1, 2, Block::from_bytes(4096));
        let segment = SegmentId::get(offset);

        assert_eq!(segment.as_disk_offset().storage_class(), 1);
        assert_eq!(segment.disk_id(), 2);
        assert_eq!(
            segment.as_disk_offset().block_offset(),
            Block::from_bytes(0)
        );
        assert_eq!(SegmentId::get_block_offset(offset), 1);
    }
}
