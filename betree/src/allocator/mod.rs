use crate::{cow_bytes::CowBytes, storage_pool::DiskOffset, vdev::Block};
use bitvec::prelude::*;
use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};

mod first_fit;
pub use self::first_fit::FirstFit;

mod segment_allocator;
pub use self::segment_allocator::SegmentAllocator;

/// 256KiB, so that `vdev::BLOCK_SIZE * SEGMENT_SIZE == 1GiB`
pub const SEGMENT_SIZE: usize = 1 << SEGMENT_SIZE_LOG_2;
/// Number of bytes required to store a segments allocation bitmap
pub const SEGMENT_SIZE_BYTES: usize = SEGMENT_SIZE / 8;
const SEGMENT_SIZE_LOG_2: usize = 18;
const SEGMENT_SIZE_MASK: usize = SEGMENT_SIZE - 1;

/// The `AllocatorType` enum represents different strategies for allocating blocks
/// of memory within a fixed-size segment.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")] // This will deserialize "firstfit" as FirstFit
pub enum AllocatorType {
    /// **First Fit:**
    /// This allocator searches the segment from the beginning and allocates the
    /// first free block that is large enough to satisfy the request.
    FirstFit,

    /// **Segment Allocator:**
    /// This is a first fit allocator that was used before making the allocators
    /// generic. It is not efficient and mainly included for reference.
    SegmentAllocator,
}

/// The `Allocator` trait defines an interface for allocating and deallocating
/// blocks of memory within a fixed-size segment. Different allocators can
/// implement various strategies for managing free space within the segment.
pub trait Allocator: Send + Sync {
    /// Accesses the underlying bitmap data.
    fn data(&mut self) -> &mut BitArr!(for SEGMENT_SIZE, in u8, Lsb0);

    /// Constructs a new `Allocator` instance given the segment allocation bitmap.
    /// The `bitmap` must have a length of `SEGMENT_SIZE`.
    fn new(bitmap: [u8; SEGMENT_SIZE_BYTES]) -> Self
    where
        Self: Sized;

    /// Allocates a block of memory of the given `size`.
    ///
    /// This method attempts to find a contiguous free block of memory within the
    /// segment that is large enough to satisfy the request.
    fn allocate(&mut self, size: u32) -> Option<u32>;

    /// Allocates a block of memory of the given `size` at the specified `offset`.
    ///
    /// This method attempts to allocate a contiguous block of memory at the given offset.
    fn allocate_at(&mut self, size: u32, offset: u32) -> bool;

    /// Deallocates a previously allocated block of memory.
    ///
    /// This method marks the specified block as free, making it available for
    /// future allocations.
    fn deallocate(&mut self, offset: u32, size: u32);

    /// Marks a range of bits in the bitmap with the given action.
    fn mark(&mut self, offset: u32, size: u32, action: Action) {
        let start_idx = offset as usize;
        let end_idx = (offset + size) as usize;
        let range = &mut self.data()[start_idx..end_idx];

        match action {
            Action::Allocate => debug_assert!(!range.any()),
            Action::Deallocate => debug_assert!(range.all()),
        }

        range.fill(action.as_bool());
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    use rand::{
        distributions::WeightedIndex, prelude::Distribution, rngs::StdRng, Rng, SeedableRng,
    };

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

    fn run_fuzz_tests<T: Allocator>(allocator: &mut T) {
        let mut rng = StdRng::seed_from_u64(0);

        // Store allocation records (offset, size)
        let mut allocations: Vec<(u32, u32)> = Vec::new();

        // Define action weights (allocate: 60%, deallocate: 30%, allocate_at: 10%)
        let weights = [60, 30, 10];
        let dist = WeightedIndex::new(&weights).unwrap();

        for _ in 0..100000 {
            let action = dist.sample(&mut rng);
            match action {
                0 => {
                    // Allocate
                    let size = rng.gen_range(1..256);
                    if let Some(offset) = allocator.allocate(size) {
                        allocations.push((offset, size));
                    }
                }
                1 => {
                    // Deallocate
                    if !allocations.is_empty() {
                        let index = rng.gen_range(0..allocations.len());
                        let (offset, size) = allocations.swap_remove(index); // Remove the chosen allocation
                        allocator.deallocate(offset, size);
                    }
                }
                2 => {
                    // Allocate at
                    let offset = rng.gen_range(0..(SEGMENT_SIZE * 2) as u32);
                    let size = rng.gen_range(1..256);
                    if allocator.allocate_at(offset, size) {
                        allocations.push((offset, size)); // Store if successful
                    }
                }
                _ => unreachable!(),
            }
        }

        // TODO: find some generic assertions
    }

    macro_rules! generate_fuzz_tests {
        ($test_name:ident, $allocator_type:ty) => {
            #[test]
            fn $test_name() {
                let mut allocator = <$allocator_type>::new([0; SEGMENT_SIZE_BYTES]);
                run_fuzz_tests(&mut allocator);
            }
        };
    }

    fn small_tests<T: Allocator>(allocator: &mut T) {
        let offset = allocator.allocate(10);
        assert!(offset.is_some());

        let offset = offset.unwrap();
        allocator.deallocate(offset, 10);

        let success = allocator.allocate_at(5, 10);
        assert!(success);

        // Allocation Failure
        let offset = allocator.allocate(SEGMENT_SIZE as u32); // Assuming the entire segment isn't free
        assert!(offset.is_none());

        // Out-of-bounds
        let success = allocator.allocate_at(SEGMENT_SIZE as u32, 1);
        assert!(!success);

        // Zero-sized allocation
        let offset = allocator.allocate(0);
        assert!(offset.is_some());

        // Repeated Allocation/Deallocation
        for _ in 0..10 {
            let offset = allocator.allocate(12).unwrap();
            allocator.deallocate(offset, 12);
        }

        // 8.  Large Allocation Followed by Small Allocations
        let large_offset = allocator.allocate(100).unwrap();
        _ = allocator.allocate(4).unwrap();
        _ = allocator.allocate(7).unwrap();
        allocator.deallocate(large_offset, 100);
    }

    macro_rules! generate_small_tests {
        ($test_name:ident, $allocator_type:ty) => {
            #[test]
            fn $test_name() {
                let mut allocator = <$allocator_type>::new([0; SEGMENT_SIZE_BYTES]);
                small_tests(&mut allocator);
            }
        };
    }

    // Generate tests for each allocator
    generate_small_tests!(test_first_fit, FirstFit);
    generate_small_tests!(test_segment_allocator, SegmentAllocator);

    // Generate fuzz tests for each allocator
    generate_fuzz_tests!(test_first_fit_fuzz, FirstFit);
    generate_fuzz_tests!(test_segment_allocator_fuzz, SegmentAllocator);
}
