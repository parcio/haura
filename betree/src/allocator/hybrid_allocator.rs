use super::*;
use std::cmp::min;

// Define pool configurations at compile time.
// Each tuple represents a pool: (SECTION_SIZE, POOL_PERCENTAGE)
// NOTE: Unfortunately Rust cannot infer the number of array elements, so change that, when adding
// or removing pools.
const POOL_CONFIGS: [(usize, f64); 3] = [(768, 0.80), (128, 0.05), (256, 0.05)];
const NUM_POOLS: usize = POOL_CONFIGS.len();

// Number elements/slots a pool has.
const POOL_ELEMENTS: [usize; NUM_POOLS] = {
    let mut arr = [0usize; NUM_POOLS];
    let mut i = 0;
    while i < NUM_POOLS {
        let tentative_blocks = (SEGMENT_SIZE as f64 * POOL_CONFIGS[i].1) as usize;
        arr[i] = tentative_blocks / POOL_CONFIGS[i].0;
        i += 1;
    }
    arr
};

// Number of 4KiB blocks each pool manages.
const POOL_BLOCKS_PER_POOL: [usize; NUM_POOLS] = {
    let mut arr = [0usize; NUM_POOLS];
    let mut i = 0;
    while i < NUM_POOLS {
        arr[i] = POOL_ELEMENTS[i] * POOL_CONFIGS[i].0;
        i += 1;
    }
    arr
};

// Sum of POOL_BLOCKS_PER_POOL.
const POOL_BLOCKS: usize = {
    let mut total_blocks = 0;
    let mut i = 0;
    while i < NUM_POOLS {
        total_blocks += POOL_BLOCKS_PER_POOL[i];
        i += 1;
    }
    total_blocks
};

// Offset where the blocks, that a pool manage, start in the global bitmap.
const POOL_OFFSET_START: [usize; NUM_POOLS] = {
    let mut arr = [0usize; NUM_POOLS];
    let mut current_offset = 0;
    let mut i = 0;
    while i < NUM_POOLS {
        arr[i] = current_offset;
        current_offset += POOL_BLOCKS_PER_POOL[i];
        i += 1;
    }
    arr
};

struct Pool {
    bitmap: BitVec<u8, Lsb0>,
    last_offset: usize,
    section_size: usize,
    elements: usize,
}

impl Pool {
    fn new(section_size: usize, elements: usize) -> Self {
        let bytes = (elements + 7) / 8;
        Pool {
            bitmap: BitVec::with_capacity(elements), // Initialize with capacity for performance
            last_offset: 0,
            section_size,
            elements,
        }
    }

    fn initialize_bitmap(
        &mut self,
        global_bitmap: &BitArr!(for SEGMENT_SIZE, in u8, Lsb0),
        pool_start_offset: usize,
    ) {
        self.bitmap.resize(self.elements, false); // Actually create the bits now
                                                  // Initialize the pool bitmap based on the global bitmap
        for i in 0..self.elements {
            let start = pool_start_offset + i * self.section_size;
            let end = pool_start_offset + (i + 1) * self.section_size;
            if global_bitmap[start..end].any() {
                self.bitmap.set(i, true);
            }
        }
    }

    fn allocate_section(&mut self) -> Option<u32> {
        // Next-Fit allocation within the pool
        for _ in 0..self.elements {
            if self.last_offset >= self.elements {
                self.last_offset = 0; // Wrap around
            }

            let offset = self.last_offset;
            self.last_offset += 1;

            if !self.bitmap[offset] {
                // Found free space.
                self.bitmap.set(offset, true);
                return Some(offset as u32);
            }
        }
        None
    }
}

/// Hybrid allocator with pools for different sized-block allocations and NextFitScan for the rest.
pub struct HybridAllocator {
    // Underlying bitmap of the allocator. The NextFitScan allocator works directly on this bitmap.
    data: BitArr!(for SEGMENT_SIZE, in u8, Lsb0),
    last_offset: u32,
    // Pools for fixed-size allocations, using a vector of Pools
    pools: [Pool; NUM_POOLS],
}

impl Allocator for HybridAllocator {
    fn data(&mut self) -> &mut BitArr!(for SEGMENT_SIZE, in u8, Lsb0) {
        &mut self.data
    }

    fn new(bitmap: [u8; SEGMENT_SIZE_BYTES]) -> Self
    where
        Self: Sized,
    {
        let data = BitArray::new(bitmap);
        let mut allocator = HybridAllocator {
            data,
            last_offset: POOL_BLOCKS as u32, // Next-fit starts after pools
            pools: {
                core::array::from_fn(|i| {
                    let section_size = POOL_CONFIGS[i].0;
                    let elements = POOL_ELEMENTS[i];
                    Pool::new(section_size, elements)
                })
            },
        };

        // Initialize pool bitmaps
        for i in 0..NUM_POOLS {
            let start_offset = POOL_OFFSET_START[i];
            allocator.pools[i].initialize_bitmap(&allocator.data, start_offset);
        }

        allocator
    }

    fn allocate(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return Some(0);
        }

        for i in 0..NUM_POOLS {
            if size as usize == self.pools[i].section_size {
                // Try to allocate from the pool.
                if let Some(pool_offset) = self.pools[i].allocate_section() {
                    // Found free space in pool.
                    let offset = (POOL_OFFSET_START[i] as u32
                        + pool_offset * self.pools[i].section_size as u32)
                        as u32;
                    self.mark(offset, size, Action::Allocate);
                    return Some(offset);
                }
                // Pool is full, break and use fallback.
                break; // Only check one pool if size matches.
            }
        }

        // Fallback to next-fit allocator for other sizes and if no space found in pools.
        let mut offset = self.last_offset;
        let mut wrap_around = false;

        loop {
            if offset + size > SEGMENT_SIZE as u32 {
                // Wrap around to the beginning of the segment.
                wrap_around = true;
            }

            if wrap_around && offset >= self.last_offset {
                // We've circled back to the starting point, no space found
                return None;
            }

            if offset + size > SEGMENT_SIZE as u32 {
                offset = POOL_BLOCKS as u32; // Start next-fit scan after pools
            }

            let start_idx = offset as usize;
            let end_idx = (offset + size) as usize;

            match self.data[start_idx..end_idx].last_one() {
                Some(last_alloc_idx) => {
                    // Skip to the end of the last allocated block if there is any one at all.
                    offset += last_alloc_idx as u32 + 1
                }
                None => {
                    // No allocated blocks found, so allocate here.
                    self.mark(offset, size, Action::Allocate);
                    self.last_offset = offset + size;
                    return Some(offset);
                }
            }
        }
    }

    fn allocate_at(&mut self, size: u32, offset: u32) -> bool {
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

        // Find all pools and elements of these pools, that intersect the allocation in some way
        // and mark them as allocated.
        for i in 0..NUM_POOLS {
            let pool = &mut self.pools[i];
            let pool_start = POOL_OFFSET_START[i] as u32;
            let pool_end = pool_start + POOL_BLOCKS_PER_POOL[i] as u32;
            if offset >= pool_start && offset < pool_end {
                let pool_section_offset = (offset - pool_start) as usize / pool.section_size;
                let pool_section_size = (size as usize / pool.section_size) + 1;

                let pool_section_end =
                    min(pool_section_size + pool_section_offset, pool.bitmap.len());

                pool.bitmap[pool_section_offset..pool_section_size].fill(true);
            }
        }

        true
    }
}
