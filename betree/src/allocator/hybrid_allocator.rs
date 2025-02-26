use super::*;

const POOL_PERCENTAGE: f64 = 0.75;
const SECTION_SIZE: usize = 768;

// Only tentative, because this is not guaranteed to be a multiple of `SECTION_SIZE`.
const POOL_BLOCKS_TENTATIVE: usize = (SEGMENT_SIZE as f64 * POOL_PERCENTAGE) as usize;

const POOL_ELEMENTS: usize = POOL_BLOCKS_TENTATIVE / SECTION_SIZE;
const POOL_BITMAP_BYTES: usize = POOL_ELEMENTS / 8;
const POOL_BLOCKS: usize = POOL_ELEMENTS * SECTION_SIZE;

/// Hybrid allocator with a pool for `SECTION_SIZE`d-block allocations and NextFitScan for the rest.
pub struct HybridAllocator {
    // Underlying bitmap of the allocator. The NextFitScan allocator works directly on this bitmap.
    data: BitArr!(for SEGMENT_SIZE, in u8, Lsb0),
    // Bitmap for the pool, one bit manages one section
    pool_bitmap: BitArr!(for POOL_ELEMENTS, in u8, Lsb0),
    last_offset_pool: usize,
    last_offset: u32,
}

impl Allocator for HybridAllocator {
    fn data(&mut self) -> &mut BitArr!(for SEGMENT_SIZE, in u8, Lsb0) {
        &mut self.data
    }

    fn new(bitmap: [u8; SEGMENT_SIZE_BYTES]) -> Self
    where
        Self: Sized,
    {
        let mut allocator = HybridAllocator {
            data: BitArray::new(bitmap),
            pool_bitmap: BitArray::new([0u8; POOL_BITMAP_BYTES]),
            last_offset: POOL_BLOCKS as u32,
            last_offset_pool: 0,
        };
        allocator.initialize_pool();
        allocator
    }

    fn allocate(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return Some(0);
        }

        if size == SECTION_SIZE as u32 {
            // Try to allocate from the pool using Next-Fit within the pool.
            for _ in 0..POOL_ELEMENTS {
                // Iterate through all pool elements for Next-Fit
                if self.last_offset_pool >= POOL_ELEMENTS {
                    self.last_offset_pool = 0; // Wrap around
                }

                let pool_offset_index = self.last_offset_pool;

                if !self.pool_bitmap[pool_offset_index] {
                    self.pool_bitmap.set(pool_offset_index, true);
                    let offset = (pool_offset_index * SECTION_SIZE) as u32;
                    self.mark(offset, size, Action::Allocate);
                    self.last_offset_pool += 1;
                    return Some(offset);
                }

                self.last_offset_pool += 1;
            }
        }

        // Fallback to next-fit allocator for other sizes and if no space found.
        let mut offset = self.last_offset;
        let mut wrap_around = false;

        loop {
            if offset + size > SEGMENT_SIZE as u32 {
                // Wrap around to the beginning of the segment.
                // NOTE: We **can't** set offset here.
                wrap_around = true;
            }

            if wrap_around && offset >= self.last_offset {
                // We've circled back to the starting point, no space found
                return None;
            }

            if offset + size > SEGMENT_SIZE as u32 {
                // NOTE: We **can't** set offset above because of the case if self.last_offset is
                // larger than SEGMENT_SIZE - size. If it is and we would set offset above we would
                // run into an infinite loop. Because the `offset >= self.last_offset` condition
                // would never be fulfilled because offset is reset beforehand.
                offset = POOL_BLOCKS as u32;
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
                    self.last_offset = offset + size + 1;
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

        // Mark the correct bit in the pool as allocated if the offset is in the pool.
        if offset < POOL_BLOCKS as u32 {
            self.pool_bitmap.set(offset as usize / SECTION_SIZE, true);
        }

        true
    }
}

impl HybridAllocator {
    fn initialize_pool(&mut self) {
        // Initialize the pool bitmap
        for i in 0..POOL_ELEMENTS {
            if self.data[i * SECTION_SIZE..(i + 1) * SECTION_SIZE].any() {
                self.pool_bitmap.set(i, true);
            }
        }
    }
}
