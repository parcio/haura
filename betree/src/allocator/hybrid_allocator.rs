use super::*;

const POOL_PERCENTAGE: f64 = 0.75;
const SECTION_SIZE: usize = 768;

// Only tentative, because this is not guaranteed to be a multiple of `SECTION_SIZE`.
const POOL_BLOCKS_TENTATIVE: usize = (SEGMENT_SIZE as f64 * POOL_PERCENTAGE) as usize;

const POOL_ELEMENTS: usize = POOL_BLOCKS_TENTATIVE / SECTION_SIZE;
const POOL_BITMAP_BYTES: usize = POOL_ELEMENTS / 8;
const POOL_BLOCKS: usize = POOL_ELEMENTS * SECTION_SIZE;

struct Pool<const SECTION_SIZE: usize, const ELEMENTS: usize, const BYTES: usize> {
    bitmap: BitArray<[u8; BYTES], Lsb0>,
    last_offset: usize,
}

impl<const SECTION_SIZE: usize, const ELEMENTS: usize, const BYTES: usize>
    Pool<SECTION_SIZE, ELEMENTS, BYTES>
{
    fn new(bitmap: BitArr!(for SEGMENT_SIZE, in u8, Lsb0)) -> Self {
        let mut pool = Pool {
            bitmap: BitArray::new([0u8; BYTES]),
            last_offset: 0,
        };

        // Initialize the pool bitmap
        for i in 0..ELEMENTS {
            let start = i * SECTION_SIZE;
            let end = (i + 1) * SECTION_SIZE;
            if bitmap[start..end].any() {
                pool.bitmap.set(i, true);
            }
        }

        pool
    }

    fn allocate_section(&mut self) -> Option<u32> {
        // Next-Fit allocation within the pool
        for _ in 0..ELEMENTS {
            if self.last_offset >= ELEMENTS {
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

/// Hybrid allocator with a pool for `SECTION_SIZE`d-block allocations and NextFitScan for the rest.
pub struct HybridAllocator {
    // Underlying bitmap of the allocator. The NextFitScan allocator works directly on this bitmap.
    data: BitArr!(for SEGMENT_SIZE, in u8, Lsb0),
    last_offset: u32,
    // Bitmap for the pool, one bit manages one section
    pool: Pool<SECTION_SIZE, POOL_ELEMENTS, POOL_BITMAP_BYTES>,
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
            last_offset: POOL_BLOCKS as u32,
            pool: Pool::new(data),
        };
        allocator
    }

    fn allocate(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return Some(0);
        }

        if size == SECTION_SIZE as u32 {
            // Try to allocate from the pool.
            if let Some(pool_offset) = self.pool.allocate_section() {
                // Found free space.
                let offset = pool_offset * SEGMENT_SIZE as u32;
                self.mark(offset, size, Action::Allocate);
                return Some(offset);
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
            self.pool.bitmap.set(offset as usize / SECTION_SIZE, true);
        }

        true
    }
}
