use std::iter;

/// 128 kibibyte
//pub const CHUNK_SIZE: u32 = 128 * 1024;

pub const CHUNK_MAX: u32 = u32::MAX - 1024;
// pub const CHUNK_META_SIZE: u32 = CHUNK_MAX + 1;
// pub const CHUNK_META_MODIFICATION_TIME: u32 = CHUNK_MAX + 2;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct ChunkOffset {
    pub chunk_id: u32,
    pub offset: u32,
}

impl ChunkOffset {
    /// The total byte offset, as it would be without chunking.
    pub fn as_bytes(&self) -> u64 {
        self.chunk_id as u64 * unsafe{crate::g_CHUNK_SIZE} as u64 + self.offset as u64
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ChunkRange {
    pub start: ChunkOffset,
    pub end: ChunkOffset,
}

impl ChunkRange {
    pub fn from_byte_bounds(offset: u64, len: u64) -> Self {
        let SIZE: u64 = unsafe{crate::g_CHUNK_SIZE} as u64;
        let end = offset.saturating_add(len);

        ChunkRange {
            start: ChunkOffset {
                chunk_id: (offset / SIZE) as u32,
                offset: (offset % SIZE) as u32,
            },

            end: ChunkOffset {
                chunk_id: (end / SIZE) as u32,
                offset: (end % SIZE) as u32,
            },
        }
    }

    fn is_single_chunk(&self) -> bool {
        self.start.chunk_id == self.end.chunk_id
    }

    pub fn single_chunk_len(&self) -> u32 {
        debug_assert!(self.is_single_chunk());
        debug_assert!(self.end.offset >= self.start.offset);
        self.end.offset - self.start.offset
    }

    pub fn split_at_chunk_bounds(&self) -> impl Iterator<Item = ChunkRange> {
        let mut range_start = Some(self.start);
        let end = self.end;

        iter::from_fn(move || {
            if let Some(start) = range_start {
                // is this range empty?
                if start.chunk_id > end.chunk_id || start == end {
                    return None;
                }

                // no, it's not. return remainder of current chunk and remove it from this range
                let last_chunk = start.chunk_id == end.chunk_id;
                let curr_start = start;

                // chunk_id increment could overflow, but we still need to return this remainder,
                // even if it does
                range_start = curr_start
                    .chunk_id
                    .checked_add(1)
                    .map(|chunk_id| ChunkOffset {
                        chunk_id,
                        offset: 0,
                    });

                let remainder = ChunkRange {
                    start: curr_start,
                    end: ChunkOffset {
                        chunk_id: curr_start.chunk_id,
                        offset: if last_chunk { end.offset } else { unsafe{crate::g_CHUNK_SIZE} },
                    },
                };

                Some(remainder)
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_chunk_empty() {
        let chunk_range = ChunkRange::from_byte_bounds(42, 0);
        assert_eq!(chunk_range.start, chunk_range.end);
        assert_eq!(chunk_range.split_at_chunk_bounds().count(), 0);
    }

    #[test]
    fn test_chunk_bounds() {
        let chunk_range = ChunkRange::from_byte_bounds(3120312, 12836187);
        assert_eq!(
            chunk_range.start,
            ChunkOffset {
                chunk_id: 23,
                offset: 105656
            }
        );
        assert_eq!(
            chunk_range.end,
            ChunkOffset {
                chunk_id: 121,
                offset: 96787
            }
        );
    }

    #[test]
    fn test_chunk_iter() {
        let chunk_range = ChunkRange::from_byte_bounds(20, 200);
        assert_eq!(chunk_range.split_at_chunk_bounds().count(), 1);

        assert_eq!(
            ChunkRange::from_byte_bounds(2 * unsafe{crate::g_CHUNK_SIZE} as u64 + 112, unsafe{crate::g_CHUNK_SIZE} as u64 - 112)
                .split_at_chunk_bounds()
                .collect::<Vec<_>>(),
            vec![ChunkRange {
                start: ChunkOffset {
                    chunk_id: 2,
                    offset: 112
                },
                end: ChunkOffset {
                    chunk_id: 2,
                    offset: unsafe{crate::g_CHUNK_SIZE}
                }
            },]
        );

        assert_eq!(
            ChunkRange::from_byte_bounds(1, 21 * unsafe{crate::g_CHUNK_SIZE} as u64)
                .split_at_chunk_bounds()
                .count(),
            22
        );

        assert_eq!(
            ChunkRange::from_byte_bounds(1, 21 * unsafe{crate::g_CHUNK_SIZE} as u64)
                .split_at_chunk_bounds()
                .map(|chunk| chunk.single_chunk_len())
                .sum::<u32>(),
            21 * unsafe{crate::g_CHUNK_SIZE}
        );
    }
}
