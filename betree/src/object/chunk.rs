use std::iter;

/// 128 kibibyte
pub const CHUNK_SIZE: u32 = 128 * 1024;

pub const CHUNK_MAX: u32 = u32::MAX - 1024;
// pub const CHUNK_META_SIZE: u32 = CHUNK_MAX + 1;
// pub const CHUNK_META_MODIFICATION_TIME: u32 = CHUNK_MAX + 2;

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct ChunkOffset {
    pub chunk_id: u32,
    pub offset: u32,
}

impl ChunkOffset {
    /// The total byte offset, as it would be without chunking.
    pub fn as_bytes(&self) -> u64 {
        self.chunk_id as u64 * CHUNK_SIZE as u64 + self.offset as u64
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ChunkRange {
    pub start: ChunkOffset,
    pub end: ChunkOffset,
}

impl ChunkRange {
    pub fn from_byte_bounds(offset: u64, len: u64) -> Self {
        const SIZE: u64 = CHUNK_SIZE as u64;
        let end = offset + len;

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
        let mut range = self.clone();

        iter::from_fn(move || {
            // is this range empty?
            if range.start.chunk_id > range.end.chunk_id || range.start == range.end {
                return None;
            }

            // no, it's not. return remainder of current chunk and remove it from this range
            let last_chunk = range.start.chunk_id == range.end.chunk_id;
            let curr_start = range.start;
            range.start = ChunkOffset {
                chunk_id: curr_start.chunk_id + 1,
                offset: 0,
            };

            let remainder = ChunkRange {
                start: curr_start,
                end: ChunkOffset {
                    chunk_id: curr_start.chunk_id,
                    offset: if last_chunk {
                        range.end.offset
                    } else {
                        CHUNK_SIZE
                    },
                },
            };

            Some(remainder)
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
            ChunkRange::from_byte_bounds(2 * CHUNK_SIZE as u64 + 112, CHUNK_SIZE as u64 - 112)
                .split_at_chunk_bounds()
                .collect::<Vec<_>>(),
            vec![ChunkRange {
                start: ChunkOffset {
                    chunk_id: 2,
                    offset: 112
                },
                end: ChunkOffset {
                    chunk_id: 2,
                    offset: CHUNK_SIZE
                }
            },]
        );

        assert_eq!(
            ChunkRange::from_byte_bounds(1, 21 * CHUNK_SIZE as u64)
                .split_at_chunk_bounds()
                .count(),
            22
        );

        assert_eq!(
            ChunkRange::from_byte_bounds(1, 21 * CHUNK_SIZE as u64)
                .split_at_chunk_bounds()
                .map(|chunk| chunk.single_chunk_len())
                .sum::<u32>(),
            21 * CHUNK_SIZE
        );
    }
}
