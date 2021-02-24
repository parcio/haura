use super::leaf::LeafNode;
use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    size::Size,
};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use std::{
    cmp,
    io::{self, Write},
    mem::{align_of, size_of},
    slice::{from_raw_parts, from_raw_parts_mut},
};

// account for trailing fake element
pub(crate) const HEADER_FIXED_LEN: usize = size_of::<Header>() + HEADER_PER_KEY_METADATA_LEN;
pub(crate) const HEADER_PER_KEY_METADATA_LEN: usize = 2 * size_of::<Offset>();

/// ```text
/// Layout:
///     header: Header,
///     # We compute the length by subtracting two consecutive offsets,
///     # this requires a trailing fake offset to compute the length of the last actual element.
///     keys: [Offset; header.entry_count + 1],
///     values: [Offset; header.entry_count + 1],
///     data: [u8],
/// Header:
///     entry_count: u32,
/// Offset:
///     u32
/// ```
#[derive(Debug)]
pub struct PackedMap {
    entry_count: u32,
    data: CowBytes,
}

#[derive(Debug, Copy, Clone)]
#[repr(packed)]
struct Header {
    entry_count: u32,
}

#[derive(Debug, Copy, Clone)]
#[repr(packed)]
struct Offset(u32);

fn ref_u32_from_bytes_mut(p: &mut [u8]) -> &mut [u32] {
    assert!(p.len() % size_of::<u32>() == 0);
    assert!(p.as_ptr() as usize % align_of::<[u32; 1]>() == 0);
    unsafe { from_raw_parts_mut(p.as_mut_ptr() as *mut u32, p.len() / 4) }
}

fn prefix_size(entry_count: u32) -> usize {
    size_of::<Header>() + HEADER_PER_KEY_METADATA_LEN * (entry_count + 1) as usize
}

impl PackedMap {
    pub fn new(mut data: Vec<u8>) -> Self {
        assert!(data.len() >= 4);
        let entry_count = LittleEndian::read_u32(&data[..4]);
        let total_bytes = prefix_size(entry_count);
        {
            let s = ref_u32_from_bytes_mut(&mut data[..total_bytes]);
            LittleEndian::from_slice_u32(s);
        }
        PackedMap {
            data: data.into(),
            entry_count,
        }
    }

    /// Returns (position, len) for the metadata entry at position `m_idx`.
    /// If `m_idx < entry_count`, it will return the metadata of a value,
    /// if `m_idx == entry_count`, undefined nonsense will be returned,
    /// if `m_idx > entry_count`, the metadata of the value at position `m_idx - entry_count - 1` will
    /// be returned.
    fn position(&self, m_idx: u32) -> (Offset, u32) {
        let m_idx = m_idx as usize;
        let positions = unsafe {
            let ptr = self.data.as_ptr();
            let start = ptr.add(size_of::<Header>()) as *const Offset;
            from_raw_parts(start, (self.entry_count as usize + 1) * 2)
        };

        (
            positions[m_idx],
            positions[m_idx + 1].0 - positions[m_idx].0,
        )
    }

    fn key_pos(&self, idx: u32) -> (Offset, u32) {
        self.position(idx)
    }
    fn val_pos(&self, idx: u32) -> (Offset, u32) {
        self.position(self.entry_count + 1 + idx)
    }

    fn get_slice(&self, (Offset(pos), len): (Offset, u32)) -> &[u8] {
        &self.data[pos as usize..pos as usize + len as usize]
    }

    fn get_slice_cow(&self, (Offset(pos), len): (Offset, u32)) -> SlicedCowBytes {
        self.data.clone().slice(pos, len)
    }

    // Adapted from std::slice::binary_search_by
    fn binary_search(&self, key: &[u8]) -> Result<u32, u32> {
        use cmp::Ordering::*;
        let mut size = self.entry_count;
        if size == 0 {
            return Err(0);
        }
        let mut base = 0;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            let cmp = self.get_slice(self.key_pos(mid)).cmp(key);
            base = if cmp == Greater { base } else { mid };
            size -= half;
        }
        let cmp = self.get_slice(self.key_pos(base)).cmp(key);
        if cmp == Equal {
            Ok(base)
        } else {
            Err(base + (cmp == Less) as u32)
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<SlicedCowBytes> {
        let result = self.binary_search(key);
        let idx = match result {
            Err(_) => return None,
            Ok(idx) => idx,
        };

        Some(self.get_slice_cow(self.val_pos(idx)))
    }

    pub fn get_all<'a>(&'a self) -> impl Iterator<Item = (&'a [u8], SlicedCowBytes)> + 'a {
        struct Iter<'a> {
            packed: &'a PackedMap,
            idx: u32,
        }
        impl<'a> Iterator for Iter<'a> {
            type Item = (&'a [u8], SlicedCowBytes);

            fn next(&mut self) -> Option<Self::Item> {
                if self.idx < self.packed.entry_count {
                    let ret = Some((
                        self.packed.get_slice(self.packed.key_pos(self.idx)),
                        self.packed.get_slice_cow(self.packed.val_pos(self.idx)),
                    ));
                    self.idx += 1;
                    ret
                } else {
                    None
                }
            }
        }

        Iter {
            packed: self,
            idx: 0,
        }
    }

    pub(super) fn unpack_leaf(&self) -> LeafNode {
        self.get_all().collect()
    }

    pub(super) fn pack<W: Write>(leaf: &LeafNode, mut writer: W) -> io::Result<()> {
        let entries = leaf.entries();
        let entries_cnt = entries.len() as u32;
        writer.write_u32::<LittleEndian>(entries_cnt)?;
        let mut pos = prefix_size(entries_cnt) as u32;
        for key in entries.keys() {
            writer.write_u32::<LittleEndian>(pos)?;
            let len = key.len() as u32;
            pos += len;
        }
        writer.write_u32::<LittleEndian>(pos)?;

        for value in entries.values() {
            writer.write_u32::<LittleEndian>(pos)?;
            let len = value.len() as u32;
            pos += len;
        }
        writer.write_u32::<LittleEndian>(pos)?;

        for key in entries.keys() {
            writer.write_all(key)?;
        }
        for value in entries.values() {
            writer.write_all(value)?;
        }
        Ok(())
    }

    pub(super) fn inner(&self) -> &CowBytes {
        &self.data
    }
}

impl Size for PackedMap {
    fn size(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{LeafNode, PackedMap};

    #[quickcheck]
    fn check_packed_contents(leaf: LeafNode) {
        let mut v = Vec::new();
        PackedMap::pack(&leaf, &mut v).unwrap();

        let packed = PackedMap::new(v);

        for (k, v) in leaf.entries() {
            assert_eq!(Some(v), packed.get(k).as_ref());
        }

        assert_eq!(
            leaf.entries()
                .iter()
                .map(|(k, v)| (&k[..], v.clone()))
                .collect::<Vec<_>>(),
            packed.get_all().collect::<Vec<_>>()
        );
    }
}
