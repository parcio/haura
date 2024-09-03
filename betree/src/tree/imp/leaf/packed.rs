//! On-disk representation of a node.
//!
//! Can be used for read-only access to avoid deserialization.
use super::leaf::LeafNode;
use crate::{
    buffer::Buf,
    cow_bytes::SlicedCowBytes,
    data_management::{HasStoragePreference, IntegrityMode},
    size::Size,
    tree::KeyInfo,
    StoragePreference,
};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use std::{
    cmp,
    io::{self, Write},
    mem::size_of,
};

// account for trailing fake element
pub(crate) const HEADER_FIXED_LEN: usize = HEADER_LEN + OFFSET_LEN;

const HEADER_LEN: usize = size_of::<u32>() + size_of::<u8>();

// Offsets are stored as 24-bit unsigned integers in little-endian order
pub(crate) const OFFSET_LEN: usize = 3;
// 2 offsets (u24) and a keyinfo (u8)
pub(crate) const ENTRY_LEN: usize = 2 * OFFSET_LEN + 1;
pub(crate) const ENTRY_KEY_OFFSET: usize = 0;
pub(crate) const ENTRY_KEY_INFO_OFFSET: usize = ENTRY_KEY_OFFSET + OFFSET_LEN;
pub(crate) const ENTRY_DATA_OFFSET: usize = ENTRY_KEY_INFO_OFFSET + 1;

/// On-disk serialized leaf node. Simplified to a map contains 40 bytes of
/// headers followed by data.
///
/// ```text
/// Layout:
///     entry_count: u32,
///     system_pref: u8,
///     entries: [Entry; entry_count],
///     # necessary to compute the length of the last value
///     data_end: Offset,
///     data: [u8]
///
/// # These positions only mark the beginning of the key and value section in data,
/// # the length is computed by subtracting two consecutive offsets.
/// # This is possible because entries are written in the same order as their data
/// # will have in `data`.
/// Entry:
///     key_pos: Offset,
///     key_info: KeyInfo,
///     data_pos: Offset
///
/// # 2^24 byte ~= 16.7MB, plenty at a max node size of 4MiB
/// Offset:
///     u24
///
/// KeyInfo:
///     storage_preference: u8
///
/// ```
#[derive(Debug)]
pub(crate) struct PackedMap {
    entry_count: u32,
    system_preference: u8,
    data: SlicedCowBytes,
}

/// New type for safe-handling of data offsets u32s.
#[derive(Debug, Copy, Clone)]
struct Offset(u32);

fn prefix_size(entry_count: u32) -> usize {
    HEADER_FIXED_LEN + ENTRY_LEN * entry_count as usize
}

impl PackedMap {
    pub fn new(data: Buf) -> Self {
        // Skip the 4 bytes node identifier prefix
        let data = data
            .into_sliced_cow_bytes()
            .slice_from(crate::tree::imp::node::NODE_PREFIX_LEN as u32);
        debug_assert!(data.len() >= 4);
        let entry_count = LittleEndian::read_u32(&data[..4]);
        let system_preference = data[4];

        PackedMap {
            data,
            entry_count,
            system_preference,
        }
    }

    fn read_offset(&self, byte_idx: usize) -> Offset {
        Offset(LittleEndian::read_u24(
            &self.data[byte_idx..byte_idx + OFFSET_LEN],
        ))
    }

    // In the data segment, the value is always written directly after the key,
    // so the key length can be calculated by subtraction.
    fn key_pos(&self, idx: u32) -> (Offset, u32) {
        debug_assert!(idx < self.entry_count);

        let entry_pos = HEADER_LEN + idx as usize * ENTRY_LEN;

        let key_offset = self.read_offset(entry_pos + ENTRY_KEY_OFFSET);
        let data_offset = self.read_offset(entry_pos + ENTRY_DATA_OFFSET);
        let key_len = data_offset.0 - key_offset.0;

        (key_offset, key_len)
    }

    // In the data segment, the next key is usually written after the current value,
    // so the value length can usually be calculated by subtraction.
    fn val_pos(&self, idx: u32) -> (Offset, u32) {
        debug_assert!(idx < self.entry_count);

        let entry_pos = HEADER_LEN + idx as usize * ENTRY_LEN;
        let data_offset = self.read_offset(entry_pos + ENTRY_DATA_OFFSET);

        // this works even for the last entry, as a single offset is appended to the last full
        // entry, and the key offset comes first, so the rest of that fake entry is not missed.
        let next_entry_pos = entry_pos + ENTRY_LEN;
        let next_key_offset = self.read_offset(next_entry_pos + ENTRY_KEY_OFFSET);

        let data_len = next_key_offset.0 - data_offset.0;
        (data_offset, data_len)
    }

    fn key_info(&self, idx: u32) -> KeyInfo {
        debug_assert!(idx < self.entry_count);
        let entry_pos = HEADER_LEN + idx as usize * ENTRY_LEN;

        KeyInfo {
            storage_preference: StoragePreference::from_u8(
                self.data[entry_pos + ENTRY_KEY_INFO_OFFSET],
            ),
        }
    }

    fn get_slice(&self, (Offset(pos), len): (Offset, u32)) -> &[u8] {
        &self.data[pos as usize..pos as usize + len as usize]
    }

    fn get_slice_cow(&self, (Offset(pos), len): (Offset, u32)) -> SlicedCowBytes {
        self.data.clone().subslice(pos, len)
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

    pub fn get_by_index(&self, idx: u32) -> Option<(KeyInfo, SlicedCowBytes)> {
        Some((self.key_info(idx), self.get_slice_cow(self.val_pos(idx))))
    }

    pub fn get_full_by_index(
        &self,
        idx: u32,
    ) -> Option<(SlicedCowBytes, (KeyInfo, SlicedCowBytes))> {
        Some((
            self.get_slice_cow(self.key_pos(idx)),
            (self.key_info(idx), self.get_slice_cow(self.val_pos(idx))),
        ))
    }

    pub fn get(&self, key: &[u8]) -> Option<(KeyInfo, SlicedCowBytes)> {
        let result = self.binary_search(key);
        let idx = match result {
            Err(_) => return None,
            Ok(idx) => idx,
        };

        self.get_by_index(idx)
    }

    pub fn get_all(&self) -> impl Iterator<Item = (&[u8], (KeyInfo, SlicedCowBytes))> + '_ {
        struct Iter<'a> {
            packed: &'a PackedMap,
            idx: u32,
        }
        impl<'a> Iterator for Iter<'a> {
            type Item = (&'a [u8], (KeyInfo, SlicedCowBytes));

            fn next(&mut self) -> Option<Self::Item> {
                if self.idx < self.packed.entry_count {
                    let ret = Some((
                        self.packed.get_slice(self.packed.key_pos(self.idx)),
                        (
                            self.packed.key_info(self.idx),
                            self.packed.get_slice_cow(self.packed.val_pos(self.idx)),
                        ),
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

    pub(crate) fn unpack_leaf(&self) -> LeafNode {
        let mut leaf: LeafNode = self.get_all().collect();
        // Restore system storage preference state
        leaf.set_system_storage_preference(StoragePreference::from_u8(self.system_preference));
        leaf
    }

    pub(crate) fn pack<W: Write>(leaf: &LeafNode, mut writer: W) -> io::Result<IntegrityMode> {
        let entries = leaf.entries();
        let entries_cnt = entries.len() as u32;
        writer.write_u32::<LittleEndian>(entries_cnt)?;
        writer.write_u8(leaf.system_storage_preference().as_u8())?;

        let mut pos = prefix_size(entries_cnt) as u32;
        for (key, (keyinfo, value)) in entries {
            writer.write_u24::<LittleEndian>(pos)?;
            pos += key.len() as u32;

            writer.write_u8(keyinfo.storage_preference.as_u8())?;

            writer.write_u24::<LittleEndian>(pos)?;
            pos += value.len() as u32;
        }

        writer.write_u24::<LittleEndian>(pos)?;

        for (key, (_keyinfo, value)) in entries {
            writer.write_all(key)?;
            writer.write_all(value)?;
        }
        Ok(IntegrityMode::External)
    }

    pub(crate) fn inner(&self) -> &SlicedCowBytes {
        &self.data
    }

    pub(crate) fn entry_count(&self) -> u32 {
        self.entry_count
    }
}

impl Size for PackedMap {
    fn size(&self) -> usize {
        self.data.len()
    }

    fn actual_size(&self) -> Option<usize> {
        Some(self.size())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::{LeafNode, PackedMap};

    #[quickcheck]
    fn check_packed_contents(leaf: LeafNode) {
        let mut v = Vec::new();
        assert!(
            v.write(&[0; crate::tree::imp::node::NODE_PREFIX_LEN])
                .unwrap()
                == 4
        );
        PackedMap::pack(&leaf, &mut v).unwrap();

        let packed = PackedMap::new(v.into_boxed_slice());

        for (k, (ki, v)) in leaf.entries() {
            let (pki, pv) = packed.get(k).unwrap();
            assert_eq!(ki, &pki, "keyinfo mismatch");
            assert_eq!(v, &pv, "value mismatch");
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
