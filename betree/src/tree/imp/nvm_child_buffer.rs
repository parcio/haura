//! Implementation of a message buffering node wrapper.
//! Encapsulating common nodes like [super::internal::NVMInternalNode] and
//! [super::leaf::NVMNVMLeafNode].
use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::HasStoragePreference,
    size::Size,
    storage_pool::AtomicSystemStoragePreference,
    tree::{KeyInfo, MessageAction},
    AtomicStoragePreference, StoragePreference,
};
use std::{
    borrow::Borrow,
    cmp::Ordering,
    collections::{
        btree_map::{self, Entry},
        BTreeMap, Bound,
    },
    mem::replace,
    ptr::slice_from_raw_parts,
};

use super::child_buffer::ChildBuffer;

trait CutSlice<T> {
    fn cut(&self, pos: usize, len: usize) -> &[T];
}

impl<T> CutSlice<T> for [T] {
    fn cut(&self, pos: usize, len: usize) -> &[T] {
        &self[pos..pos + len]
    }
}

/// A buffer for messages that belong to a child of a tree node.
#[derive(Debug)]
pub(super) struct NVMChildBuffer {
    pub(super) messages_preference: AtomicStoragePreference,
    // This preference should always be set by the parent. Needs to be on fast
    // memory or NVMe to be worth the additional queries.
    pub(super) system_storage_preference: AtomicSystemStoragePreference,
    pub(super) entries_size: usize,
    pub(super) buffer: Map,
}

pub const BUFFER_STATIC_SIZE: usize = HEADER;
const NODE_ID: usize = 4;
const HEADER: usize =
    NODE_ID + std::mem::size_of::<u32>() + std::mem::size_of::<u32>() + std::mem::size_of::<u8>();
const KEY_IDX_SIZE: usize =
    std::mem::size_of::<u32>() + std::mem::size_of::<u8>() + std::mem::size_of::<u32>();

#[derive(Debug)]
pub(super) enum Map {
    Packed { entry_count: usize, data: SlicedCowBytes },
    Unpacked(BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>),
}

#[repr(C)]
pub struct KeyIdx {
    pos: u32,
    len: u32,
    pref: u8,
}

impl KeyIdx {
    pub fn unpack(buf: &[u8; 9]) -> KeyIdx {
        KeyIdx {
            pos: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            len: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            pref: u8::from_le_bytes(buf[8..9].try_into().unwrap()),
        }
    }
}

impl Map {
    /// Fetch a mutable version of the internal btree map.
    pub(super) fn unpacked(&mut self) -> &mut BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)> {
        match self {
            Map::Packed { entry_count, data } => {
                let mut keys: Vec<CowBytes> = Vec::with_capacity(*entry_count);
                let mut key_info = Vec::with_capacity(*entry_count);
                let mut values_pos: Vec<(u32, u32)> = Vec::with_capacity(*entry_count);

                for idx in 0..*entry_count {
                    let off = HEADER + idx * KEY_IDX_SIZE;
                    let kidx = KeyIdx::unpack(data.cut(off, 9).try_into().unwrap());
                    key_info.push(KeyInfo {
                        storage_preference: StoragePreference::from_u8(kidx.pref),
                    });
                    keys.push(CowBytes::from(
                        data.cut(kidx.pos as usize, kidx.len as usize),
                    ));

                    let val_pos_off = kidx.pos as usize + kidx.len as usize;
                    let val_pos = u32::from_le_bytes(data.cut(val_pos_off, 4).try_into().unwrap());
                    let val_len =
                        u32::from_le_bytes(data.cut(val_pos_off + 4, 4).try_into().unwrap());
                    values_pos.push((val_pos, val_len));
                }

                *self = Map::Unpacked(BTreeMap::from_iter(
                    keys.into_iter().zip(
                        key_info.into_iter().zip(
                            values_pos
                                .into_iter()
                                // NOTE: This copy is cheap as the data is behind an Arc.
                                .map(|(pos, len)| data.clone().subslice(pos, len)),
                        ),
                    ),
                ));

                match self {
                    Map::Unpacked(ref mut map) => map,
                    _ => unreachable!(),
                }
            }
            Map::Unpacked(ref mut map) => map,
        }
    }

    /// Assert an unpacked instance.
    fn assert_unpacked(&self) -> &BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)> {
        match self {
            Map::Packed { .. } => {
                panic!("Tried to assert a packed ChildBuffer instance.")
            }
            Map::Unpacked(ref map) => map,
        }
    }

    /// True if a proper btree map has been created for this instance.
    fn is_unpacked(&self) -> bool {
        match self {
            Map::Packed { .. } => false,
            Map::Unpacked(_) => true,
        }
    }

    /// Returns whether there is no message in this buffer for the given `key`.
    pub fn is_empty(&self, key: &[u8]) -> bool {
        match self {
            Map::Packed { .. } => self.find(key).is_none(),
            Map::Unpacked(btree) => !btree.contains_key(key),
        }
    }

    /// Return the number of elements.
    pub fn len(&self) -> usize {
        match self {
            Map::Packed { entry_count, .. } => *entry_count,
            Map::Unpacked(btree) => btree.len(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<(KeyInfo, SlicedCowBytes)> {
        match self {
            Map::Packed { data, .. } => self.find(key).map(|(pref, pos, len)| {
                (
                    KeyInfo {
                        storage_preference: StoragePreference::from_u8(pref),
                    },
                    unsafe { SlicedCowBytes::from_raw(data.as_ptr().add(pos), len) },
                )
            }),
            // TODO: This should be a cheap copy (a few bytes for the pref and
            // the ptrs in slicedcowbytes) but please check this again.
            Map::Unpacked(btree) => btree.get(key).cloned(),
        }
    }

    // Return the preference and location of the value within the boxed value.
    fn find(&self, key: &[u8]) -> Option<(u8, usize, usize)> {
        match self {
            Map::Packed { entry_count, data } => {
                // Perform binary search
                let mut left = 0 as isize;
                let mut right = (*entry_count as isize) - 1;
                loop {
                    if left > right {
                        break;
                    }
                    let mid = (left + right) / 2 + (left + right) % 2;
                    let kidx = KeyIdx::unpack(
                        data.cut(HEADER + (KEY_IDX_SIZE * mid as usize), KEY_IDX_SIZE)
                            .try_into()
                            .unwrap(),
                    );

                    let k = slice_from_raw_parts(
                        unsafe { data.as_ptr().add(kidx.pos as usize) },
                        kidx.len as usize,
                    );

                    match key.cmp(unsafe { &*k }) {
                        Ordering::Less => {
                            right = mid as isize - 1;
                        }
                        Ordering::Equal => {
                            let val_pos_off = kidx.pos as usize + kidx.len as usize;
                            let val_pos =
                                u32::from_le_bytes(data.cut(val_pos_off, 4).try_into().unwrap())
                                    as usize;
                            let val_len = u32::from_le_bytes(
                                data.cut(val_pos_off + 4, 4).try_into().unwrap(),
                            ) as usize;
                            return Some((kidx.pref, val_pos, val_len));
                        }
                        Ordering::Greater => {
                            left = mid + 1;
                        }
                    }
                }
                None
            }
            Map::Unpacked(_) => unreachable!(),
        }
    }
}

impl HasStoragePreference for NVMChildBuffer {
    fn current_preference(&self) -> Option<StoragePreference> {
        self.messages_preference
            .as_option()
            // .map(|msg_pref| {
            //     StoragePreference::choose_faster(
            //         msg_pref,
            //         self.node_pointer.read().correct_preference(),
            //     )
            // })
            .map(|p| self.system_storage_preference.weak_bound(&p))
    }

    fn recalculate(&self) -> StoragePreference {
        let mut pref = StoragePreference::NONE;

        for (keyinfo, _v) in self.buffer.assert_unpacked().values() {
            pref.upgrade(keyinfo.storage_preference)
        }

        self.messages_preference.set(pref);

        // pref can't be lower than that of child nodes
        StoragePreference::choose_faster(
            pref,
            StoragePreference::NONE,
            // self.parent_preference
            //     .as_option()
            //     .unwrap_or(StoragePreference::NONE),
        )
    }

    fn system_storage_preference(&self) -> StoragePreference {
        self.system_storage_preference.borrow().into()
    }

    fn set_system_storage_preference(&mut self, pref: StoragePreference) {
        self.system_storage_preference.set(pref)
    }
}

impl Size for NVMChildBuffer {
    fn size(&self) -> usize {
        HEADER + self.entries_size
    }

    fn actual_size(&self) -> Option<usize> {
        Some(self.size())
    }
}

impl NVMChildBuffer {
    pub fn buffer_size(&self) -> usize {
        self.entries_size
    }

    /// Returns whether there is no message in this buffer for the given `key`.
    pub fn is_empty(&self, key: &[u8]) -> bool {
        self.buffer.is_empty(key)
    }

    pub fn get(&self, key: &[u8]) -> Option<(KeyInfo, SlicedCowBytes)> {
        self.buffer.get(key)
    }

    pub fn apply_with_info(&mut self, key: &[u8], pref: StoragePreference) -> Option<()> {
        self.buffer
            .unpacked()
            .get_mut(key)
            .map(|(keyinfo, _bytes)| {
                keyinfo.storage_preference = pref;
            })
    }
}

pub struct PackedBufferIterator<'a> {
    buffer: &'a SlicedCowBytes,
    cur: usize,
    entry_count: usize,
    keys: Vec<KeyIdx>,
}

impl<'a> Iterator for PackedBufferIterator<'a> {
    type Item = (CowBytes, (KeyInfo, SlicedCowBytes));

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur >= self.entry_count {
            return None;
        }

        let kpos = &self.keys[self.cur];
        let key = self.buffer.clone().subslice(kpos.pos, kpos.len);

        let vpos_off = (kpos.pos + kpos.len) as usize;
        let vpos = u32::from_le_bytes(self.buffer.cut(vpos_off, 4).try_into().unwrap());
        let vlen = u32::from_le_bytes(self.buffer.cut(vpos_off + 4, 4).try_into().unwrap());
        let val = self.buffer.clone().subslice(vpos, vlen);
        self.cur += 1;
        Some((
            // FIXME: Expensive copy when returning results here.
            CowBytes::from(&key[..]),
            (
                KeyInfo {
                    storage_preference: StoragePreference::from_u8(kpos.pref),
                },
                val,
            ),
        ))
    }
}

pub enum Iter<'a> {
    Packed(PackedBufferIterator<'a>),
    Unpacked(btree_map::Iter<'a, CowBytes, (KeyInfo, SlicedCowBytes)>),
}

impl<'a> Iter<'a> {
    fn new(cbuf: &'a NVMChildBuffer) -> Self {
        match cbuf.buffer {
            Map::Packed {
                entry_count,
                ref data,
            } => Iter::Packed(PackedBufferIterator {
                keys: (0..entry_count)
                    .map(|idx| {
                        KeyIdx::unpack(
                            data.cut(HEADER + KEY_IDX_SIZE * idx, KEY_IDX_SIZE)
                                .try_into()
                                .unwrap(),
                        )
                    })
                    .collect(),
                buffer: data,
                cur: 0,
                entry_count,
            }),
            Map::Unpacked(ref btree) => Iter::Unpacked(btree.iter()),
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = (CowBytes, (KeyInfo, SlicedCowBytes));

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Packed(i) => i.next(),
            // FIXME: Is this a good way to do this now? We exploit interior
            // somewhat cheap copies to unify the return type, but it's not so
            // nice.
            Iter::Unpacked(i) => i.next().map(|(a, b)| (a.clone(), b.clone())),
        }
    }
}

impl NVMChildBuffer {
    /// Returns an iterator over all messages.
    pub fn get_all_messages(
        &self,
    ) -> impl Iterator<Item = (CowBytes, (KeyInfo, SlicedCowBytes))> + '_ {
        Iter::new(self)
    }

    /// Takes the message buffer out this `NVMChildBuffer`,
    /// leaving an empty one in its place.
    pub fn take(&mut self) -> (BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>, usize) {
        self.messages_preference.invalidate();
        (
            std::mem::take(&mut self.buffer.unpacked()),
            replace(&mut self.entries_size, 0),
        )
    }

    pub fn append(&mut self, other: &mut Self) {
        self.buffer.unpacked().append(&mut other.buffer.unpacked());
        self.entries_size += other.entries_size;
        self.messages_preference
            .upgrade_atomic(&other.messages_preference);
    }

    /// Splits this `NVMChildBuffer` at `pivot` so that `self` contains all
    /// entries up to (and including) `pivot_key` and the returned `Self`
    /// contains the other entries.
    pub fn split_at(&mut self, pivot: &CowBytes) -> Self {
        let (buffer, buffer_entries_size) = self.split_off(pivot);
        NVMChildBuffer {
            messages_preference: AtomicStoragePreference::unknown(),
            buffer: Map::Unpacked(buffer),
            entries_size: buffer_entries_size,
            system_storage_preference: AtomicSystemStoragePreference::from(StoragePreference::NONE),
        }
    }

    fn split_off(
        &mut self,
        pivot: &CowBytes,
    ) -> (BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>, usize) {
        // `split_off` puts the split-key into the right buffer.
        let mut next_key = pivot.to_vec();
        next_key.push(0);
        let right_buffer = self.buffer.unpacked().split_off(&next_key[..]);
        self.messages_preference.invalidate();

        let right_entry_size = right_buffer
            .iter()
            .map(|(key, value)| key.size() + value.size())
            .sum();
        self.entries_size -= right_entry_size;
        (right_buffer, right_entry_size)
    }

    pub fn rebalance(&mut self, right_sibling: &mut Self, new_pivot_key: &CowBytes) {
        self.append(right_sibling);
        let (buffer, buffer_entries_size) = self.split_off(new_pivot_key);
        right_sibling.buffer = Map::Unpacked(buffer);
        right_sibling.entries_size = buffer_entries_size;
    }

    /// Inserts a message to this buffer for the given `key`.
    pub fn insert<Q, M>(
        &mut self,
        key: Q,
        keyinfo: KeyInfo,
        msg: SlicedCowBytes,
        msg_action: M,
    ) -> isize
    where
        Q: Borrow<[u8]> + Into<CowBytes>,
        M: MessageAction,
    {
        let key = key.into();
        let key_size = key.size();

        self.messages_preference.upgrade(keyinfo.storage_preference);

        match self.buffer.unpacked().entry(key.clone()) {
            Entry::Vacant(e) => {
                let size_delta =
                    key_size + msg.size() + keyinfo.size() + 4 * std::mem::size_of::<u32>();
                e.insert((keyinfo, msg));
                self.entries_size += size_delta;
                size_delta as isize
            }
            Entry::Occupied(mut e) => {
                let lower = e.get_mut().clone();
                let (_, lower_msg) = lower;
                let lower_size = lower_msg.size();
                let merged_msg = msg_action.merge(&key, msg, lower_msg);
                let merged_msg_size = merged_msg.size();
                e.get_mut().1 = merged_msg;
                self.entries_size -= lower_size;
                self.entries_size += merged_msg_size;
                merged_msg_size as isize - lower_size as isize
            }
        }
    }

    /// Constructs a new, empty buffer.
    pub fn new() -> Self {
        NVMChildBuffer {
            messages_preference: AtomicStoragePreference::known(StoragePreference::NONE),
            buffer: Map::Unpacked(BTreeMap::new()),
            entries_size: 0,
            system_storage_preference: AtomicSystemStoragePreference::from(StoragePreference::NONE),
        }
    }

    /// This method packs entries similar to the packed leaf as they are quite
    /// similar in their behavior.
    ///
    ///
    ///
    /// Packed Stream is constructed as so (all numbers are in Little Endian):
    /// - u32: len entries
    /// - u32: entries_size
    /// - u8: storage pref
    /// - [
    ///     u32: pos key,
    ///     u32: len key,
    ///     u8: pref key,
    ///   ]
    /// - [
    ///     bytes: key,
    ///     u32: pos val,
    ///     u32: len val,
    ///   ]
    /// - [
    ///     bytes: val,
    ///   ]
    ///
    pub fn pack<W>(&self, mut w: W) -> Result<(), std::io::Error>
    where
        W: std::io::Write,
    {
        debug_assert!(self.buffer.is_unpacked());
        w.write_all(&(self.buffer.len() as u32).to_le_bytes())?;
        w.write_all(&(self.entries_size as u32).to_le_bytes())?;
        w.write_all(
            &self
                .system_storage_preference
                .strong_bound(&StoragePreference::NONE)
                .as_u8()
                .to_le_bytes(),
        )?;

        let mut free_after = HEADER + self.buffer.len() * KEY_IDX_SIZE;
        for (key, (info, _)) in self.buffer.assert_unpacked().iter() {
            let key_len = key.len();
            w.write_all(&(free_after as u32).to_le_bytes())?;
            w.write_all(&(key_len as u32).to_le_bytes())?;
            w.write_all(&info.storage_preference.as_u8().to_le_bytes())?;
            free_after += key_len + std::mem::size_of::<u32>() + std::mem::size_of::<u32>();
        }
        for (key, (_, val)) in self.buffer.assert_unpacked().iter() {
            w.write_all(&key)?;
            w.write_all(&(free_after as u32).to_le_bytes())?;
            w.write_all(&(val.len() as u32).to_le_bytes())?;
            free_after += val.len();
        }
        for (_, (_, val)) in self.buffer.assert_unpacked().iter() {
            w.write_all(&val)?;
        }

        Ok(())
    }

    pub fn unpack(buf: SlicedCowBytes) -> Result<Self, std::io::Error> {
        let entry_count =
            u32::from_le_bytes(buf[NODE_ID..NODE_ID + 4].try_into().unwrap()) as usize;
        let entries_size =
            u32::from_le_bytes(buf[NODE_ID + 4..NODE_ID + 4 + 4].try_into().unwrap()) as usize;
        let pref = u8::from_le_bytes(buf[NODE_ID + 8..NODE_ID + 9].try_into().unwrap());
        Ok(Self {
            messages_preference: AtomicStoragePreference::known(StoragePreference::from_u8(pref)),
            system_storage_preference: AtomicSystemStoragePreference::from(
                StoragePreference::from_u8(pref),
            ),
            entries_size,
            buffer: Map::Packed {
                entry_count,
                data: buf,
            },
        })
    }

    pub fn from_block_child_buffer<N>(other: ChildBuffer<N>) -> (Self, N) {
        todo!()
    }
}

impl NVMChildBuffer {
    pub fn range_delete(&mut self, start: &[u8], end: Option<&[u8]>) -> usize {
        // Context: Previously we mentioned the usage of a drain filter here and
        // linked to an existing issue of how it is missing from the standard
        // library.
        //
        // Adding a drain filter here would make things easier from the code
        // perspective, but with the generic predicate, we cannot utilize the
        // nice property of the BTreeMap that data is ordered and the traversal
        // of the tree can be nicely restrictred with a proper range. Due to
        // this I changed the T0D0 placed here to this very explanation you are
        // reading.
        let mut size_delta = 0;
        let range = (
            Bound::Included(start),
            end.map_or(Bound::Unbounded, Bound::Excluded),
        );
        let mut keys = Vec::new();
        for (key, msg) in self.buffer.unpacked().range_mut::<[u8], _>(range) {
            size_delta += key.size() + msg.size();
            keys.push(key.clone());
        }
        for key in keys {
            self.buffer.unpacked().remove(&key);
        }
        self.entries_size -= size_delta;
        self.messages_preference.invalidate();
        size_delta
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{arbitrary::GenExt, tree::default_message_action::DefaultMessageActionMsg};
    use quickcheck::{Arbitrary, Gen, TestResult};
    use rand::Rng;

    impl Clone for NVMChildBuffer {
        fn clone(&self) -> Self {
            NVMChildBuffer {
                messages_preference: self.messages_preference.clone(),
                entries_size: self.entries_size,
                buffer: Map::Unpacked(self.buffer.assert_unpacked().clone()),
                system_storage_preference: self.system_storage_preference.clone(),
            }
        }
    }

    impl PartialEq for NVMChildBuffer {
        fn eq(&self, other: &Self) -> bool {
            self.entries_size == other.entries_size
                && self.buffer.assert_unpacked() == other.buffer.assert_unpacked()
        }
    }

    impl Arbitrary for NVMChildBuffer {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut rng = g.rng();
            let entries_cnt = rng.gen_range(0..20);
            let buffer: BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)> = (0..entries_cnt)
                .map(|_| {
                    (
                        super::super::disjoint_internal::TestKey::arbitrary(g).0,
                        (
                            KeyInfo::arbitrary(g),
                            DefaultMessageActionMsg::arbitrary(g).0,
                        ),
                    )
                })
                .collect();
            NVMChildBuffer {
                messages_preference: AtomicStoragePreference::unknown(),
                entries_size: buffer
                    .iter()
                    .map(|(key, value)| key.size() + value.size())
                    .sum::<usize>(),
                buffer: Map::Unpacked(buffer),
                system_storage_preference: AtomicSystemStoragePreference::from(
                    StoragePreference::NONE,
                ),
            }
        }
    }

    fn check_size(child_buffer: &NVMChildBuffer) {
        let mut buf = Vec::new();
        child_buffer.pack(&mut buf).unwrap();
        assert_eq!(buf.len() + NODE_ID, child_buffer.size())
    }

    #[quickcheck]
    fn actual_size(child_buffer: NVMChildBuffer) {
        check_size(&child_buffer)
    }

    #[quickcheck]
    fn size_split_at(mut child_buffer: NVMChildBuffer, pivot_key: CowBytes) {
        let sbl = child_buffer.split_at(&pivot_key);
        check_size(&child_buffer);
        assert!(child_buffer.checked_size().is_ok());
        check_size(&sbl);
        assert!(sbl.checked_size().is_ok());
    }

    #[quickcheck]
    fn split_at(mut child_buffer: NVMChildBuffer, pivot_key: CowBytes) {
        let sbl = child_buffer.split_at(&pivot_key);
        assert!(child_buffer
            .buffer
            .assert_unpacked()
            .last_key_value()
            .map(|(k, _)| *k <= pivot_key)
            .unwrap_or(true));
        assert!(sbl
            .buffer
            .assert_unpacked()
            .first_key_value()
            .map(|(k, _)| *k > pivot_key)
            .unwrap_or(true));
    }

    #[quickcheck]
    fn append(mut child_buffer: NVMChildBuffer) -> TestResult {
        if child_buffer.buffer.len() < 4 {
            return TestResult::discard();
        }
        let before_size = child_buffer.size();
        let pivot = child_buffer
            .buffer
            .assert_unpacked()
            .iter()
            .nth(3)
            .unwrap()
            .0
            .clone();

        let mut other = child_buffer.split_at(&pivot);
        child_buffer.append(&mut other);

        assert_eq!(before_size, child_buffer.size());

        TestResult::passed()
    }

    #[quickcheck]
    fn unpack_equality(child_buffer: NVMChildBuffer) {
        let mut buf = Vec::new();
        buf.extend_from_slice(&[0u8; 4]);
        child_buffer.pack(&mut buf).unwrap();

        let mut other = NVMChildBuffer::unpack(buf.into_boxed_slice()).unwrap();
        other.buffer.unpacked();

        for (key, (info, val)) in child_buffer.buffer.assert_unpacked() {
            let res = other.get(key).unwrap();
            assert_eq!((&res.0, &res.1), (info, val));
        }
    }

    #[quickcheck]
    fn unpackless_access(child_buffer: NVMChildBuffer) {
        let mut buf = Vec::new();
        buf.extend_from_slice(&[0u8; 4]);
        child_buffer.pack(&mut buf).unwrap();

        let other = NVMChildBuffer::unpack(buf.into_boxed_slice()).unwrap();

        for (key, (info, val)) in child_buffer.buffer.assert_unpacked() {
            let res = other.get(key).unwrap();
            assert_eq!((&res.0, &res.1), (info, val));
        }
    }

    #[quickcheck]
    fn unpackless_iter(child_buffer: NVMChildBuffer) {
        let mut buf = Vec::new();
        buf.extend_from_slice(&[0u8; 4]);
        child_buffer.pack(&mut buf).unwrap();

        let other = NVMChildBuffer::unpack(buf.into_boxed_slice()).unwrap();

        for (idx, (key, tup)) in child_buffer.get_all_messages().enumerate() {
            let res = other.get_all_messages().nth(idx).unwrap();
            assert_eq!((key, tup), res);
        }
    }

    #[quickcheck]
    fn serialize_deserialize_idempotent(child_buffer: NVMChildBuffer) {
        let mut buf = Vec::new();
        buf.extend_from_slice(&[0u8; 4]);
        child_buffer.pack(&mut buf).unwrap();
        let mut other = NVMChildBuffer::unpack(buf.into_boxed_slice()).unwrap();
        other.buffer.unpacked();
        assert_eq!(other, child_buffer);
    }
}
