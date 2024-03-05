//! Implementation of a message buffering node wrapper.
//!
//! Encapsulating common nodes like [super::internal::NVMInternalNode] and
//! [super::leaf::NVMNVMLeafNode].
use crate::{
    compression::CompressionBuilder,
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::{impls::ObjRef, HasStoragePreference, ObjectPointer, ObjectReference},
    size::{Size, StaticSize},
    storage_pool::AtomicSystemStoragePreference,
    tree::{pivot_key::LocalPivotKey, KeyInfo, MessageAction, PivotKey},
    AtomicStoragePreference, StoragePreference,
};
use parking_lot::RwLock;
//use serde::{Deserialize, Serialize};
use rkyv::{
    archived_root,
    ser::{serializers::AllocSerializer, ScratchSpace, Serializer},
    vec::{ArchivedVec, VecResolver},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
    AlignedVec, Archive, Archived, Deserialize, Fallible, Infallible, Serialize,
};
use std::{
    borrow::Borrow,
    collections::{btree_map::Entry, BTreeMap, Bound},
    mem::replace,
};

pub struct EncodeNodePointer;
pub struct NodePointerResolver {
    len: usize,
    inner: VecResolver,
}

/// A buffer for messages that belong to a child of a tree node.
#[derive(serde::Serialize, serde::Deserialize, Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
//#[serde(bound(serialize = "N: Serialize", deserialize = "N: Deserialize<'de>"))]
pub(super) struct NVMChildBuffer {
    pub(super) messages_preference: AtomicStoragePreference,
    //#[serde(skip)]
    pub(super) system_storage_preference: AtomicSystemStoragePreference,
    //
    // FIXME: Ensure that this child node is serialized to the correct
    // preference and not for example on HDD which would make the access
    // horrifyingly slow.
    //
    // parent_preference: AtomicStoragePreference,
    entries_size: usize,
    #[with(rkyv::with::AsVec)]
    pub(super) buffer: BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>,
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

        for (keyinfo, _v) in self.buffer.values() {
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
        // FIXME: This is a magic bincode offset for vector length and storage prefs sizes
        18 + self
            .buffer
            .iter()
            .map(|(key, msg)| key.size() + msg.size())
            .sum::<usize>()
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
        !self.buffer.contains_key(key)
    }

    pub fn get(&self, key: &[u8]) -> Option<&(KeyInfo, SlicedCowBytes)> {
        self.buffer.get(key)
    }

    pub fn apply_with_info(&mut self, key: &[u8], pref: StoragePreference) -> Option<()> {
        self.buffer.get_mut(key).map(|(keyinfo, _bytes)| {
            keyinfo.storage_preference = pref;
        })
    }
}

impl NVMChildBuffer {
    /// Returns an iterator over all messages.
    pub fn get_all_messages(
        &self,
    ) -> impl Iterator<Item = (&CowBytes, &(KeyInfo, SlicedCowBytes))> + '_ {
        self.buffer.iter().map(|(key, msg)| (key, msg))
    }

    /// Takes the message buffer out this `NVMChildBuffer`,
    /// leaving an empty one in its place.
    pub fn take(&mut self) -> (BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>, usize) {
        self.messages_preference.invalidate();
        (
            std::mem::take(&mut self.buffer),
            replace(&mut self.entries_size, 0),
        )
    }

    pub fn append(&mut self, other: &mut Self) {
        self.buffer.append(&mut other.buffer);
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
            buffer,
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
        let right_buffer = self.buffer.split_off(&next_key[..]);
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
        right_sibling.buffer = buffer;
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

        match self.buffer.entry(key.clone()) {
            Entry::Vacant(e) => {
                let size_delta = key_size + msg.size() + keyinfo.size();
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
            buffer: BTreeMap::new(),
            entries_size: 0,
            system_storage_preference: AtomicSystemStoragePreference::from(StoragePreference::NONE),
        }
    }

    pub fn pack<W>(&self, w: W) -> Result<(), std::io::Error>
    where
        W: std::io::Write,
    {
        bincode::serialize_into(w, self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    pub fn unpack(buf: &[u8]) -> Result<Self, std::io::Error> {
        bincode::deserialize(buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
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
        for (key, msg) in self.buffer.range_mut::<[u8], _>(range) {
            size_delta += key.size() + msg.size();
            keys.push(key.clone());
        }
        for key in keys {
            self.buffer.remove(&key);
        }
        self.entries_size -= size_delta;
        self.messages_preference.invalidate();
        size_delta
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        arbitrary::GenExt,
        tree::{default_message_action::DefaultMessageActionMsg, imp::child_buffer},
    };
    use quickcheck::{Arbitrary, Gen, TestResult};
    use rand::Rng;

    impl Clone for NVMChildBuffer {
        fn clone(&self) -> Self {
            NVMChildBuffer {
                messages_preference: self.messages_preference.clone(),
                entries_size: self.entries_size,
                buffer: self.buffer.clone(),
                system_storage_preference: self.system_storage_preference.clone(),
            }
        }
    }

    impl PartialEq for NVMChildBuffer {
        fn eq(&self, other: &Self) -> bool {
            self.entries_size == other.entries_size && self.buffer == other.buffer
        }
    }

    impl Arbitrary for NVMChildBuffer {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut rng = g.rng();
            let entries_cnt = rng.gen_range(0..20);
            let buffer: BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)> = (0..entries_cnt)
                .map(|_| {
                    (
                        super::super::nvminternal::TestKey::arbitrary(g).0,
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
                buffer,
                system_storage_preference: AtomicSystemStoragePreference::from(
                    StoragePreference::NONE,
                ),
            }
        }
    }

    fn check_size(child_buffer: &NVMChildBuffer) {
        let mut buf = Vec::new();
        child_buffer.pack(&mut buf).unwrap();
        assert_eq!(buf.len(), child_buffer.size())
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
            .last_key_value()
            .map(|(k, _)| *k <= pivot_key)
            .unwrap_or(true));
        assert!(sbl
            .buffer
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
        let pivot = child_buffer.buffer.iter().nth(3).unwrap().0.clone();

        let mut other = child_buffer.split_at(&pivot);
        child_buffer.append(&mut other);

        assert_eq!(before_size, child_buffer.size());

        TestResult::passed()
    }

    #[quickcheck]
    fn serialize_then_deserialize(child_buffer: NVMChildBuffer) {
        let mut buf = Vec::new();
        child_buffer.pack(&mut buf).unwrap();

        let other = NVMChildBuffer::unpack(&buf).unwrap();
        assert_eq!(other, child_buffer)
    }
}
