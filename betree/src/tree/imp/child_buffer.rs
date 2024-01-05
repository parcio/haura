//! Implementation of a message buffering node wrapper.
//!
//! Encapsulating common nodes like [super::internal::InternalNode] and
//! [super::leaf::LeafNode].
use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::{HasStoragePreference, ObjectReference},
    size::{Size, StaticSize},
    storage_pool::AtomicSystemStoragePreference,
    tree::{pivot_key::LocalPivotKey, KeyInfo, MessageAction, PivotKey},
    AtomicStoragePreference, StoragePreference,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    collections::{btree_map::Entry, BTreeMap, Bound},
    mem::replace,
};

/// A buffer for messages that belong to a child of a tree node.
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound(serialize = "N: Serialize", deserialize = "N: Deserialize<'de>"))]
pub(super) struct ChildBuffer<N: 'static> {
    pub(super) messages_preference: AtomicStoragePreference,
    #[serde(skip)]
    pub(super) system_storage_preference: AtomicSystemStoragePreference,
    buffer_entries_size: usize,
    pub(super) buffer: BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>,
    #[serde(with = "ser_np")]
    pub(super) node_pointer: RwLock<N>,
}

impl Size for (KeyInfo, SlicedCowBytes) {
    fn size(&self) -> usize {
        let (_keyinfo, data) = self;
        KeyInfo::static_size() + data.size()
    }
}

impl<N: HasStoragePreference> HasStoragePreference for ChildBuffer<N> {
    fn current_preference(&self) -> Option<StoragePreference> {
        self.messages_preference
            .as_option()
            .map(|msg_pref| {
                StoragePreference::choose_faster(
                    msg_pref,
                    self.node_pointer.read().correct_preference(),
                )
            })
            .map(|p| self.system_storage_preference.weak_bound(&p))
    }

    fn recalculate(&self) -> StoragePreference {
        let mut pref = StoragePreference::NONE;

        for (keyinfo, _v) in self.buffer.values() {
            pref.upgrade(keyinfo.storage_preference)
        }

        self.messages_preference.set(pref);

        // pref can't be lower than that of child nodes
        StoragePreference::choose_faster(pref, self.node_pointer.read().correct_preference())
    }

    fn system_storage_preference(&self) -> StoragePreference {
        self.system_storage_preference.borrow().into()
    }

    fn set_system_storage_preference(&mut self, pref: StoragePreference) {
        self.system_storage_preference.set(pref)
    }
}

impl<N: ObjectReference> ChildBuffer<N> {
    /// Access the pivot key of the underlying object reference and update it to
    /// reflect a structural change in the tree.
    pub fn update_pivot_key(&mut self, lpk: LocalPivotKey) {
        let or = self.node_pointer.get_mut();
        let d_id = or.index().d_id();
        or.set_index(lpk.to_global(d_id));
    }

    /// Insert an arbitrary PivotKey into the `ObjectReference`.
    ///
    /// FIXME: This is best replaced with actual type exclusion.
    pub fn complete_object_ref(&mut self, pk: PivotKey) {
        self.node_pointer.get_mut().set_index(pk)
    }
}

mod ser_np {
    //! Serialization utilities of a node pointer type.
    use super::RwLock;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<N, S>(np: &RwLock<N>, serializer: S) -> Result<S::Ok, S::Error>
    where
        N: Serialize,
        S: Serializer,
    {
        np.read().serialize(serializer)
    }

    pub fn deserialize<'de, N, D>(deserializer: D) -> Result<RwLock<N>, D::Error>
    where
        N: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        N::deserialize(deserializer).map(RwLock::new)
    }
}

impl<N: Size> Size for ChildBuffer<N> {
    fn size(&self) -> usize {
        Self::static_size() + self.buffer_entries_size + self.node_pointer.read().size()
    }

    fn actual_size(&self) -> Option<usize> {
        Some(
            Self::static_size()
                + self.node_pointer.read().size()
                + self
                    .buffer
                    .iter()
                    .map(|(key, msg)| key.size() + msg.size())
                    .sum::<usize>(),
        )
    }
}

impl<N> ChildBuffer<N> {
    pub fn static_size() -> usize {
        17
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_entries_size
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

impl<N> ChildBuffer<N> {
    /// Returns an iterator over all messages.
    pub fn get_all_messages(
        &self,
    ) -> impl Iterator<Item = (&CowBytes, &(KeyInfo, SlicedCowBytes))> + '_ {
        self.buffer.iter().map(|(key, msg)| (key, msg))
    }

    /// Takes the message buffer out this `ChildBuffer`,
    /// leaving an empty one in its place.
    pub fn take(&mut self) -> (BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>, usize) {
        self.messages_preference.invalidate();
        (
            std::mem::take(&mut self.buffer),
            replace(&mut self.buffer_entries_size, 0),
        )
    }

    pub fn append(&mut self, other: &mut Self) {
        self.buffer.append(&mut other.buffer);
        self.buffer_entries_size += other.buffer_entries_size;
        self.messages_preference
            .upgrade_atomic(&other.messages_preference);
    }

    /// Splits this `ChildBuffer` at `pivot`
    /// so that `self` contains all entries up to (and including) `pivot_key`
    /// and the returned `Self` contains the other entries and `node_pointer`.
    pub fn split_at(&mut self, pivot: &CowBytes, node_pointer: N) -> Self {
        let (buffer, buffer_entries_size) = self.split_off(pivot);
        ChildBuffer {
            messages_preference: AtomicStoragePreference::unknown(),
            buffer,
            buffer_entries_size,
            node_pointer: RwLock::new(node_pointer),
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
        self.buffer_entries_size -= right_entry_size;
        (right_buffer, right_entry_size)
    }

    pub fn rebalance(&mut self, right_sibling: &mut Self, new_pivot_key: &CowBytes) {
        self.append(right_sibling);
        let (buffer, buffer_entries_size) = self.split_off(new_pivot_key);
        right_sibling.buffer = buffer;
        right_sibling.buffer_entries_size = buffer_entries_size;
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
                self.buffer_entries_size += size_delta;
                size_delta as isize
            }
            Entry::Occupied(mut e) => {
                let lower = e.get_mut().clone();
                let (_, lower_msg) = lower;
                let lower_size = lower_msg.size();
                let merged_msg = msg_action.merge(&key, msg, lower_msg);
                let merged_msg_size = merged_msg.size();
                e.get_mut().1 = merged_msg;
                self.buffer_entries_size -= lower_size;
                self.buffer_entries_size += merged_msg_size;
                merged_msg_size as isize - lower_size as isize
            }
        }
    }

    /// Constructs a new, empty buffer.
    pub fn new(node_pointer: N) -> Self {
        ChildBuffer {
            messages_preference: AtomicStoragePreference::known(StoragePreference::NONE),
            buffer: BTreeMap::new(),
            buffer_entries_size: 0,
            node_pointer: RwLock::new(node_pointer),
            system_storage_preference: AtomicSystemStoragePreference::from(StoragePreference::NONE),
        }
    }
}

impl<N> ChildBuffer<N> {
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
        self.buffer_entries_size -= size_delta;
        self.messages_preference.invalidate();
        size_delta
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{arbitrary::GenExt, tree::default_message_action::DefaultMessageActionMsg};
    use bincode::serialized_size;
    use quickcheck::{Arbitrary, Gen};
    use rand::Rng;

    impl<N: Clone> Clone for ChildBuffer<N> {
        fn clone(&self) -> Self {
            ChildBuffer {
                messages_preference: self.messages_preference.clone(),
                buffer_entries_size: self.buffer_entries_size,
                buffer: self.buffer.clone(),
                node_pointer: RwLock::new(self.node_pointer.read().clone()),
                system_storage_preference: self.system_storage_preference.clone(),
            }
        }
    }

    impl<N: PartialEq> PartialEq for ChildBuffer<N> {
        fn eq(&self, other: &Self) -> bool {
            self.buffer_entries_size == other.buffer_entries_size
                && self.buffer == other.buffer
                && *self.node_pointer.read() == *other.node_pointer.read()
        }
    }

    impl<N: Arbitrary> Arbitrary for ChildBuffer<N> {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut rng = g.rng();
            let entries_cnt = rng.gen_range(0..20);
            let buffer: BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)> = (0..entries_cnt)
                .map(|_| {
                    (
                        CowBytes::arbitrary(g),
                        (
                            KeyInfo::arbitrary(g),
                            DefaultMessageActionMsg::arbitrary(g).0,
                        ),
                    )
                })
                .collect();
            ChildBuffer {
                messages_preference: AtomicStoragePreference::unknown(),
                buffer_entries_size: buffer
                    .iter()
                    .map(|(key, value)| key.size() + value.size())
                    .sum::<usize>(),
                buffer,
                node_pointer: RwLock::new(Arbitrary::arbitrary(g)),
                system_storage_preference: AtomicSystemStoragePreference::from(
                    StoragePreference::NONE,
                ),
            }
        }
    }

    #[quickcheck]
    fn check_serialize_size(child_buffer: ChildBuffer<()>) {
        assert_eq!(
            child_buffer.size(),
            serialized_size(&child_buffer).unwrap() as usize
        );

        assert_eq!(Some(child_buffer.size()), child_buffer.actual_size());
    }

    #[quickcheck]
    fn check_size_split_at(mut child_buffer: ChildBuffer<()>, pivot_key: CowBytes) {
        let size_before = child_buffer.size();
        let sibling = child_buffer.split_at(&pivot_key, ());
        assert_eq!(
            child_buffer.size(),
            serialized_size(&child_buffer).unwrap() as usize
        );
        assert_eq!(sibling.size(), serialized_size(&sibling).unwrap() as usize);
        assert_eq!(
            child_buffer.size() + sibling.buffer_entries_size,
            size_before
        );
    }

    #[quickcheck]
    fn check_split_at(mut child_buffer: ChildBuffer<()>, pivot_key: CowBytes) {
        let this = child_buffer.clone();
        let mut sibling = child_buffer.split_at(&pivot_key, ());
        assert!(child_buffer
            .buffer
            .iter()
            .next_back()
            .map_or(true, |(key, _value)| key.clone() <= pivot_key));
        assert!(sibling
            .buffer
            .iter()
            .next()
            .map_or(true, |(key, _value)| key.clone() > pivot_key));
        let (mut buffer, _) = child_buffer.take();
        buffer.append(&mut sibling.take().0);
        assert_eq!(this.buffer, buffer);
    }
}
