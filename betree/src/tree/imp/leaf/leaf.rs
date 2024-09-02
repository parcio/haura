//! Implementation of the [LeafNode] node type.
use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::HasStoragePreference,
    size::Size,
    storage_pool::AtomicSystemStoragePreference,
    tree::{pivot_key::LocalPivotKey, KeyInfo, MessageAction},
    AtomicStoragePreference, StoragePreference,
};

use super::{packed, FillUpResult};
use std::{borrow::Borrow, collections::BTreeMap, iter::FromIterator};

/// A leaf node of the tree holds pairs of keys values which are plain data.
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct LeafNode {
    storage_preference: AtomicStoragePreference,
    /// A storage preference assigned by the Migration Policy
    system_storage_preference: AtomicSystemStoragePreference,
    entries_size: usize,
    entries: BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>,
}


impl Size for LeafNode {
    fn size(&self) -> usize {
        packed::HEADER_FIXED_LEN + self.entries_size
    }

    fn actual_size(&self) -> Option<usize> {
        Some(
            packed::HEADER_FIXED_LEN
                + self
                    .entries
                    .iter()
                    .map(|(key, (_keyinfo, value))| packed::ENTRY_LEN + key.len() + value.len())
                    .sum::<usize>(),
        )
    }
}

impl HasStoragePreference for LeafNode {
    fn current_preference(&self) -> Option<StoragePreference> {
        self.storage_preference
            .as_option()
            .map(|pref| self.system_storage_preference.weak_bound(&pref))
    }

    fn recalculate(&self) -> StoragePreference {
        let mut pref = StoragePreference::NONE;

        for (keyinfo, _v) in self.entries.values() {
            pref.upgrade(keyinfo.storage_preference);
        }

        self.storage_preference.set(pref);
        self.system_storage_preference.weak_bound(&pref)
    }

    fn system_storage_preference(&self) -> StoragePreference {
        self.system_storage_preference.borrow().into()
    }

    fn set_system_storage_preference(&mut self, pref: StoragePreference) {
        self.system_storage_preference.set(pref)
    }
}

impl<'a> FromIterator<(CowBytes, (KeyInfo, SlicedCowBytes))> for LeafNode {
    fn from_iter<T: IntoIterator<Item = (CowBytes, (KeyInfo, SlicedCowBytes))>>(iter: T) -> Self {
        let mut storage_pref = StoragePreference::NONE;
        let mut entries_size = 0;

        let mut entries = BTreeMap::new();
        let mut needs_second_pass = false;

        for (key, (keyinfo, value)) in iter.into_iter() {
            // pref of overall node is highest pref from keys.
            // We're already looking at every entry here, so finding the overall pref here
            // avoids a full scan later.
            storage_pref.upgrade(keyinfo.storage_preference);
            let key_len = key.len();
            entries_size += packed::ENTRY_LEN + key_len + value.len();

            let curr_storage_pref = keyinfo.storage_preference;
            if let Some((ckeyinfo, cvalue)) = entries.insert(key, (keyinfo, value)) {
                // iterator has collisions, try to compensate
                //
                // this entry will no longer be part of the final map, subtract its size
                entries_size -= packed::ENTRY_LEN + key_len + cvalue.len();

                // In case the old value increased the overall storage priority (faster), and the new
                // value wouldn't have increased it as much, we might need to recalculate the
                // proper preference in a second pass.
                if ckeyinfo.storage_preference != curr_storage_pref {
                    needs_second_pass = true;
                }
            }
        }

        if needs_second_pass {
            storage_pref = StoragePreference::NONE;
            for (keyinfo, _value) in entries.values() {
                storage_pref.upgrade(keyinfo.storage_preference);
            }
        }

        LeafNode {
            storage_preference: AtomicStoragePreference::known(storage_pref),
            system_storage_preference: AtomicSystemStoragePreference::from(StoragePreference::NONE),
            entries_size,
            entries,
        }
    }
}

impl<'a> FromIterator<(&'a [u8], (KeyInfo, SlicedCowBytes))> for LeafNode {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (&'a [u8], (KeyInfo, SlicedCowBytes))>,
    {
        LeafNode::from_iter(iter.into_iter().map(|(a, b)| (CowBytes::from(a), b)))
    }
}

impl LeafNode {
    /// Constructs a new, empty `LeafNode`.
    pub fn new() -> Self {
        LeafNode {
            storage_preference: AtomicStoragePreference::known(StoragePreference::NONE),
            system_storage_preference: AtomicSystemStoragePreference::from(StoragePreference::NONE),
            entries_size: 0,
            entries: BTreeMap::new(),
        }
    }

    /// Returns the value for the given key.
    pub fn get(&self, key: &[u8]) -> Option<SlicedCowBytes> {
        self.entries.get(key).map(|(_info, data)| data).cloned()
    }

    pub(in crate::tree) fn get_with_info(&self, key: &[u8]) -> Option<(KeyInfo, SlicedCowBytes)> {
        self.entries.get(key).cloned()
    }

    pub(in crate::tree) fn entries(&self) -> &BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)> {
        &self.entries
    }

    pub(in crate::tree) fn entry_info(&mut self, key: &[u8]) -> Option<&mut KeyInfo> {
        self.entries.get_mut(key).map(|e| &mut e.0)
    }

    /// Split the node and transfer entries to a given other node `right_sibling`.
    /// Use entries which are, when summed up in-order, above the `min_size` limit.
    /// Returns new pivot key and size delta to the left sibling.
    fn do_split_off(
        &mut self,
        right_sibling: &mut Self,
        min_size: usize,
        max_size: usize,
    ) -> (CowBytes, isize) {
        debug_assert!(self.size() > max_size);
        debug_assert!(right_sibling.entries_size == 0);

        let mut sibling_size = 0;
        let mut sibling_pref = StoragePreference::NONE;
        let mut split_key = None;
        for (k, (keyinfo, v)) in self.entries.iter().rev() {
            sibling_size += packed::ENTRY_LEN + k.len() + v.len();
            sibling_pref.upgrade(keyinfo.storage_preference);

            if packed::HEADER_FIXED_LEN + sibling_size >= min_size {
                split_key = Some(k.clone());
                break;
            }
        }
        let split_key = split_key.unwrap();

        right_sibling.entries = self.entries.split_off(&split_key);
        self.entries_size -= sibling_size;
        right_sibling.entries_size = sibling_size;
        right_sibling.storage_preference.set(sibling_pref);

        // have removed many keys from self, no longer certain about own pref, mark invalid
        self.storage_preference.invalidate();

        let size_delta = -(sibling_size as isize);

        let pivot_key = self.entries.keys().next_back().cloned().unwrap();
        (pivot_key, size_delta)
    }

    pub fn apply<K>(&mut self, key: K, pref: StoragePreference) -> Option<KeyInfo>
    where
        K: Borrow<[u8]>,
    {
        self.storage_preference.invalidate();
        self.entries.get_mut(key.borrow()).map(|entry| {
            entry.0.storage_preference = pref;
            entry.0.clone()
        })
    }

    /// Inserts a new message as leaf entry.
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
        let size_before = self.entries_size as isize;
        let key_size = key.borrow().len();
        let mut data = self.get(key.borrow());
        msg_action.apply_to_leaf(key.borrow(), msg, &mut data);

        if let Some(data) = data {
            // Value was added or preserved by msg
            self.entries_size += data.len();
            self.storage_preference.upgrade(keyinfo.storage_preference);

            if let Some((old_info, old_data)) =
                self.entries.insert(key.into(), (keyinfo.clone(), data))
            {
                // There was a previous value in entries, which was now replaced
                self.entries_size -= old_data.len();

                // if previous entry was stricter than new entry, invalidate
                if old_info.storage_preference < keyinfo.storage_preference {
                    self.storage_preference.invalidate();
                }
            } else {
                // There was no previous value in entries
                self.entries_size += packed::ENTRY_LEN;
                self.entries_size += key_size;
            }
        } else if let Some((old_info, old_data)) = self.entries.remove(key.borrow()) {
            // The value was removed by msg, this may be a downgrade opportunity.
            // The preference of the removed entry can't be stricter than the current node
            // preference, by invariant. That leaves "less strict" and "as strict" as the
            // node preference:
            //
            // - less strict:
            //     If the preference of the removed entry is less strict than the current
            //     node preference, there must be another entry which is preventing a downgrade.
            // - as strict:
            //     The removed entry _may_ have caused the original upgrade to this preference,
            //     we'll have to trigger a scan to find out.
            if self.storage_preference.as_option() == Some(old_info.storage_preference) {
                self.storage_preference.invalidate();
            }

            self.entries_size -= packed::ENTRY_LEN;
            self.entries_size -= key_size;
            self.entries_size -= old_data.len();
        }
        self.entries_size as isize - size_before
    }

    /// Inserts messages as leaf entries.
    pub fn insert_msg_buffer<M, I>(&mut self, msg_buffer: I, msg_action: M) -> isize
    where
        M: MessageAction,
        I: IntoIterator<Item = (CowBytes, (KeyInfo, SlicedCowBytes))>,
    {
        let mut size_delta = 0;
        for (key, (keyinfo, msg)) in msg_buffer {
            size_delta += self.insert(key, keyinfo, msg, &msg_action);
        }
        size_delta
    }

    /// Splits this `LeafNode` into to two leaf nodes.
    /// Returns a new right sibling, the corresponding pivot key, and the size
    /// delta of this node.
    pub fn split(
        &mut self,
        min_size: usize,
        max_size: usize,
    ) -> (Self, CowBytes, isize, LocalPivotKey) {
        // assert!(self.size() > S::MAX);
        let mut right_sibling = LeafNode {
            // During a split, preference can't be inherited because the new subset of entries
            // might be a subset with a lower maximal preference.
            storage_preference: AtomicStoragePreference::known(StoragePreference::NONE),
            system_storage_preference: AtomicSystemStoragePreference::from(StoragePreference::NONE),
            entries_size: 0,
            entries: BTreeMap::new(),
        };

        // This adjusts sibling's size and pref according to its new entries
        let (pivot_key, size_delta) = self.do_split_off(&mut right_sibling, min_size, max_size);

        (
            right_sibling,
            pivot_key.clone(),
            size_delta,
            LocalPivotKey::Right(pivot_key),
        )
    }

    /// Merge all entries from the *right* node into the *left* node.  Returns
    /// the size change, positive for the left node, negative for the right
    /// node.
    pub fn merge(&mut self, right_sibling: &mut Self) -> isize {
        self.entries.append(&mut right_sibling.entries);
        let size_delta = right_sibling.entries_size;
        self.entries_size += right_sibling.entries_size;

        self.storage_preference
            .upgrade_atomic(&right_sibling.storage_preference);

        // right_sibling is now empty, reset to defaults
        right_sibling.entries_size = 0;
        right_sibling
            .storage_preference
            .set(StoragePreference::NONE);

        size_delta as isize
    }

    /// Rebalances `self` and `right_sibling`. Returns `Merged`
    /// if all entries of `right_sibling` have been merged into this node.
    /// Otherwise, returns a new pivot key.
    pub fn rebalance(
        &mut self,
        right_sibling: &mut Self,
        min_size: usize,
        max_size: usize,
    ) -> FillUpResult {
        let size_delta = self.merge(right_sibling);
        if self.size() <= max_size {
            FillUpResult::Merged { size_delta }
        } else {
            // First size_delta is from the merge operation where we split
            let (pivot_key, split_size_delta) =
                self.do_split_off(right_sibling, min_size, max_size);
            FillUpResult::Rebalanced {
                pivot_key,
                size_delta: size_delta + split_size_delta,
            }
        }
    }

    pub fn to_memory_leaf(self) -> super::copyless_leaf::CopylessLeaf {
        todo!()
    }

    /*pub fn range_delete(&mut self, start: &[u8], end: Option<&[u8]>) -> usize {
        // https://github.com/rust-lang/rust/issues/42849
        let size_before = self.entries_size;
        let range = (
            Bound::Included(start),
            end.map_or(Bound::Unbounded, Bound::Excluded),
        );
        let mut keys = Vec::new();
        for (key, (_keyinfo, value)) in self.entries.range_mut::<[u8], _>(range) {
            self.entries_size -= key.len() + value.len();
            keys.push(key.clone());
        }
        for key in keys {
            self.entries.remove(&key);
        }
        size_before - self.entries_size
    }*/
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::{CowBytes, LeafNode, Size};
    use crate::{
        arbitrary::GenExt,
        data_management::HasStoragePreference,
        tree::{
            default_message_action::{DefaultMessageAction, DefaultMessageActionMsg},
            imp::leaf::PackedMap,
            KeyInfo,
        },
        StoragePreference,
    };
    use quickcheck::{Arbitrary, Gen, TestResult};
    use rand::Rng;

    impl Arbitrary for KeyInfo {
        fn arbitrary(g: &mut Gen) -> Self {
            let sp = g.rng().gen_range(0..=3);
            KeyInfo {
                storage_preference: StoragePreference::from_u8(sp),
            }
        }
    }

    impl Arbitrary for LeafNode {
        fn arbitrary(g: &mut Gen) -> Self {
            let len = g.rng().gen_range(0..20);
            let entries: Vec<_> = (0..len)
                .map(|_| {
                    (
                        CowBytes::arbitrary(g),
                        DefaultMessageActionMsg::arbitrary(g),
                    )
                })
                .map(|(k, v)| (k, v.0))
                .collect();

            let node: LeafNode = entries
                .iter()
                .map(|(k, v)| (&k[..], (KeyInfo::arbitrary(g), v.clone())))
                .collect();
            node.recalculate();
            node
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            let v: Vec<_> = self
                .entries
                .clone()
                .into_iter()
                .map(|(k, (info, v))| (k, (info, CowBytes::from(v.to_vec()))))
                .collect();
            Box::new(v.shrink().map(|entries| {
                entries
                    .iter()
                    .map(|(k, (info, v))| (&k[..], (info.clone(), v.clone().into())))
                    .collect()
            }))
        }
    }

    fn serialized_size(leaf_node: &LeafNode) -> usize {
        let mut data = Vec::new();
        PackedMap::pack(leaf_node, &mut data).unwrap();
        data.len()
    }

    #[quickcheck]
    fn check_actual_size(leaf_node: LeafNode) {
        assert_eq!(leaf_node.actual_size(), Some(serialized_size(&leaf_node)));
    }

    #[quickcheck]
    fn check_serialize_size(leaf_node: LeafNode) {
        let size = leaf_node.size();
        let serialized = serialized_size(&leaf_node);
        if size != serialized {
            eprintln!(
                "leaf {:?}, size {}, actual_size {:?}, serialized_size {}",
                leaf_node,
                size,
                leaf_node.actual_size(),
                serialized
            );
            assert_eq!(size, serialized);
        }
    }

    #[quickcheck]
    fn check_serialization(leaf_node: LeafNode) {
        let mut data = Vec::new();
        assert!(data.write(&[0; crate::tree::imp::node::NODE_PREFIX_LEN]).unwrap() == 4);
        PackedMap::pack(&leaf_node, &mut data).unwrap();
        let twin = PackedMap::new(data.into_boxed_slice()).unpack_leaf();

        assert_eq!(leaf_node, twin);
    }

    #[quickcheck]
    fn check_size_insert(
        mut leaf_node: LeafNode,
        key: CowBytes,
        key_info: KeyInfo,
        msg: DefaultMessageActionMsg,
    ) {
        let size_before = leaf_node.size();
        let size_delta = leaf_node.insert(key, key_info, msg.0, DefaultMessageAction);
        let size_after = leaf_node.size();
        assert_eq!((size_before as isize + size_delta) as usize, size_after);
        assert_eq!({ serialized_size(&leaf_node) }, size_after);
    }

    const MIN_LEAF_SIZE: usize = 512;
    const MAX_LEAF_SIZE: usize = 2048;

    #[quickcheck]
    fn check_size_split(mut leaf_node: LeafNode) -> TestResult {
        let size_before = leaf_node.size();

        if size_before <= MAX_LEAF_SIZE {
            return TestResult::discard();
        }

        let (sibling, _, size_delta, _pivot_key) = leaf_node.split(MIN_LEAF_SIZE, MAX_LEAF_SIZE);
        assert_eq!({ serialized_size(&leaf_node) }, leaf_node.size());
        assert_eq!({ serialized_size(&sibling) }, sibling.size());
        assert_eq!(
            (size_before as isize + size_delta) as usize,
            leaf_node.size()
        );
        assert!(sibling.size() <= MAX_LEAF_SIZE);
        assert!(sibling.size() >= MIN_LEAF_SIZE);
        // assert!(leaf_node.size() >= MIN_LEAF_SIZE);
        TestResult::passed()
    }

    #[quickcheck]
    fn check_split_merge_idempotent(mut leaf_node: LeafNode) -> TestResult {
        if leaf_node.size() <= MAX_LEAF_SIZE {
            return TestResult::discard();
        }
        let this = leaf_node.clone();
        let (mut sibling, ..) = leaf_node.split(MIN_LEAF_SIZE, MAX_LEAF_SIZE);
        leaf_node.recalculate();
        leaf_node.merge(&mut sibling);
        assert_eq!(this, leaf_node);
        TestResult::passed()
    }
}
