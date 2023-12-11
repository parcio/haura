//! Implementation of the [InternalNode] node type.
use super::{
    child_buffer::ChildBuffer,
    node::{PivotGetMutResult, PivotGetResult},
    PivotKey,
};
use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::{HasStoragePreference, ObjectReference},
    database::DatasetId,
    size::{Size, SizeMut, StaticSize},
    storage_pool::AtomicSystemStoragePreference,
    tree::{pivot_key::LocalPivotKey, KeyInfo, MessageAction},
    AtomicStoragePreference, StoragePreference,
};
use bincode::serialized_size;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, collections::BTreeMap, mem::replace};

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub(super) struct InternalNode<N: 'static> {
    level: u32,
    entries_size: usize,
    #[serde(skip)]
    system_storage_preference: AtomicSystemStoragePreference,
    #[serde(skip)]
    pref: AtomicStoragePreference,
    pub(super) pivot: Vec<CowBytes>,
    children: Vec<ChildBuffer<N>>,
}

// @tilpner:
// Previously, this literal was magically spread across the code below, and I've (apparently
// correctly) guessed it to be the fixed size of an empty InternalNode<_> when encoded with bincode.
// I've added a test below to verify this and to ensure any bincode-sided change is noticed.
// This is still wrong because:
//
// * usize is platform-dependent, 28 is not. Size will be impl'd incorrectly on 32b platforms
//   * not just the top-level usize, Vec contains further address-sized fields, though bincode
//     might special-case Vec encoding so that this doesn't matter
// * the bincode format may not have changed in a while, but that's not a guarantee
//
// I'm not going to fix them, because the proper fix would be to take bincode out of everything,
// and that's a lot of implementation and testing effort. You should though, if you find the time.
// @jwuensche:
// Added TODO to better find this in the future.
// Will definitely need to adjust this at some point, though this is not now.
// const TEST_BINCODE_FIXED_SIZE: usize = 28;
//
// UPDATE:
// We removed by now the fixed constant and determine the base size of an
// internal node with bincode provided methods based on an empty node created on
// compile-time. We might want to store this value for future access or even
// better determine the size on compile time directly, this requires
// `serialized_size` to be const which it could but its not on their task list
// yet.

// NOTE: Waiting for OnceCell to be stabilized...
// https://doc.rust-lang.org/stable/std/cell/struct.OnceCell.html
static EMPTY_NODE: InternalNode<()> = InternalNode {
    level: 0,
    entries_size: 0,
    system_storage_preference: AtomicSystemStoragePreference::none(),
    pref: AtomicStoragePreference::unknown(),
    pivot: vec![],
    children: vec![],
};

#[inline]
fn internal_node_base_size() -> usize {
    // NOTE: The overhead introduced by using `serialized_size` is negligible
    // and only about 3ns, but we can use OnceCell once (🥁) it is available.
    serialized_size(&EMPTY_NODE)
        .expect("Known node layout could not be estimated. This is an error in bincode.")
        // We know that this is valid as the maximum size in bytes is below u32
        as usize
}

impl<N: StaticSize> Size for InternalNode<N> {
    fn size(&self) -> usize {
        internal_node_base_size() + self.entries_size
    }

    fn actual_size(&self) -> Option<usize> {
        Some(
            internal_node_base_size()
                + self.pivot.iter().map(Size::size).sum::<usize>()
                + self
                    .children
                    .iter()
                    .map(|child| {
                        child
                            .checked_size()
                            .expect("Child doesn't impl actual_size")
                    })
                    .sum::<usize>(),
        )
    }
}

impl<N: HasStoragePreference> HasStoragePreference for InternalNode<N> {
    fn current_preference(&self) -> Option<StoragePreference> {
        self.pref
            .as_option()
            .map(|pref| self.system_storage_preference.weak_bound(&pref))
    }

    fn recalculate(&self) -> StoragePreference {
        let mut pref = StoragePreference::NONE;

        for child in &self.children {
            pref.upgrade(child.correct_preference())
        }

        self.pref.set(pref);
        pref
    }

    fn correct_preference(&self) -> StoragePreference {
        self.system_storage_preference
            .weak_bound(&self.recalculate())
    }

    fn system_storage_preference(&self) -> StoragePreference {
        self.system_storage_preference.borrow().into()
    }

    fn set_system_storage_preference(&mut self, pref: StoragePreference) {
        self.system_storage_preference.set(pref);
    }
}

impl<N> InternalNode<N> {
    pub fn new(left_child: ChildBuffer<N>, right_child: ChildBuffer<N>, pivot_key: CowBytes, level: u32) -> Self
    where
        N: StaticSize,
    {
        InternalNode {
            level,
            entries_size: left_child.size() + right_child.size() + pivot_key.size(),
            pivot: vec![pivot_key],
            children: vec![left_child, right_child],
            system_storage_preference: AtomicSystemStoragePreference::from(StoragePreference::NONE),
            pref: AtomicStoragePreference::unknown(),
        }
    }

    /// Returns the number of children.
    pub fn fanout(&self) -> usize  where N: ObjectReference {
        self.children.len()
    }

    /// Returns the level of this node.
    pub fn level(&self) -> u32 {
        self.level
    }

    /// Returns the index of the child buffer
    /// corresponding to the given `key`.
    fn idx(&self, key: &[u8]) -> usize {
        match self
            .pivot
            .binary_search_by(|pivot_key| pivot_key.as_ref().cmp(key))
        {
            Ok(idx) | Err(idx) => idx,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &ChildBuffer<N>> + '_ where N: ObjectReference{
        self.children.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut ChildBuffer<N>> + '_  where N: ObjectReference {
        self.children.iter_mut()
    }

    pub fn iter_with_bounds(
        &self,
    ) -> impl Iterator<Item = (Option<&CowBytes>, &ChildBuffer<N>, Option<&CowBytes>)> + '_  where N: ObjectReference{
        self.children.iter().enumerate().map(move |(idx, child)| {
            let maybe_left = if idx == 0 {
                None
            } else {
                self.pivot.get(idx - 1)
            };

            let maybe_right = self.pivot.get(idx);

            (maybe_left, child, maybe_right)
        })
    }
}

impl<N> InternalNode<N> {
    pub fn get(&self, key: &[u8]) -> (&RwLock<N>, Option<(KeyInfo, SlicedCowBytes)>)  where N: ObjectReference {
        let child = &self.children[self.idx(key)];

        let msg = child.get(key).cloned();
        (&child.node_pointer, msg)
    }

    pub fn pivot_get(&self, pk: &PivotKey) -> PivotGetResult<N>  where N: ObjectReference{
        // Exact pivot matches are required only
        debug_assert!(!pk.is_root());
        let pivot = pk.bytes().unwrap();
        self.pivot
            .iter()
            .enumerate()
            .find(|(_idx, p)| **p == pivot)
            .map_or_else(
                || {
                    // Continue the search to the next level
                    let child = &self.children[self.idx(&pivot)];
                    PivotGetResult::NextNode(&child.node_pointer)
                },
                |(idx, _)| {
                    // Fetch the correct child pointer
                    let child;
                    if pk.is_left() {
                        child = &self.children[idx];
                    } else {
                        child = &self.children[idx + 1];
                    }
                    PivotGetResult::Target(Some(&child.node_pointer))
                },
            )
    }

    pub fn pivot_get_mut(&mut self, pk: &PivotKey) -> PivotGetMutResult<N>  where N: ObjectReference{
        // Exact pivot matches are required only
        debug_assert!(!pk.is_root());
        let pivot = pk.bytes().unwrap();
        let (id, is_target) = self
            .pivot
            .iter()
            .enumerate()
            .find(|(_idx, p)| **p == pivot)
            .map_or_else(
                || {
                    // Continue the search to the next level
                    (self.idx(&pivot), false)
                },
                |(idx, _)| {
                    // Fetch the correct child pointer
                    (idx, true)
                },
            );
        match (is_target, pk.is_left()) {
            (true, true) => {
                PivotGetMutResult::Target(Some(self.children[id].node_pointer.get_mut()))
            }
            (true, false) => {
                PivotGetMutResult::Target(Some(self.children[id + 1].node_pointer.get_mut()))
            }
            (false, _) => PivotGetMutResult::NextNode(self.children[id].node_pointer.get_mut()),
        }
    }

    pub fn apply_with_info(&mut self, key: &[u8], pref: StoragePreference) -> &mut N  where N: ObjectReference {
        let idx = self.idx(key);
        let child = &mut self.children[idx];

        child.apply_with_info(key, pref);
        child.node_pointer.get_mut()
    }

    pub fn get_range(
        &self,
        key: &[u8],
        left_pivot_key: &mut Option<CowBytes>,
        right_pivot_key: &mut Option<CowBytes>,
        all_msgs: &mut BTreeMap<CowBytes, Vec<(KeyInfo, SlicedCowBytes)>>,
    ) -> &RwLock<N> {
        let idx = self.idx(key);
        if idx > 0 {
            *left_pivot_key = Some(self.pivot[idx - 1].clone());
        }
        if idx < self.pivot.len() {
            *right_pivot_key = Some(self.pivot[idx].clone());
        }
        let child = &self.children[idx];
        for (key, msg) in child.get_all_messages() {
            all_msgs
                .entry(key.clone())
                .or_insert_with(Vec::new)
                .push(msg.clone());
        }

        &child.node_pointer
    }

    pub fn get_next_node(&self, key: &[u8]) -> Option<&RwLock<N>> {
        let idx = self.idx(key) + 1;
        self.children.get(idx).map(|child| &child.node_pointer)
    }

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
        N: ObjectReference
    {
        self.pref.invalidate();
        let idx = self.idx(key.borrow());
        let added_size = self.children[idx].insert(key, keyinfo, msg, msg_action);

        if added_size > 0 {
            self.entries_size += added_size as usize;
        } else {
            self.entries_size -= -added_size as usize;
        }
        added_size
    }

    pub fn insert_msg_buffer<I, M>(&mut self, iter: I, msg_action: M) -> isize
    where
        I: IntoIterator<Item = (CowBytes, (KeyInfo, SlicedCowBytes))>,
        M: MessageAction,
        N: ObjectReference
    {
        self.pref.invalidate();
        let mut added_size = 0;
        let mut buf_storage_pref = StoragePreference::NONE;

        for (k, (keyinfo, v)) in iter.into_iter() {
            let idx = self.idx(&k);
            buf_storage_pref.upgrade(keyinfo.storage_preference);
            added_size += self.children[idx].insert(k, keyinfo, v, &msg_action);
        }

        if added_size > 0 {
            self.entries_size += added_size as usize;
        } else {
            self.entries_size -= -added_size as usize;
        }
        added_size
    }

    pub fn drain_children(&mut self) -> impl Iterator<Item = N> + '_  where N: ObjectReference {
        self.pref.invalidate();
        self.entries_size = 0;
        self.children
            .drain(..)
            .map(|child| child.node_pointer.into_inner())
    }
}

impl<N: StaticSize + HasStoragePreference> InternalNode<N> {
    pub fn range_delete(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        dead: &mut Vec<N>,
    ) -> (usize, &mut N, Option<&mut N>) 
    where N: ObjectReference {
        self.pref.invalidate();
        let size_before = self.entries_size;
        let start_idx = self.idx(start);
        let end_idx = end.map_or(self.children.len() - 1, |i| self.idx(i));
        if start_idx == end_idx {
            let size_delta = self.children[start_idx].range_delete(start, end);
            return (
                size_delta,
                self.children[start_idx].node_pointer.get_mut(),
                None,
            );
        }
        // Skip children that may overlap.
        let dead_start_idx = start_idx + 1;
        let dead_end_idx = end_idx - end.is_some() as usize;
        if dead_start_idx <= dead_end_idx {
            for pivot_key in self.pivot.drain(dead_start_idx..dead_end_idx) {
                self.entries_size -= pivot_key.size();
            }
            let entries_size = &mut self.entries_size;
            dead.extend(
                self.children
                    .drain(dead_start_idx..=dead_end_idx)
                    .map(|child| {
                        *entries_size -= child.size();
                        child.node_pointer.into_inner()
                    }),
            );
        }

        let (left_child, mut right_child) = {
            let (left, right) = self.children.split_at_mut(start_idx + 1);
            (&mut left[start_idx], end.map(move |_| &mut right[0]))
        };
        self.entries_size -= left_child.range_delete(start, None);
        if let Some(ref mut child) = right_child {
            self.entries_size -= child.range_delete(start, end);
        }
        let size_delta = size_before - self.entries_size;

        (
            size_delta,
            left_child.node_pointer.get_mut(),
            right_child.map(|child| child.node_pointer.get_mut()),
        )
    }
}

impl<N: ObjectReference> InternalNode<N> {
    pub fn split(&mut self) -> (Self, CowBytes, isize, LocalPivotKey) {
        self.pref.invalidate();
        let split_off_idx = self.fanout() / 2;
        let pivot = self.pivot.split_off(split_off_idx);
        let pivot_key = self.pivot.pop().unwrap();
        let mut children = self.children.split_off(split_off_idx);

        if let (Some(new_left_outer), Some(new_left_pivot)) = (children.first_mut(), pivot.first())
        {
            new_left_outer.update_pivot_key(LocalPivotKey::LeftOuter(new_left_pivot.clone()))
        }

        let entries_size = pivot.iter().map(Size::size).sum::<usize>()
            + children.iter_mut().map(SizeMut::size).sum::<usize>();

        let size_delta = entries_size + pivot_key.size();
        self.entries_size -= size_delta;

        let right_sibling = InternalNode {
            level: self.level,
            entries_size,
            pivot,
            children,
            // Copy the system storage preference of the other node as we cannot
            // be sure which key was targeted by recorded accesses.
            system_storage_preference: self.system_storage_preference.clone(),
            pref: AtomicStoragePreference::unknown(),
        };
        (
            right_sibling,
            pivot_key.clone(),
            -(size_delta as isize),
            LocalPivotKey::Right(pivot_key),
        )
    }

    pub fn merge(&mut self, right_sibling: &mut Self, old_pivot_key: CowBytes) -> isize {
        self.pref.invalidate();
        let size_delta = right_sibling.entries_size + old_pivot_key.size();
        self.entries_size += size_delta;
        self.pivot.push(old_pivot_key);
        self.pivot.append(&mut right_sibling.pivot);
        self.children.append(&mut right_sibling.children);

        size_delta as isize
    }

    /// Translate any object ref in a `ChildBuffer` from `Incomplete` to `Unmodified` state.
    pub fn complete_object_refs(mut self, d_id: DatasetId) -> Self {
        // TODO:
        let first_pk = match self.pivot.first() {
            Some(p) => PivotKey::LeftOuter(p.clone(), d_id),
            None => unreachable!(
                "The store contains an empty InternalNode, this should never be the case."
            ),
        };
        for (id, pk) in [first_pk]
            .into_iter()
            .chain(self.pivot.iter().map(|p| PivotKey::Right(p.clone(), d_id)))
            .enumerate()
        {
            // SAFETY: There must always be pivots + 1 many children, otherwise
            // the state of the Internal Node is broken.
            self.children[id].complete_object_ref(pk)
        }
        self
    }
}

impl<N: HasStoragePreference> InternalNode<N>
where
    N: StaticSize,
    N: ObjectReference
{
    pub fn try_walk(&mut self, key: &[u8]) -> Option<TakeChildBuffer<N>> {
        let child_idx = self.idx(key);
        if self.children[child_idx].is_empty(key) {
            Some(TakeChildBuffer {
                node: self,
                child_idx,
            })
        } else {
            None
        }
    }

    pub fn try_find_flush_candidate(
        &mut self,
        min_flush_size: usize,
        max_node_size: usize,
        min_fanout: usize,
    ) -> Option<TakeChildBuffer<N>> where N: ObjectReference{
        let child_idx = {
            let size = self.size();
            let fanout = self.fanout();
            let (child_idx, child) = self
                .children
                .iter()
                .enumerate()
                .max_by_key(|&(_, child)| child.buffer_size())
                .unwrap();

            debug!("Largest child's buffer size: {}", child.buffer_size());

            if child.buffer_size() >= min_flush_size
                && (size - child.buffer_size() <= max_node_size || fanout < 2 * min_fanout)
            {
                Some(child_idx)
            } else {
                None
            }
        };
        child_idx.map(move |child_idx| TakeChildBuffer {
            node: self,
            child_idx,
        })
    }
}

pub(super) struct TakeChildBuffer<'a, N: 'a + 'static> {
    node: &'a mut InternalNode<N>,
    child_idx: usize,
}

impl<'a, N: StaticSize + HasStoragePreference> TakeChildBuffer<'a, N> {
    pub(super) fn split_child(
        &mut self,
        sibling_np: N,
        pivot_key: CowBytes,
        select_right: bool,
    ) -> isize {
        // split_at invalidates both involved children (old and new), but as the new child
        // is added to self, the overall entries don't change, so this node doesn't need to be
        // invalidated

        let sibling = self.node.children[self.child_idx].split_at(&pivot_key, sibling_np);
        let size_delta = sibling.size() + pivot_key.size();
        self.node.children.insert(self.child_idx + 1, sibling);
        self.node.pivot.insert(self.child_idx, pivot_key);
        self.node.entries_size += size_delta;
        if select_right {
            self.child_idx += 1;
        }
        size_delta as isize
    }
}

impl<'a, N> TakeChildBuffer<'a, N>
where
    N: StaticSize,
{
    pub(super) fn size(&self) -> usize {
        Size::size(&*self.node)
    }

    pub(super) fn prepare_merge(&mut self) -> PrepareMergeChild<N> where N: ObjectReference {
        if self.child_idx + 1 < self.node.children.len() {
            PrepareMergeChild {
                node: self.node,
                pivot_key_idx: self.child_idx,
                other_child_idx: self.child_idx + 1,
            }
        } else {
            PrepareMergeChild {
                node: self.node,
                pivot_key_idx: self.child_idx - 1,
                other_child_idx: self.child_idx - 1,
            }
        }
    }
}

pub(super) struct PrepareMergeChild<'a, N: 'a + 'static> {
    node: &'a mut InternalNode<N>,
    pivot_key_idx: usize,
    other_child_idx: usize,
}

impl<'a, N> PrepareMergeChild<'a, N> {
    pub(super) fn sibling_node_pointer(&mut self) -> &mut RwLock<N> where N: ObjectReference {
        &mut self.node.children[self.other_child_idx].node_pointer
    }
    pub(super) fn is_right_sibling(&self) -> bool {
        self.pivot_key_idx != self.other_child_idx
    }
}

pub(super) struct MergeChildResult<NP> {
    pub(super) pivot_key: CowBytes,
    pub(super) old_np: NP,
    pub(super) size_delta: isize,
}

impl<'a, N: Size + HasStoragePreference> PrepareMergeChild<'a, N> {
    pub(super) fn merge_children(self) -> MergeChildResult<N> {
        let mut right_sibling = self.node.children.remove(self.pivot_key_idx + 1);
        let pivot_key = self.node.pivot.remove(self.pivot_key_idx);
        let size_delta =
            pivot_key.size() + ChildBuffer::<N>::static_size() + right_sibling.node_pointer.size();
        self.node.entries_size -= size_delta;

        let left_sibling = &mut self.node.children[self.pivot_key_idx];
        left_sibling.append(&mut right_sibling);
        left_sibling
            .messages_preference
            .upgrade_atomic(&right_sibling.messages_preference);

        MergeChildResult {
            pivot_key,
            old_np: right_sibling.node_pointer.into_inner(),
            size_delta: -(size_delta as isize),
        }
    }
}

impl<'a, N: Size + HasStoragePreference> PrepareMergeChild<'a, N> {
    fn get_children(&mut self) -> (&mut ChildBuffer<N>, &mut ChildBuffer<N>) {
        let (left, right) = self.node.children[self.pivot_key_idx..].split_at_mut(1);
        (&mut left[0], &mut right[0])
    }

    pub(super) fn rebalanced(&mut self, new_pivot_key: CowBytes) -> isize {
        {
            // Move messages around
            let (left_child, right_child) = self.get_children();
            left_child.rebalance(right_child, &new_pivot_key);
        }

        let mut size_delta = new_pivot_key.size() as isize;
        let old_pivot_key = replace(&mut self.node.pivot[self.pivot_key_idx], new_pivot_key);
        size_delta -= old_pivot_key.size() as isize;

        size_delta
    }
}

impl<'a, N: Size + HasStoragePreference> TakeChildBuffer<'a, N> {
    pub fn node_pointer_mut(&mut self) -> &mut RwLock<N> where N: ObjectReference{
        &mut self.node.children[self.child_idx].node_pointer
    }
    pub fn take_buffer(&mut self) -> (BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>, isize) where N: ObjectReference{
        let (buffer, size_delta) = self.node.children[self.child_idx].take();
        self.node.entries_size -= size_delta;
        (buffer, -(size_delta as isize))
    }
}

#[cfg(test)]
mod tests {
    

    use super::*;
    use crate::{
        arbitrary::GenExt,
        database::DatasetId,
        tree::default_message_action::{DefaultMessageAction, DefaultMessageActionMsg},
    };
    use bincode::serialized_size;
    
    use quickcheck::{Arbitrary, Gen, TestResult};
    use rand::Rng;
    use serde::Serialize;

    // Keys are not allowed to be empty. This is usually caught at the tree layer, but these are
    // bypassing that check. There's probably a good way to do this, but we can also just throw
    // away the empty keys until we find one that isn't empty.
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct Key(CowBytes);
    impl Arbitrary for Key {
        fn arbitrary(g: &mut Gen) -> Self {
            loop {
                let c = CowBytes::arbitrary(g);
                if !c.is_empty() {
                    return Key(c);
                }
            }
        }
    }

    impl<T: Clone> Clone for InternalNode<T> {
        fn clone(&self) -> Self {
            InternalNode {
                level: self.level,
                entries_size: self.entries_size,
                pivot: self.pivot.clone(),
                children: self.children.to_vec(),
                system_storage_preference: self.system_storage_preference.clone(),
                pref: self.pref.clone(),
            }
        }
    }

    impl<T: Arbitrary + Size> Arbitrary for InternalNode<T> {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut rng = g.rng();
            let pivot_key_cnt = rng.gen_range(1..20);
            let mut entries_size = 0;

            let mut pivot = Vec::with_capacity(pivot_key_cnt);
            for _ in 0..pivot_key_cnt {
                let pivot_key = CowBytes::arbitrary(g);
                entries_size += pivot_key.size();
                pivot.push(pivot_key);
            }

            let mut children = Vec::with_capacity(pivot_key_cnt + 1);
            for _ in 0..pivot_key_cnt + 1 {
                let child = T::arbitrary(g);
                entries_size += child.size();
                children.push(child);
            }

            InternalNode {
                pivot,
                children,
                entries_size,
                level: 1,
                system_storage_preference: AtomicSystemStoragePreference::from(
                    StoragePreference::NONE,
                ),
                pref: AtomicStoragePreference::unknown(),
            }
        }
    }

    fn check_size<T: Serialize + Size>(node: &mut InternalNode<T>) {
        assert_eq!(
            node.size() as u64,
            serialized_size(node).unwrap(),
            "predicted size does not match serialized size"
        );
    }

    #[quickcheck]
    fn check_serialize_size(mut node: InternalNode<CowBytes>) {
        check_size(&mut node);
    }

    #[quickcheck]
    fn check_idx(node: InternalNode<()>, key: Key) {
        let key = key.0;
        let idx = node.idx(&key);

        if let Some(upper_key) = node.pivot.get(idx) {
            assert!(&key <= upper_key);
        }
        if idx > 0 {
            let lower_key = &node.pivot[idx - 1];
            assert!(lower_key < &key);
        }
    }

    #[quickcheck]
    fn check_size_insert_single(
        mut node: InternalNode<ChildBuffer<()>>,
        key: Key,
        keyinfo: KeyInfo,
        msg: DefaultMessageActionMsg,
    ) {
        let size_before = node.size() as isize;
        let added_size = node.insert(key.0, keyinfo, msg.0, DefaultMessageAction);
        assert_eq!(size_before + added_size, node.size() as isize);

        check_size(&mut node);
    }

    #[quickcheck]
    fn check_size_insert_msg_buffer(
        mut node: InternalNode<ChildBuffer<()>>,
        buffer: BTreeMap<Key, (KeyInfo, DefaultMessageActionMsg)>,
    ) {
        let size_before = node.size() as isize;
        let added_size = node.insert_msg_buffer(
            buffer
                .into_iter()
                .map(|(Key(key), (keyinfo, msg))| (key, (keyinfo, msg.0))),
            DefaultMessageAction,
        );
        assert_eq!(
            size_before + added_size,
            node.size() as isize,
            "size delta mismatch"
        );

        check_size(&mut node);
    }

    #[quickcheck]
    fn check_insert_msg_buffer(
        mut node: InternalNode<ChildBuffer<()>>,
        buffer: BTreeMap<Key, (KeyInfo, DefaultMessageActionMsg)>,
    ) {
        let mut node_twin = node.clone();
        let added_size = node.insert_msg_buffer(
            buffer
                .iter()
                .map(|(Key(key), (keyinfo, msg))| (key.clone(), (keyinfo.clone(), msg.0.clone()))),
            DefaultMessageAction,
        );

        let mut added_size_twin = 0;
        for (Key(key), (keyinfo, msg)) in buffer {
            let idx = node_twin.idx(&key);
            added_size_twin +=
                node_twin.children[idx].insert(key, keyinfo, msg.0, DefaultMessageAction);
        }
        if added_size_twin > 0 {
            node_twin.entries_size += added_size_twin as usize;
        } else {
            node_twin.entries_size -= -added_size_twin as usize;
        }

        assert_eq!(node, node_twin);
        assert_eq!(added_size, added_size_twin);
    }

    static mut PK: Option<PivotKey> = None;

    impl ObjectReference for () {
        type ObjectPointer = ();

        fn get_unmodified(&self) -> Option<&Self::ObjectPointer> {
            Some(&())
        }

        fn set_index(&mut self, _pk: PivotKey) {
            // NO-OP
        }

        fn index(&self) -> &PivotKey {
            unsafe {
                if PK.is_none() {
                    PK = Some(PivotKey::LeftOuter(
                        CowBytes::from(vec![42u8]),
                        DatasetId::default(),
                    ));
                }
                PK.as_ref().unwrap()
            }
        }
    }

    #[quickcheck]
    fn check_size_split(mut node: InternalNode<ChildBuffer<()>>) -> TestResult {
        if node.fanout() < 2 {
            return TestResult::discard();
        }
        let size_before = node.size();
        let (mut right_sibling, _pivot, size_delta, _pivot_key) = node.split();
        assert_eq!(size_before as isize + size_delta, node.size() as isize);
        check_size(&mut node);
        check_size(&mut right_sibling);

        TestResult::passed()
    }

    #[quickcheck]
    fn check_split(mut node: InternalNode<ChildBuffer<()>>) -> TestResult {
        if node.fanout() < 4 {
            return TestResult::discard();
        }
        let twin = node.clone();
        let (mut right_sibling, pivot, _size_delta, _pivot_key) = node.split();

        assert!(node.fanout() >= 2);
        assert!(right_sibling.fanout() >= 2);

        node.entries_size += pivot.size() + right_sibling.entries_size;
        node.pivot.push(pivot);
        node.pivot.append(&mut right_sibling.pivot);
        node.children.append(&mut right_sibling.children);

        assert_eq!(node, twin);

        TestResult::passed()
    }

    #[quickcheck]
    fn check_split_key(mut node: InternalNode<ChildBuffer<()>>) -> TestResult {
        if node.fanout() < 4 {
            return TestResult::discard();
        }
        let (right_sibling, pivot, _size_delta, pivot_key) = node.split();
        assert!(node.fanout() >= 2);
        assert!(right_sibling.fanout() >= 2);
        assert_eq!(LocalPivotKey::Right(pivot), pivot_key);
        TestResult::passed()
    }

    // #[test]
    // fn check_constant() {
    //     let node: InternalNode<ChildBuffer<()>> = InternalNode {
    //         entries_size: 0,
    //         level: 1,
    //         children: vec![],
    //         pivot: vec![],
    //         system_storage_preference: AtomicSystemStoragePreference::from(StoragePreference::NONE),
    //         pref: AtomicStoragePreference::unknown(),
    //     };

    //     assert_eq!(
    //         serialized_size(&node).unwrap(),
    //         TEST_BINCODE_FIXED_SIZE as u64,
    //         "magic constants are wrong"
    //     );
    // }

    // TODO tests
    // split
    // child split
    // flush buffer
    // get with max_msn
}
