use super::child_buffer::ChildBuffer;
use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::HasStoragePreference,
    size::{Size, SizeMut, StaticSize},
    tree::{KeyInfo, MessageAction},
    StoragePreference,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, collections::BTreeMap, mem::replace};

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub(super) struct InternalNode<T> {
    level: u32,
    entries_size: usize,
    pub(super) pivot: Vec<CowBytes>,
    children: Vec<T>,
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
const BINCODE_FIXED_SIZE: usize = 28;

impl<T: Size> Size for InternalNode<T> {
    fn size(&self) -> usize {
        BINCODE_FIXED_SIZE + self.entries_size
    }

    fn actual_size(&self) -> Option<usize> {
        Some(
            BINCODE_FIXED_SIZE
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

impl<T: HasStoragePreference> HasStoragePreference for InternalNode<T> {
    // TODO: This should by convention really not recalculte based on this request
    // Furthermore, have a look at the HasStoragePreference trait.
    // We might perform way more operations than really should.
    fn current_preference(&self) -> Option<StoragePreference> {
        Some(self.recalculate())
    }

    fn recalculate(&self) -> StoragePreference {
        let mut pref = StoragePreference::NONE;

        for child in &self.children {
            pref.upgrade(child.correct_preference())
        }

        pref
    }

    fn correct_preference(&self) -> StoragePreference {
        self.recalculate()
    }
}

impl<T> InternalNode<T> {
    pub fn new(left_child: T, right_child: T, pivot_key: CowBytes, level: u32) -> Self
    where
        T: Size,
    {
        InternalNode {
            level,
            entries_size: left_child.size() + right_child.size() + pivot_key.size(),
            pivot: vec![pivot_key],
            children: vec![left_child, right_child],
        }
    }

    /// Returns the number of children.
    pub fn fanout(&self) -> usize {
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

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = &'a T> + 'a {
        self.children.iter()
    }

    pub fn iter_mut<'a>(&'a mut self) -> impl Iterator<Item = &'a mut T> + 'a {
        self.children.iter_mut()
    }

    pub fn iter_with_bounds<'a>(
        &'a self,
    ) -> impl Iterator<Item = (Option<&'a CowBytes>, &'a T, Option<&'a CowBytes>)> + 'a {
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

impl<N> InternalNode<ChildBuffer<N>> {
    pub fn get(&self, key: &[u8]) -> (&RwLock<N>, Option<(KeyInfo, SlicedCowBytes)>) {
        // TODO Merge range messages into msg stream
        let child = &self.children[self.idx(key)];

        let msg = child.get(key).cloned();
        (&child.node_pointer, msg)
    }

    pub fn get_range(
        &self,
        key: &[u8],
        left_pivot_key: &mut Option<CowBytes>,
        right_pivot_key: &mut Option<CowBytes>,
        all_msgs: &mut BTreeMap<CowBytes, Vec<(KeyInfo, SlicedCowBytes)>>,
    ) -> &RwLock<N> {
        // TODO Merge range messages into msg stream
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
    {
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
    {
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

    pub fn drain_children<'a>(&'a mut self) -> impl Iterator<Item = N> + 'a {
        self.entries_size = 0;
        self.children
            .drain(..)
            .map(|child| child.node_pointer.into_inner())
    }
}

impl<N: StaticSize + HasStoragePreference> InternalNode<ChildBuffer<N>> {
    pub fn range_delete(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        dead: &mut Vec<N>,
    ) -> (usize, &mut N, Option<&mut N>) {
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

impl<T: Size> InternalNode<T> {
    pub fn split(&mut self) -> (Self, CowBytes, isize) {
        let split_off_idx = self.fanout() / 2;
        let pivot = self.pivot.split_off(split_off_idx);
        let pivot_key = self.pivot.pop().unwrap();
        let mut children = self.children.split_off(split_off_idx);

        let entries_size = pivot.iter().map(Size::size).sum::<usize>()
            + children.iter_mut().map(SizeMut::size).sum::<usize>();

        let size_delta = entries_size + pivot_key.size();
        self.entries_size -= size_delta;

        // Neither side is sure about its current storage preference after a split
        let right_sibling = InternalNode {
            level: self.level,
            entries_size,
            pivot,
            children,
        };
        (right_sibling, pivot_key, -(size_delta as isize))
    }

    pub fn merge(&mut self, right_sibling: &mut Self, old_pivot_key: CowBytes) -> isize {
        let size_delta = right_sibling.entries_size + old_pivot_key.size();
        self.entries_size += size_delta;
        self.pivot.push(old_pivot_key);
        self.pivot.append(&mut right_sibling.pivot);
        self.children.append(&mut right_sibling.children);

        size_delta as isize
    }
}

impl<N: HasStoragePreference> InternalNode<ChildBuffer<N>>
where
    ChildBuffer<N>: Size,
{
    pub fn try_walk(&mut self, key: &[u8]) -> Option<TakeChildBuffer<ChildBuffer<N>>> {
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

    pub fn try_flush(
        &mut self,
        min_flush_size: usize,
        max_node_size: usize,
        min_fanout: usize,
    ) -> Option<TakeChildBuffer<ChildBuffer<N>>> {
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

pub(super) struct TakeChildBuffer<'a, T: 'a> {
    node: &'a mut InternalNode<T>,
    child_idx: usize,
}

impl<'a, N: StaticSize + HasStoragePreference> TakeChildBuffer<'a, ChildBuffer<N>> {
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

impl<'a, T> TakeChildBuffer<'a, T>
where
    InternalNode<T>: Size,
{
    pub(super) fn size(&self) -> usize {
        Size::size(&*self.node)
    }

    pub(super) fn prepare_merge(&mut self) -> PrepareMergeChild<T> {
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

pub(super) struct PrepareMergeChild<'a, T: 'a> {
    node: &'a mut InternalNode<T>,
    pivot_key_idx: usize,
    other_child_idx: usize,
}

impl<'a, N> PrepareMergeChild<'a, ChildBuffer<N>> {
    pub(super) fn sibling_node_pointer(&mut self) -> &mut RwLock<N> {
        &mut self.node.children[self.other_child_idx].node_pointer
    }
    pub(super) fn is_right_sibling(&self) -> bool {
        self.pivot_key_idx != self.other_child_idx
    }
}
impl<'a, N: Size + HasStoragePreference> PrepareMergeChild<'a, ChildBuffer<N>> {
    pub(super) fn merge_children(self) -> (CowBytes, N, isize) {
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

        (
            pivot_key,
            right_sibling.node_pointer.into_inner(),
            -(size_delta as isize),
        )
    }
}

impl<'a, N: Size + HasStoragePreference> PrepareMergeChild<'a, ChildBuffer<N>> {
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

impl<'a, N: Size + HasStoragePreference> TakeChildBuffer<'a, ChildBuffer<N>> {
    pub fn node_pointer_mut(&mut self) -> &mut RwLock<N> {
        &mut self.node.children[self.child_idx].node_pointer
    }
    pub fn take_buffer(&mut self) -> (BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>, isize) {
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
                children: self.children.iter().cloned().collect(),
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

    #[quickcheck]
    fn check_size_split(mut node: InternalNode<ChildBuffer<()>>) -> TestResult {
        if node.fanout() < 2 {
            return TestResult::discard();
        }
        let size_before = node.size();
        let (mut right_sibling, pivot_key, size_delta) = node.split();
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
        let (mut right_sibling, pivot_key, size_delta) = node.split();

        assert!(node.fanout() >= 2);
        assert!(right_sibling.fanout() >= 2);

        node.entries_size += pivot_key.size() + right_sibling.entries_size;
        node.pivot.push(pivot_key);
        node.pivot.append(&mut right_sibling.pivot);
        node.children.append(&mut right_sibling.children);

        assert_eq!(node, twin);

        TestResult::passed()
    }

    #[test]
    fn check_constant() {
        let node: InternalNode<ChildBuffer<()>> = InternalNode {
            entries_size: 0,
            level: 1,
            children: vec![],
            pivot: vec![],
        };

        assert_eq!(
            serialized_size(&node).unwrap(),
            BINCODE_FIXED_SIZE as u64,
            "magic constants are wrong"
        );
    }

    // TODO tests
    // split
    // child split
    // flush buffer
    // get with max_msn
}
