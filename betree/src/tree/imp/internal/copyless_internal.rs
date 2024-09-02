//! Implementation of the [DisjointInternalNode] node type.
use crate::tree::imp::{
    node::{PivotGetMutResult, PivotGetResult},
    PivotKey,
};

use super::{
    packed_child_buffer::NVMChildBuffer,
    take_child_buffer::{MergeChildResult, TakeChildBufferWrapper},
};

use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::{HasStoragePreference, ObjectReference},
    database::DatasetId,
    size::{Size, StaticSize},
    storage_pool::AtomicSystemStoragePreference,
    tree::{imp::MIN_FANOUT, pivot_key::LocalPivotKey, KeyInfo},
    AtomicStoragePreference, StoragePreference,
};
use parking_lot::RwLock;
use std::{borrow::Borrow, collections::BTreeMap, mem::replace};

use super::serialize_nodepointer;
use serde::{Deserialize, Serialize};

pub(in crate::tree::imp) struct CopylessInternalNode<N> {
    // FIXME: This type can be used as zero-copy
    pub meta_data: InternalNodeMetaData,
    pub children: Vec<ChildLink<N>>,
}

/// A link to the next child, this contains a buffer for messages as well as a
/// pointer to the child.
#[derive(Serialize, Deserialize, Debug)]
#[serde(bound(serialize = "N: Serialize", deserialize = "N: Deserialize<'de>"))]
pub(in crate::tree::imp) struct ChildLink<N> {
    #[serde(skip)]
    buffer: NVMChildBuffer,
    #[serde(with = "serialize_nodepointer")]
    ptr: RwLock<N>,
}

impl<N: PartialEq> PartialEq for ChildLink<N> {
    fn eq(&self, other: &Self) -> bool {
        // TODO: Needs buffer check?
        &*self.ptr.read() == &*other.ptr.read()
    }
}

impl<N> ChildLink<N> {
    pub fn new(buffer: NVMChildBuffer, ptr: N) -> Self {
        ChildLink {
            buffer,
            ptr: RwLock::new(ptr),
        }
    }

    pub fn buffer_mut(&mut self) -> &mut NVMChildBuffer {
        &mut self.buffer
    }

    pub fn buffer(&self) -> &NVMChildBuffer {
        &self.buffer
    }

    pub fn ptr_mut(&mut self) -> &mut RwLock<N> {
        &mut self.ptr
    }

    pub fn ptr(&self) -> &RwLock<N> {
        &self.ptr
    }
}

impl<N> std::fmt::Debug for CopylessInternalNode<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.meta_data.fmt(f)
    }
}

#[derive(Serialize, Deserialize, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[archive(check_bytes)]
#[cfg_attr(test, derive(PartialEq))]
pub(in crate::tree::imp) struct InternalNodeMetaData {
    pub level: u32,
    pub entries_size: usize,
    pub system_storage_preference: AtomicSystemStoragePreference,
    pub pref: AtomicStoragePreference,
    pub(in crate::tree::imp) pivot: Vec<CowBytes>,
    pub entries_sizes: Vec<usize>,
    pub entries_prefs: Vec<StoragePreference>,
    #[serde(skip)]
    pub current_size: Option<usize>,
}

impl InternalNodeMetaData {
    fn invalidate(&mut self) {
        self.pref.invalidate();
        self.current_size = None;
    }
}

const INTERNAL_BINCODE_STATIC: usize = 4 + 8;
impl<N: StaticSize> Size for CopylessInternalNode<N> {
    fn size(&self) -> usize {
        std::mem::size_of::<u32>()
            + self.meta_data.size()
            + std::mem::size_of::<u32>()
            + self.children.len() * N::static_size()
            + 8
            + self.children.iter().map(|c| c.buffer.size()).sum::<usize>()
    }

    fn actual_size(&self) -> Option<usize> {
        // FIXME: Actually cache the serialized size and track delta
        Some(self.size())
    }
}

const META_BINCODE_STATIC: usize = 33;
impl Size for InternalNodeMetaData {
    fn size(&self) -> usize {
        *self.actual_size().get_or_insert_with(|| {
            std::mem::size_of::<u32>()
                + std::mem::size_of::<usize>()
                + std::mem::size_of::<u8>()
                + std::mem::size_of::<u8>()
                + self.pivot.iter().map(|p| p.size()).sum::<usize>()
                + self.pivot.len() * std::mem::size_of::<usize>()
                + self.pivot.len() * std::mem::size_of::<u8>()
                + META_BINCODE_STATIC
        })
    }

    fn actual_size(&self) -> Option<usize> {
        self.current_size
    }
}

impl<N: HasStoragePreference> HasStoragePreference for CopylessInternalNode<N> {
    fn current_preference(&self) -> Option<StoragePreference> {
        self.meta_data
            .pref
            .as_option()
            .map(|pref| self.meta_data.system_storage_preference.weak_bound(&pref))
    }

    fn recalculate(&self) -> StoragePreference {
        let mut pref = StoragePreference::NONE;

        for child in self.meta_data.entries_prefs.iter() {
            pref.upgrade(*child)
        }

        self.meta_data.pref.set(pref);
        pref
    }

    fn correct_preference(&self) -> StoragePreference {
        let storagepref = self.recalculate();
        self.meta_data
            .system_storage_preference
            .weak_bound(&storagepref)
    }

    fn system_storage_preference(&self) -> StoragePreference {
        self.meta_data.system_storage_preference.borrow().into()
    }

    fn set_system_storage_preference(&mut self, pref: StoragePreference) {
        self.meta_data.system_storage_preference.set(pref);
    }
}

pub struct InternalNodeLink<N> {
    pub ptr: N,
    pub buffer: NVMChildBuffer,
    pub buffer_size: usize,
}

impl<N> InternalNodeLink<N> {
    pub fn destruct(self) -> (N, NVMChildBuffer) {
        (self.ptr, self.buffer)
    }
}

impl<N> Into<ChildLink<N>> for InternalNodeLink<N> {
    fn into(self) -> ChildLink<N> {
        ChildLink {
            buffer: self.buffer,
            ptr: RwLock::new(self.ptr),
        }
    }
}

impl<N> CopylessInternalNode<N> {
    pub fn new(
        left_child: InternalNodeLink<N>,
        right_child: InternalNodeLink<N>,
        pivot_key: CowBytes,
        level: u32,
    ) -> Self
    where
        N: StaticSize,
    {
        CopylessInternalNode {
            meta_data: InternalNodeMetaData {
                level,
                entries_size: pivot_key.size(),
                entries_sizes: vec![left_child.buffer_size, right_child.buffer_size],
                pivot: vec![pivot_key],
                system_storage_preference: AtomicSystemStoragePreference::from(
                    StoragePreference::NONE,
                ),
                pref: AtomicStoragePreference::unknown(),
                entries_prefs: vec![StoragePreference::NONE, StoragePreference::NONE],
                current_size: None,
            },
            children: vec![left_child.into(), right_child.into()],
        }
    }

    /// Returns the number of children.
    pub fn fanout(&self) -> usize {
        self.children.len()
    }

    /// Returns the level of this node.
    pub fn level(&self) -> u32 {
        self.meta_data.level
    }

    /// Returns the index of the child buffer
    /// corresponding to the given `key`.
    pub(in crate::tree::imp) fn idx(&self, key: &[u8]) -> usize {
        match self
            .meta_data
            .pivot
            .binary_search_by(|pivot_key| pivot_key.as_ref().cmp(key))
        {
            Ok(idx) | Err(idx) => idx,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &ChildLink<N>>
    where
        N: ObjectReference,
    {
        self.children.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut ChildLink<N>>
    where
        N: ObjectReference,
    {
        self.children.iter_mut()
    }

    pub fn iter_with_bounds(
        &self,
    ) -> impl Iterator<Item = (Option<&CowBytes>, &ChildLink<N>, Option<&CowBytes>)> + '_
    where
        N: ObjectReference,
    {
        self.children.iter().enumerate().map(move |(idx, child)| {
            let maybe_left = if idx == 0 {
                None
            } else {
                self.meta_data.pivot.get(idx - 1)
            };

            let maybe_right = self.meta_data.pivot.get(idx);

            (maybe_left, child, maybe_right)
        })
    }

    /// Serialize the object into a writer.
    ///
    /// Layout
    /// ------
    ///
    /// - LE u32 Metadata len
    /// - InternalNodeMetaData bytes
    /// - [child PTR; LEN]
    /// - [child BUFFER; LEN]
    pub fn pack<W: std::io::Write>(&self, mut w: W) -> Result<(), std::io::Error>
    where
        N: serde::Serialize + StaticSize,
    {
        let bytes_meta_data_len = bincode::serialized_size(&self.meta_data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        w.write_all(&(bytes_meta_data_len as u32).to_le_bytes())?;
        bincode::serialize_into(&mut w, &self.meta_data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let bytes_child_len = bincode::serialized_size(&self.children)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        w.write_all(&(bytes_child_len as u32).to_le_bytes())?;
        bincode::serialize_into(&mut w, &self.children)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        for (size, child) in self.meta_data.entries_sizes.iter().zip(self.children.iter()) {
            assert_eq!(*size, child.buffer.size());
        }

        for child in self.children.iter() {
            child.buffer.pack(&mut w)?;
        }

        Ok(())
    }

    /// Read object from a byte buffer and instantiate it.
    pub fn unpack(buf: CowBytes) -> Result<Self, std::io::Error>
    where
        N: serde::de::DeserializeOwned + StaticSize,
    {
        const NODE_ID: usize = 4;
        let mut cursor = NODE_ID;
        let len = u32::from_le_bytes(buf[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;

        let meta_data: InternalNodeMetaData = bincode::deserialize(&buf[cursor..cursor + len])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        cursor += len;

        let ptrs_len = u32::from_le_bytes(buf[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;

        let mut ptrs: Vec<ChildLink<N>> = bincode::deserialize(&buf[cursor..cursor + ptrs_len])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        cursor += ptrs_len;
        for idx in 0..meta_data.entries_sizes.len() {
            let sub = buf.clone().slice_from(cursor as u32);
            let b = NVMChildBuffer::unpack(sub)?;
            cursor += b.size();
            assert_eq!(meta_data.entries_sizes[idx], b.size());
            let _ = std::mem::replace(&mut ptrs[idx].buffer, b);
            assert_eq!(meta_data.entries_sizes[idx], ptrs[idx].buffer.size());
        }

        Ok(CopylessInternalNode {
            meta_data,
            children: ptrs,
        })
    }

    pub fn after_insert_size_delta(&mut self, idx: usize, size_delta: isize) {
        assert!(size_delta != 0);
        if size_delta > 0 {
            self.meta_data.entries_sizes[idx] += size_delta as usize;
            self.meta_data.entries_size += size_delta as usize;
            debug_assert_eq!(self.children[idx].buffer.size(), self.meta_data.entries_sizes[idx]);
        } else {
            self.meta_data.entries_sizes[idx] -= -size_delta as usize;
            self.meta_data.entries_size -= -size_delta as usize;
            debug_assert_eq!(self.children[idx].buffer.size(), self.meta_data.entries_sizes[idx]);
        }
    }
}

impl<N> CopylessInternalNode<N> {
    pub fn get(&self, key: &[u8]) -> (&RwLock<N>, Option<(KeyInfo, SlicedCowBytes)>)
    where
        N: ObjectReference,
    {
        let child = &self.children[self.idx(key)];
        (&child.ptr, child.buffer.get(key))
    }

    pub fn get_mut(&mut self, key: &[u8]) -> &mut ChildLink<N>
    where
        N: ObjectReference,
    {
        let idx = self.idx(key);
        &mut self.children[idx]
    }

    pub fn pivot_get(&self, pk: &PivotKey) -> PivotGetResult<N>
    where
        N: ObjectReference,
    {
        // Exact pivot matches are required only
        debug_assert!(!pk.is_root());
        let pivot = pk.bytes().unwrap();
        self.meta_data
            .pivot
            .iter()
            .enumerate()
            .find(|(_idx, p)| **p == pivot)
            .map_or_else(
                || {
                    // Continue the search to the next level
                    PivotGetResult::NextNode(&self.children[self.idx(&pivot)].ptr)
                },
                |(idx, _)| {
                    // Fetch the correct child pointer
                    let child;
                    if pk.is_left() {
                        child = &self.children[idx].ptr;
                    } else {
                        child = &self.children[idx + 1].ptr;
                    }
                    PivotGetResult::Target(Some(child))
                },
            )
    }

    pub fn pivot_get_mut(&mut self, pk: &PivotKey) -> PivotGetMutResult<N>
    where
        N: ObjectReference,
    {
        // Exact pivot matches are required only
        debug_assert!(!pk.is_root());
        let pivot = pk.bytes().unwrap();
        let (id, is_target) = self
            .meta_data
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
            (true, true) => PivotGetMutResult::Target(Some(self.children[id].ptr.get_mut())),
            (true, false) => PivotGetMutResult::Target(Some(self.children[id + 1].ptr.get_mut())),
            (false, _) => PivotGetMutResult::NextNode(self.children[id].ptr.get_mut()),
        }
    }

    pub fn apply_with_info(&mut self, key: &[u8], pref: StoragePreference) -> &mut N
    where
        N: ObjectReference,
    {
        let idx = self.idx(key);
        let child = self.children[idx].ptr.get_mut();
        self.meta_data.entries_prefs[idx].upgrade(pref);

        child
    }

    pub fn get_range(
        &self,
        key: &[u8],
        left_pivot_key: &mut Option<CowBytes>,
        right_pivot_key: &mut Option<CowBytes>,
        _all_msgs: &mut BTreeMap<CowBytes, Vec<(KeyInfo, SlicedCowBytes)>>,
    ) -> &ChildLink<N> {
        let idx = self.idx(key);
        if idx > 0 {
            *left_pivot_key = Some(self.meta_data.pivot[idx - 1].clone());
        }
        if idx < self.meta_data.pivot.len() {
            *right_pivot_key = Some(self.meta_data.pivot[idx].clone());
        }
        &self.children[idx]
    }

    pub fn get_next_node(&self, key: &[u8]) -> Option<&ChildLink<N>> {
        let idx = self.idx(key) + 1;
        self.children.get(idx)
    }

    pub fn drain_children(&mut self) -> impl Iterator<Item = ChildLink<N>> + '_
    where
        N: ObjectReference,
    {
        self.meta_data.invalidate();
        self.meta_data.entries_size = 0;
        self.children.drain(..)
    }
}

impl<N: StaticSize> Size for Vec<N> {
    fn size(&self) -> usize {
        8 + self.len() * N::static_size()
    }
}

impl<N: ObjectReference> CopylessInternalNode<N> {
    pub fn split(&mut self) -> (Self, CowBytes, isize, LocalPivotKey) {
        self.meta_data.invalidate();

        // println!("Disjoint node has {} children", self.children.len());

        assert!(self.fanout() >= 2 * MIN_FANOUT);

        let split_off_idx = self.fanout() / 2;
        let pivot = self.meta_data.pivot.split_off(split_off_idx);
        let pivot_key = self.meta_data.pivot.pop().unwrap();

        let children = self.children.split_off(split_off_idx);
        let entries_sizes = self.meta_data.entries_sizes.split_off(split_off_idx);
        let entries_prefs = self.meta_data.entries_prefs.split_off(split_off_idx);

        let entries_size = entries_sizes.len() * std::mem::size_of::<usize>()
            + entries_prefs.len()
            + pivot.iter().map(|p| p.size()).sum::<usize>()
            + children.len() * N::static_size()
            + entries_sizes.iter().sum::<usize>();

        let size_delta = entries_size + pivot_key.size();
        self.meta_data.entries_size -= size_delta;

        let right_sibling = CopylessInternalNode {
            meta_data: InternalNodeMetaData {
                level: self.meta_data.level,
                entries_size,
                entries_sizes,
                entries_prefs,
                pivot,
                // Copy the system storage preference of the other node as we cannot
                // be sure which key was targeted by recorded accesses.
                system_storage_preference: self.meta_data.system_storage_preference.clone(),
                pref: AtomicStoragePreference::unknown(),
                current_size: None,
            },
            children,
        };

        assert!(self.fanout() >= MIN_FANOUT);
        assert!(right_sibling.fanout() >= MIN_FANOUT);
        (
            right_sibling,
            pivot_key.clone(),
            -(size_delta as isize),
            LocalPivotKey::Right(pivot_key),
        )
    }

    pub fn merge(&mut self, right_sibling: &mut Self, old_pivot_key: CowBytes) -> isize {
        self.meta_data.invalidate();
        let size_delta = right_sibling.meta_data.entries_size + old_pivot_key.size();
        self.meta_data.entries_size += size_delta;
        self.meta_data.pivot.push(old_pivot_key);
        self.meta_data
            .pivot
            .append(&mut right_sibling.meta_data.pivot);
        self.meta_data
            .entries_prefs
            .append(&mut right_sibling.meta_data.entries_prefs);
        self.meta_data
            .entries_sizes
            .append(&mut right_sibling.meta_data.entries_sizes);

        self.children.append(&mut right_sibling.children);

        size_delta as isize
    }

    /// Translate any object ref in a `NVMChildBuffer` from `Incomplete` to `Unmodified` state.
    pub fn complete_object_refs(self, d_id: DatasetId) -> Self {
        let first_pk = match self.meta_data.pivot.first() {
            Some(p) => PivotKey::LeftOuter(p.clone(), d_id),
            None => unreachable!(
                "The store contains an empty InternalNode, this should never be the case."
            ),
        };
        for (id, pk) in [first_pk]
            .into_iter()
            .chain(
                self.meta_data
                    .pivot
                    .iter()
                    .map(|p| PivotKey::Right(p.clone(), d_id)),
            )
            .enumerate()
        {
            // SAFETY: There must always be pivots + 1 many children, otherwise
            // the state of the Internal Node is broken.
            self.children[id].ptr.write().set_index(pk.clone());
        }
        self
    }
}

impl<N: HasStoragePreference> CopylessInternalNode<N>
where
    N: StaticSize,
    N: ObjectReference,
{
    pub fn try_walk(&mut self, key: &[u8]) -> NVMTakeChildBuffer<N> {
        let child_idx = self.idx(key);

        NVMTakeChildBuffer {
            node: self,
            child_idx,
        }
    }

    pub fn try_find_flush_candidate(
        &mut self,
        min_flush_size: usize,
        max_node_size: usize,
        min_fanout: usize,
    ) -> Option<TakeChildBufferWrapper<N>>
    where
        N: ObjectReference,
    {
        let child_idx = {
            let (child_idx, child) = self
                .meta_data
                .entries_sizes
                .iter()
                .enumerate()
                .max_by_key(|(_, v)| *v)
                .unwrap();
            assert_eq!(self.children[child_idx].buffer.size(), *child);

            if *child >= min_flush_size
                && ((self.size() - *child) <= max_node_size || self.fanout() < 2 * min_fanout) && self.fanout() < (max_node_size as f32).sqrt() as usize
            {
                Some(child_idx)
            } else {
                None
            }

            // if *child >= min_flush_size
            //     && (size - *child <= max_node_size || self.fanout() < 2 * min_fanout)
            // {
            //     Some(child_idx)
            // } else {
            //     None
            // }
        };
        child_idx.map(move |child_idx| {
            TakeChildBufferWrapper::NVMTakeChildBuffer(NVMTakeChildBuffer {
                node: self,
                child_idx,
            })
        })
    }
}

pub(in crate::tree::imp) struct NVMTakeChildBuffer<'a, N: 'a + 'static> {
    node: &'a mut CopylessInternalNode<N>,
    child_idx: usize,
}

impl<'a, N: StaticSize + HasStoragePreference> NVMTakeChildBuffer<'a, N> {
    pub(in crate::tree::imp) fn split_child(
        &mut self,
        sibling_np: N,
        pivot_key: CowBytes,
        select_right: bool,
    ) -> isize
    where
        N: ObjectReference,
    {
        // split_at invalidates both involved children (old and new), but as the new child
        // is added to self, the overall entries don't change, so this node doesn't need to be
        // invalidated

        let sibling = self.node.children[self.child_idx]
            .buffer
            .split_at(&pivot_key);
        let sibling_size = sibling.size();
        let size_delta = sibling_size + pivot_key.size();
        self.node.children.insert(
            self.child_idx + 1,
            ChildLink {
                buffer: sibling,
                ptr: RwLock::new(sibling_np),
            },
        );
        self.node.meta_data.pivot.insert(self.child_idx, pivot_key);
        self.node.meta_data.entries_sizes[self.child_idx] =
            self.node.children[self.child_idx].buffer.size();
        self.node
            .meta_data
            .entries_sizes
            .insert(self.child_idx + 1, sibling_size);
        self.node.meta_data.entries_prefs.insert(
            self.child_idx + 1,
            self.node.meta_data.entries_prefs[self.child_idx],
        );
        if select_right {
            self.child_idx += 1;
        }
        size_delta as isize
    }

    pub fn take_buffer(&mut self) -> (BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>, isize) {
        let (map, size_delta) = self.node.children[self.child_idx].buffer.take();
        self.node
            .after_insert_size_delta(self.child_idx, -(size_delta as isize));
        (map, -(size_delta as isize))
    }
}

impl<'a, N> NVMTakeChildBuffer<'a, N>
where
    N: StaticSize,
{
    pub(in crate::tree::imp) fn size(&self) -> usize {
        (&*self.node).size()
    }

    pub(in crate::tree::imp) fn prepare_merge(
        &mut self,
    ) -> PrepareMergeChild<N>
    {
        assert!(self.node.fanout() >= 2);
        let (pivot_key_idx, other_child_idx) = if self.child_idx + 1 < self.node.children.len() {
            (self.child_idx, self.child_idx + 1)
        } else {
            (self.child_idx - 1, self.child_idx - 1)
        };

        PrepareMergeChild {
            node: self.node,
            pivot_key_idx,
            other_child_idx,
        }
    }

    // pub(in crate::tree::imp) fn add_size(&mut self, size_delta: isize) {
    //     self.node
    //         .after_insert_size_delta(self.child_idx, size_delta);
    // }
}

pub(in crate::tree::imp) struct PrepareMergeChild<'a, N: 'a + 'static> {
    node: &'a mut CopylessInternalNode<N>,
    pivot_key_idx: usize,
    other_child_idx: usize,
}

impl<'a, N> PrepareMergeChild<'a, N> {
    pub(in crate::tree::imp) fn sibling_node_pointer(&mut self) -> &mut RwLock<N>
    where
        N: ObjectReference,
    {
        &mut self.node.children[self.other_child_idx].ptr
    }
    pub(in crate::tree::imp) fn is_right_sibling(&self) -> bool {
        self.pivot_key_idx != self.other_child_idx
    }
}

impl<'a, N> PrepareMergeChild<'a, N>
where
    N: ObjectReference + HasStoragePreference,
{
    pub(in crate::tree::imp) fn merge_children(self) -> MergeChildResult<Box<dyn Iterator<Item = N>>>
    {
        let mut right_child_links = self.node.children.remove(self.pivot_key_idx + 1);
        let pivot_key = self.node.meta_data.pivot.remove(self.pivot_key_idx);
        self.node
            .meta_data
            .entries_prefs
            .remove(self.pivot_key_idx + 1);
        self.node
            .meta_data
            .entries_sizes
            .remove(self.pivot_key_idx + 1);

        let left_buffer = self.node.children[self.pivot_key_idx].buffer_mut();
        let mut right_buffer = right_child_links.buffer_mut();

        let size_delta = pivot_key.size()
            + N::static_size() * 2
            + std::mem::size_of::<u8>()
            + std::mem::size_of::<usize>();
        self.node.meta_data.entries_size -= size_delta;
        left_buffer.append(&mut right_buffer);
        self.node.meta_data.entries_sizes[self.pivot_key_idx] = left_buffer.size();
        self.node.meta_data.invalidate();

        MergeChildResult {
            pivot_key,
            old_np: Box::new([right_child_links.ptr.into_inner()].into_iter()),
            size_delta: -(size_delta as isize),
        }
    }
}

impl<'a, N> PrepareMergeChild<'a, N>
where
    N: ObjectReference + HasStoragePreference,
{
    pub(in crate::tree::imp) fn rebalanced(&mut self, new_pivot_key: CowBytes) -> isize
    {
        {
            let (left, right) = self.node.children[self.pivot_key_idx..].split_at_mut(1);
            // Move messages around
            let (left_child, right_child) = (&mut left[0].buffer, &mut right[0].buffer);
            left_child.rebalance(right_child, &new_pivot_key);
            self.node.meta_data.entries_sizes[self.pivot_key_idx] = left_child.size();
            self.node.meta_data.entries_sizes[self.pivot_key_idx + 1] = left_child.size();
        }

        let mut size_delta = new_pivot_key.size() as isize;
        let old_pivot_key = replace(
            &mut self.node.meta_data.pivot[self.pivot_key_idx],
            new_pivot_key,
        );
        size_delta -= old_pivot_key.size() as isize;

        size_delta
    }
}

impl<'a, N: Size + HasStoragePreference> NVMTakeChildBuffer<'a, N> {
    pub fn child_pointer_mut(&mut self) -> &mut RwLock<N>
    where
        N: ObjectReference,
    {
        &mut self.node.children[self.child_idx].ptr
    }

    pub fn buffer_mut(&mut self) -> &mut NVMChildBuffer
    where
        N: ObjectReference,
    {
        &mut self.node.children[self.child_idx].buffer
    }

    pub fn buffer(&self) -> &NVMChildBuffer
    where
        N: ObjectReference,
    {
        &self.node.children[self.child_idx].buffer
    }
}

#[cfg(test)]
pub(crate) use tests::Key as TestKey;

#[cfg(test)]
mod tests {

    use std::io::Write;

    use super::*;
    use crate::{arbitrary::GenExt, database::DatasetId, tree::pivot_key};

    use quickcheck::{Arbitrary, Gen, TestResult};
    use rand::Rng;
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

    // Keys are not allowed to be empty. This is usually caught at the tree layer, but these are
    // bypassing that check. There's probably a good way to do this, but we can also just throw
    // away the empty keys until we find one that isn't empty.
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Key(pub CowBytes);
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

    impl<N: Clone> Clone for ChildLink<N> {
        fn clone(&self) -> Self {
            Self {
                buffer: self.buffer.clone(),
                ptr: self.ptr.read().clone().into(),
            }
        }
    }

    impl<T: Clone> Clone for CopylessInternalNode<T> {
        fn clone(&self) -> Self {
            CopylessInternalNode {
                meta_data: InternalNodeMetaData {
                    level: self.meta_data.level,
                    entries_size: self.meta_data.entries_size,
                    pivot: self.meta_data.pivot.clone(),
                    system_storage_preference: self.meta_data.system_storage_preference.clone(),
                    pref: self.meta_data.pref.clone(),
                    entries_prefs: self.meta_data.entries_prefs.clone(),
                    entries_sizes: self.meta_data.entries_sizes.clone(),
                    current_size: None,
                },
                children: self.children.clone(),
            }
        }
    }

    impl<T: Arbitrary + StaticSize> Arbitrary for CopylessInternalNode<T> {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut rng = g.rng();
            let pivot_key_cnt = rng.gen_range(0..10);
            let mut entries_size = 0;

            let mut pivot = Vec::with_capacity(pivot_key_cnt);
            for _ in 0..pivot_key_cnt {
                let pivot_key = {
                    let k = Key::arbitrary(g);
                    k.0
                };
                entries_size += pivot_key.size();
                pivot.push(pivot_key);
            }
            pivot.sort();

            let mut children: Vec<ChildLink<T>> = Vec::with_capacity(pivot_key_cnt + 1);
            for _ in 0..pivot_key_cnt + 1 {
                let buffer = NVMChildBuffer::arbitrary(g);
                entries_size += T::static_size() + buffer.size();
                children.push(ChildLink {
                    buffer,
                    ptr: RwLock::new(T::arbitrary(g)),
                });
            }

            entries_size += 4 + 8 + pivot_key_cnt * 8 + pivot_key_cnt * 1;

            CopylessInternalNode {
                meta_data: InternalNodeMetaData {
                    pivot,
                    entries_size,
                    level: 1,
                    system_storage_preference: AtomicSystemStoragePreference::from(
                        StoragePreference::NONE,
                    ),
                    pref: AtomicStoragePreference::unknown(),
                    entries_prefs: vec![StoragePreference::NONE; pivot_key_cnt + 1],
                    entries_sizes: children.iter().map(|c| c.buffer.size()).collect::<Vec<_>>(),
                    current_size: None,
                },
                children,
            }
        }
    }

    fn serialized_size<T: ObjectReference>(node: &CopylessInternalNode<T>) -> usize {
        let mut buf = Vec::new();
        node.pack(&mut buf).unwrap();
        buf.len()
    }

    fn check_size<T: Size + ObjectReference + std::cmp::PartialEq>(node: &CopylessInternalNode<T>) {
        assert_eq!(node.size(), serialized_size(node))
    }

    #[quickcheck]
    fn actual_size(node: CopylessInternalNode<()>) {
        assert_eq!(node.size(), serialized_size(&node))
    }

    #[quickcheck]
    fn idx(node: CopylessInternalNode<()>, key: Key) {
        let key = key.0;
        let idx = node.idx(&key);

        if let Some(upper_key) = node.meta_data.pivot.get(idx) {
            assert!(&key <= upper_key);
        }
        if idx > 0 {
            let lower_key = &node.meta_data.pivot[idx - 1];
            assert!(lower_key < &key);
        }
    }

    static mut PK: Option<PivotKey> = None;

    #[quickcheck]
    fn size_split(mut node: CopylessInternalNode<()>) -> TestResult {
        if node.fanout() < 4 {
            return TestResult::discard();
        }
        let size_before = node.size();
        let (right_sibling, _, size_delta, _pivot_key) = node.split();
        assert_eq!(size_before as isize + size_delta, node.size() as isize);

        check_size(&node);
        check_size(&right_sibling);

        TestResult::passed()
    }

    #[quickcheck]
    fn split(mut node: CopylessInternalNode<()>) -> TestResult {
        if node.fanout() < 4 {
            return TestResult::discard();
        }
        let twin = node.clone();
        let (mut right_sibling, pivot, _size_delta, _pivot_key) = node.split();

        assert!(*node.meta_data.pivot.last().unwrap() <= pivot);
        assert!(*right_sibling.meta_data.pivot.first().unwrap() > pivot);
        assert!(node.fanout() >= 2);
        assert!(right_sibling.fanout() >= 2);

        assert!(node.children.len() == node.meta_data.pivot.len() + 1);
        assert!(right_sibling.children.len() == right_sibling.meta_data.pivot.len() + 1);
        assert!((node.children.len() as isize - right_sibling.children.len() as isize).abs() <= 1);

        let size_before = node.size();
        let size_delta = node.merge(&mut right_sibling, pivot);
        let size_after = node.size();
        assert_eq!(size_before as isize + size_delta, size_after as isize);
        assert_eq!(node.size(), twin.size());

        TestResult::passed()
    }

    #[quickcheck]
    fn split_key(mut node: CopylessInternalNode<()>) -> TestResult {
        if node.fanout() < 4 {
            return TestResult::discard();
        }
        let (right_sibling, pivot, _size_delta, pivot_key) = node.split();
        assert!(node.fanout() >= 2);
        assert!(right_sibling.fanout() >= 2);
        assert_eq!(LocalPivotKey::Right(pivot), pivot_key);
        TestResult::passed()
    }

    #[quickcheck]
    fn split_and_merge(mut node: CopylessInternalNode<()>) -> TestResult {
        if node.fanout() < 4 {
            return TestResult::discard();
        }

        let twin = node.clone();
        let (mut right_node, pivot, ..) = node.split();
        node.merge(&mut right_node, pivot);
        assert_eq!(node.meta_data, twin.meta_data);
        assert_eq!(node.children, twin.children);
        TestResult::passed()
    }

    #[quickcheck]
    fn serialize_then_deserialize(node: CopylessInternalNode<()>) {
        let mut buf = Vec::new();
        buf.write_all(&[0; 4]).unwrap();
        node.pack(&mut buf).unwrap();
        let unpacked = CopylessInternalNode::<()>::unpack(buf.into()).unwrap();
        assert_eq!(unpacked.meta_data, node.meta_data);
        assert_eq!(unpacked.children, node.children);
    }

    // TODO tests
    // flush buffer
    // get with max_msn
}
