//! Implementation of the [NVMInternalNode] node type.
use super::{
    node::{PivotGetMutResult, PivotGetResult},
    nvm_child_buffer::NVMChildBuffer,
    take_child_buffer::{MergeChildResult, TakeChildBufferWrapper},
    Node, PivotKey,
};
use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::{Dml, HasStoragePreference, ObjectReference},
    database::DatasetId,
    size::{Size, SizeMut, StaticSize},
    storage_pool::AtomicSystemStoragePreference,
    tree::{pivot_key::LocalPivotKey, KeyInfo},
    AtomicStoragePreference, StoragePreference,
};
use owning_ref::OwningRefMut;
use parking_lot::RwLock;
use std::{borrow::Borrow, collections::BTreeMap, mem::replace, ops::Deref};

use serde::{Deserialize, Serialize};

pub(super) struct NVMInternalNode<N> {
    // FIXME: This type can be used as zero-copy
    pub meta_data: InternalNodeMetaData,
    // We need this type everytime in memory. Requires modifications during runtime each time.
    pub children: Vec<ChildLink<N>>,
}

use super::serialize_nodepointer;

/// A link to the next child, this contains a buffer for messages as well as a
/// pointer to the child.
#[derive(Deserialize, Serialize, Debug)]
#[serde(bound(serialize = "N: Serialize", deserialize = "N: Deserialize<'de>"))]
pub(super) struct ChildLink<N> {
    #[serde(with = "serialize_nodepointer")]
    buffer: RwLock<N>,
    #[serde(with = "serialize_nodepointer")]
    ptr: RwLock<N>,
}

impl<N: PartialEq> PartialEq for ChildLink<N> {
    fn eq(&self, other: &Self) -> bool {
        &*self.buffer.read() == &*other.buffer.read() && &*self.ptr.read() == &*other.ptr.read()
    }
}

impl<N> ChildLink<N> {
    pub fn buffer_mut(&mut self) -> &mut RwLock<N> {
        &mut self.buffer
    }

    pub fn buffer(&self) -> &RwLock<N> {
        &self.buffer
    }

    pub fn ptr_mut(&mut self) -> &mut RwLock<N> {
        &mut self.ptr
    }

    pub fn ptr(&self) -> &RwLock<N> {
        &self.ptr
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut RwLock<N>> {
        [&mut self.buffer, &mut self.ptr].into_iter()
    }
}

impl<N> std::fmt::Debug for NVMInternalNode<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.meta_data.fmt(f)
    }
}

#[derive(Serialize, Deserialize, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[archive(check_bytes)]
#[cfg_attr(test, derive(PartialEq))]
pub(super) struct InternalNodeMetaData {
    pub level: u32,
    pub entries_size: usize,
    pub system_storage_preference: AtomicSystemStoragePreference,
    pub pref: AtomicStoragePreference,
    pub(super) pivot: Vec<CowBytes>,
    pub entries_sizes: Vec<usize>,
    pub entries_prefs: Vec<StoragePreference>,
}

const INTERNAL_BINCODE_STATIC: usize = 4 + 8;
impl<N: StaticSize> Size for NVMInternalNode<N> {
    fn size(&self) -> usize {
        self.meta_data.size() + self.children.len() * N::static_size() * 2 + INTERNAL_BINCODE_STATIC
    }

    fn actual_size(&self) -> Option<usize> {
        // FIXME: Actually cache the serialized size and track delta
        Some(self.size())
    }
}

// NOTE: This has become necessary as the decision when to flush a node is no
// longer dependent on just this object but it's subobjects too.
impl<N: StaticSize> NVMInternalNode<N> {
    pub fn logical_size(&self) -> usize {
        self.size() + self.meta_data.entries_sizes.iter().sum::<usize>()
    }
}

const META_BINCODE_STATIC: usize = 33;
impl Size for InternalNodeMetaData {
    fn size(&self) -> usize {
        std::mem::size_of::<u32>()
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<u8>()
            + std::mem::size_of::<u8>()
            + self.pivot.iter().map(|p| p.size()).sum::<usize>()
            + self.pivot.len() * std::mem::size_of::<usize>()
            + self.pivot.len() * std::mem::size_of::<u8>()
            + META_BINCODE_STATIC
    }
}

impl<N: HasStoragePreference> HasStoragePreference for NVMInternalNode<N> {
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
    pub buffer_ptr: N,
    pub buffer_size: usize,
}

impl<N> InternalNodeLink<N> {
    pub fn destruct(self) -> (N, N) {
        (self.ptr, self.buffer_ptr)
    }
}

impl<N> Into<ChildLink<N>> for InternalNodeLink<N> {
    fn into(self) -> ChildLink<N> {
        ChildLink {
            buffer: RwLock::new(self.buffer_ptr),
            ptr: RwLock::new(self.ptr),
        }
    }
}

impl<N> NVMInternalNode<N> {
    pub fn new(
        left_child: InternalNodeLink<N>,
        right_child: InternalNodeLink<N>,
        pivot_key: CowBytes,
        level: u32,
    ) -> Self
    where
        N: StaticSize,
    {
        NVMInternalNode {
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
            },
            children: vec![left_child.into(), right_child.into()],
        }
    }

    /// Returns the number of children.
    pub fn fanout(&self) -> usize
    where
        N: ObjectReference,
    {
        self.children.len()
    }

    /// Returns the level of this node.
    pub fn level(&self) -> u32 {
        self.meta_data.level
    }

    /// Returns the index of the child buffer
    /// corresponding to the given `key`.
    pub(super) fn idx(&self, key: &[u8]) -> usize {
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
    /// len_meta META len_c [C_PTR CBUF_PTR]
    pub fn pack<W: std::io::Write>(&self, mut w: W) -> Result<(), std::io::Error>
    where
        N: serde::Serialize,
    {
        // FIXME: Avoid additional allocation
        // let mut serializer_meta_data = rkyv::ser::serializers::AllocSerializer::<0>::default();
        // serializer_meta_data
        //     .serialize_value(&self.meta_data)
        //     .unwrap();
        // let bytes_meta_data = serializer_meta_data.into_serializer().into_inner();
        let bytes_meta_data = bincode::serialize(&self.meta_data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        w.write_all(&(bytes_meta_data.len() as u32).to_le_bytes())?;
        w.write_all(&bytes_meta_data.as_ref())?;
        bincode::serialize_into(&mut w, &self.children)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(())
    }

    /// Read object from a byte buffer and instantiate it.
    pub fn unpack<'a>(buf: &'a [u8]) -> Result<Self, std::io::Error>
    where
        N: serde::Deserialize<'a> + StaticSize,
    {
        let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
        // FIXME: useless copy in some cases, this can be replaced
        // let archivedinternalnodemetadata: &ArchivedInternalNodeMetaData =
        //     unsafe { rkyv::archived_root::<InternalNodeMetaData>(&buf[4..4 + len]) };
        // let meta_data: InternalNodeMetaData = {
        //     use rkyv::Deserialize;
        //     archivedinternalnodemetadata
        //         .deserialize(&mut rkyv::Infallible)
        //         .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
        // };
        let meta_data = bincode::deserialize(&buf[4..4 + len])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let children = bincode::deserialize(&buf[4 + len..])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        Ok(NVMInternalNode {
            meta_data,
            children,
        })
    }

    pub fn after_insert_size_delta(&mut self, idx: usize, size_delta: isize) {
        if size_delta > 0 {
            self.meta_data.entries_sizes[idx] += size_delta as usize;
            self.meta_data.entries_size += size_delta as usize;
        } else {
            self.meta_data.entries_sizes[idx] -= -size_delta as usize;
            self.meta_data.entries_size -= -size_delta as usize;
        }
    }
}

impl<N> NVMInternalNode<N> {
    pub fn get(&self, key: &[u8]) -> &ChildLink<N>
    where
        N: ObjectReference,
    {
        &self.children[self.idx(key)]
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
        unimplemented!("Apply info to messages in buffer");
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
    ) -> &RwLock<N> {
        let idx = self.idx(key);
        if idx > 0 {
            *left_pivot_key = Some(self.meta_data.pivot[idx - 1].clone());
        }
        if idx < self.meta_data.pivot.len() {
            *right_pivot_key = Some(self.meta_data.pivot[idx].clone());
        }
        &self.children[idx].ptr
    }

    pub fn get_next_node(&self, key: &[u8]) -> Option<&RwLock<N>> {
        let idx = self.idx(key) + 1;
        self.children.get(idx).map(|l| &l.ptr)
    }

    pub fn drain_children(&mut self) -> impl Iterator<Item = ChildLink<N>> + '_
    where
        N: ObjectReference,
    {
        self.meta_data.pref.invalidate();
        self.meta_data.entries_size = 0;
        self.children.drain(..)
    }
}

impl<N: StaticSize> Size for Vec<N> {
    fn size(&self) -> usize {
        8 + self.len() * N::static_size()
    }
}

impl<N: ObjectReference> NVMInternalNode<N> {
    pub fn split(&mut self) -> (Self, CowBytes, isize, LocalPivotKey) {
        self.meta_data.pref.invalidate();
        let split_off_idx = self.fanout() / 2;
        let pivot = self.meta_data.pivot.split_off(split_off_idx);
        let pivot_key = self.meta_data.pivot.pop().unwrap();

        let children = self.children.split_off(split_off_idx);
        let entries_sizes = self.meta_data.entries_sizes.split_off(split_off_idx);
        let entries_prefs = self.meta_data.entries_prefs.split_off(split_off_idx);

        // FIXME: Necessary to update, how to propagate?
        // if let (Some(new_left_outer), Some(new_left_pivot)) = (children.first_mut(), pivot.first())
        // {
        //     new_left_outer
        //         .as_mut()
        //         .unwrap()
        //         .update_pivot_key(LocalPivotKey::LeftOuter(new_left_pivot.clone()))
        // }

        let entries_size = entries_sizes.len() * std::mem::size_of::<usize>()
            + entries_prefs.len()
            + pivot.iter().map(|p| p.size()).sum::<usize>()
            + children.len() * 2 * N::static_size();

        let size_delta = entries_size + pivot_key.size();
        self.meta_data.entries_size -= size_delta;

        let right_sibling = NVMInternalNode {
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
            },
            children,
        };
        (
            right_sibling,
            pivot_key.clone(),
            -(size_delta as isize),
            LocalPivotKey::Right(pivot_key),
        )
    }

    pub fn merge(&mut self, right_sibling: &mut Self, old_pivot_key: CowBytes) -> isize {
        self.meta_data.pref.invalidate();
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
    pub fn complete_object_refs(mut self, d_id: DatasetId) -> Self {
        let first_pk = match self.meta_data.pivot.first() {
            Some(p) => PivotKey::LeftOuter(p.clone(), d_id),
            None => unreachable!(
                "The store contains an empty NVMInternalNode, this should never be the case."
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
            self.children[id].buffer.write().set_index(pk);
        }
        self
    }
}

impl<N: HasStoragePreference> NVMInternalNode<N>
where
    N: StaticSize,
    N: ObjectReference,
{
    pub fn try_walk_incomplete(&mut self, key: &[u8]) -> NVMTakeChildBuffer<N> {
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
            let size = self.size();
            let (child_idx, child) = self
                .meta_data
                .entries_sizes
                .iter()
                .enumerate()
                .max()
                .unwrap();
            debug!("Largest child's buffer size: {}", child);

            if *child >= min_flush_size
                && (size - *child <= max_node_size || self.fanout() < 2 * min_fanout)
            {
                Some(child_idx)
            } else {
                None
            }
        };
        child_idx.map(move |child_idx| {
            TakeChildBufferWrapper::NVMTakeChildBuffer(NVMTakeChildBuffer {
                node: self,
                child_idx,
            })
        })
    }
}

pub(super) struct NVMTakeChildBuffer<'a, N: 'a + 'static> {
    node: &'a mut NVMInternalNode<N>,
    child_idx: usize,
}

impl<'a, N: StaticSize + HasStoragePreference> NVMTakeChildBuffer<'a, N> {
    pub(super) fn split_child<F, G, X>(
        &mut self,
        sibling_np: N,
        pivot_key: CowBytes,
        select_right: bool,
        load: F,
        allocate: G,
    ) -> isize
    where
        N: ObjectReference,
        X: Dml,
        F: Fn(&mut RwLock<N>) -> OwningRefMut<X::CacheValueRefMut, NVMChildBuffer>,
        G: Fn(NVMChildBuffer) -> N,
    {
        // split_at invalidates both involved children (old and new), but as the new child
        // is added to self, the overall entries don't change, so this node doesn't need to be
        // invalidated

        let sibling = load(&mut self.node.children[self.child_idx].buffer).split_at(&pivot_key);
        let sibling_size = sibling.size();
        let size_delta = sibling_size + pivot_key.size();
        self.node.children.insert(
            self.child_idx + 1,
            ChildLink {
                buffer: RwLock::new(allocate(sibling)),
                ptr: RwLock::new(sibling_np),
            },
        );
        self.node.meta_data.pivot.insert(self.child_idx, pivot_key);
        self.node.meta_data.entries_sizes[self.child_idx] -=
            sibling_size - super::nvm_child_buffer::BUFFER_BINCODE_STATIC;
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
}

impl<'a, N> NVMTakeChildBuffer<'a, N>
where
    N: StaticSize,
{
    pub(super) fn size(&self) -> usize {
        Size::size(&*self.node)
    }

    pub(super) fn load_and_prepare_merge<X>(
        &mut self,
        dml: &X,
        d_id: DatasetId,
    ) -> PrepareMergeChild<N, X>
    where
        X: Dml<Object = Node<N>, ObjectRef = N>,
    {
        let (pivot_key_idx, other_child_idx) = if self.child_idx + 1 < self.node.children.len() {
            (self.child_idx, self.child_idx + 1)
        } else {
            (self.child_idx - 1, self.child_idx - 1)
        };

        let pivot_child = dml
            .get_mut(
                self.node.children[pivot_key_idx].buffer_mut().get_mut(),
                d_id,
            )
            .expect("error in prepare merge nvm");
        let other_child = dml
            .get_mut(
                self.node.children[other_child_idx].buffer_mut().get_mut(),
                d_id,
            )
            .expect("error in prepare merge nvm");

        PrepareMergeChild {
            node: self.node,
            left_child: pivot_child,
            right_child: other_child,
            pivot_key_idx,
            other_child_idx,
            d_id,
        }
    }
}

pub(super) struct PrepareMergeChild<'a, N: 'a + 'static, X>
where
    X: Dml,
{
    node: &'a mut NVMInternalNode<N>,
    left_child: X::CacheValueRefMut,
    right_child: X::CacheValueRefMut,
    pivot_key_idx: usize,
    other_child_idx: usize,
    d_id: DatasetId,
}

impl<'a, N, X: Dml> PrepareMergeChild<'a, N, X> {
    pub(super) fn sibling_node_pointer(&mut self) -> &mut RwLock<N>
    where
        N: ObjectReference,
    {
        &mut self.node.children[self.other_child_idx].ptr
    }
    pub(super) fn is_right_sibling(&self) -> bool {
        self.pivot_key_idx != self.other_child_idx
    }
}

impl<'a, N, X> PrepareMergeChild<'a, N, X>
where
    X: Dml<Object = Node<N>, ObjectRef = N>,
    N: ObjectReference<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
{
    pub(super) fn merge_children(mut self, dml: &X) -> MergeChildResult<N>
    where
        N: ObjectReference,
    {
        // FIXME: Shouldn't this be other_idx instead of + 1

        let links = self.node.children.remove(self.pivot_key_idx + 1);

        let pivot_key = self.node.meta_data.pivot.remove(self.pivot_key_idx);
        // FIXME: size calculation
        let size_delta = pivot_key.size();
        self.node.meta_data.entries_size -= size_delta;

        self.left_child
            .assert_buffer_mut()
            .append(&mut self.right_child.assert_buffer_mut());
        self.left_child
            .assert_buffer()
            .messages_preference
            .upgrade_atomic(&self.right_child.assert_buffer().messages_preference);

        MergeChildResult {
            pivot_key,
            old_np: links.ptr.into_inner(),
            size_delta: -(size_delta as isize),
        }
    }
}

impl<'a, N, X> PrepareMergeChild<'a, N, X>
where
    X: Dml<Object = Node<N>, ObjectRef = N>,
    N: ObjectReference<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
{
    pub(super) fn rebalanced<F>(&mut self, new_pivot_key: CowBytes, load: F) -> isize
    where
        N: ObjectReference,
        F: Fn(&mut RwLock<N>, DatasetId) -> X::CacheValueRefMut,
    {
        {
            let (left, right) = self.node.children[self.pivot_key_idx..].split_at_mut(1);
            // Move messages around
            let (mut left_child, mut right_child) = (
                load(&mut left[0].buffer, self.d_id),
                load(&mut right[0].buffer, self.d_id),
            );
            left_child
                .assert_buffer_mut()
                .rebalance(right_child.assert_buffer_mut(), &new_pivot_key);
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

    pub fn buffer_pointer_mut(&mut self) -> &mut RwLock<N>
    where
        N: ObjectReference,
    {
        &mut self.node.children[self.child_idx].buffer
    }

    pub fn buffer_pointer(&self) -> &RwLock<N>
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
                buffer: self.buffer.read().clone().into(),
                ptr: self.ptr.read().clone().into(),
            }
        }
    }

    impl<T: Clone> Clone for NVMInternalNode<T> {
        fn clone(&self) -> Self {
            NVMInternalNode {
                meta_data: InternalNodeMetaData {
                    level: self.meta_data.level,
                    entries_size: self.meta_data.entries_size,
                    pivot: self.meta_data.pivot.clone(),
                    system_storage_preference: self.meta_data.system_storage_preference.clone(),
                    pref: self.meta_data.pref.clone(),
                    entries_prefs: self.meta_data.entries_prefs.clone(),
                    entries_sizes: self.meta_data.entries_sizes.clone(),
                },
                children: self.children.clone(),
            }
        }
    }

    impl<T: Arbitrary + StaticSize> Arbitrary for NVMInternalNode<T> {
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
                entries_size += T::static_size() * 2;
                children.push(ChildLink {
                    buffer: RwLock::new(T::arbitrary(g)),
                    ptr: RwLock::new(T::arbitrary(g)),
                });
            }

            entries_size += 4 + 8 + pivot_key_cnt * 8 + pivot_key_cnt * 1;

            NVMInternalNode {
                meta_data: InternalNodeMetaData {
                    pivot,
                    entries_size,
                    level: 1,
                    system_storage_preference: AtomicSystemStoragePreference::from(
                        StoragePreference::NONE,
                    ),
                    pref: AtomicStoragePreference::unknown(),
                    entries_prefs: vec![StoragePreference::NONE; pivot_key_cnt + 1],
                    entries_sizes: children.iter().map(|c| 42).collect::<Vec<_>>(),
                },
                children,
            }
        }
    }

    fn serialized_size<T: ObjectReference>(node: &NVMInternalNode<T>) -> usize {
        let mut buf = Vec::new();
        node.pack(&mut buf).unwrap();
        buf.len()
    }

    fn check_size<T: Size + ObjectReference + std::cmp::PartialEq>(node: &NVMInternalNode<T>) {
        assert_eq!(node.size(), serialized_size(node))
    }

    #[quickcheck]
    fn actual_size(node: NVMInternalNode<()>) {
        assert_eq!(node.size(), serialized_size(&node))
    }

    #[quickcheck]
    fn idx(node: NVMInternalNode<()>, key: Key) {
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
    fn size_split(mut node: NVMInternalNode<()>) -> TestResult {
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
    fn split(mut node: NVMInternalNode<()>) -> TestResult {
        if node.fanout() < 4 {
            return TestResult::discard();
        }
        let twin = node.clone();
        let (right_sibling, pivot, _size_delta, _pivot_key) = node.split();

        assert!(*node.meta_data.pivot.last().unwrap() <= pivot);
        assert!(*right_sibling.meta_data.pivot.first().unwrap() > pivot);
        assert!(node.fanout() >= 2);
        assert!(right_sibling.fanout() >= 2);

        assert!(node.children.len() == node.meta_data.pivot.len() + 1);
        assert!(right_sibling.children.len() == right_sibling.meta_data.pivot.len() + 1);
        assert!((node.children.len() as isize - right_sibling.children.len() as isize).abs() <= 1);

        TestResult::passed()
    }

    #[quickcheck]
    fn split_key(mut node: NVMInternalNode<()>) -> TestResult {
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
    fn split_and_merge(mut node: NVMInternalNode<()>) -> TestResult {
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
    fn serialize_then_deserialize(node: NVMInternalNode<()>) {
        let mut buf = Vec::new();
        node.pack(&mut buf).unwrap();
        let unpacked = NVMInternalNode::<()>::unpack(&buf).unwrap();
        assert_eq!(unpacked.meta_data, node.meta_data);
        assert_eq!(unpacked.children, node.children);
    }

    // TODO tests
    // flush buffer
    // get with max_msn
}
