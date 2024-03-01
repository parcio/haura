//! Implementation of the [NVMInternalNode] node type.
use super::{
    node::{PivotGetMutResult, PivotGetResult, TakeChildBufferWrapper},
    nvm_child_buffer::NVMChildBuffer,
    PivotKey,
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
use std::{borrow::Borrow, collections::BTreeMap, mem::replace};

use rkyv::ser::Serializer;
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
#[derive(Deserialize, Serialize)]
#[serde(bound(serialize = "N: Serialize", deserialize = "N: Deserialize<'de>"))]
pub(super) struct ChildLink<N> {
    #[serde(with = "serialize_nodepointer")]
    buffer: RwLock<N>,
    #[serde(with = "serialize_nodepointer")]
    ptr: RwLock<N>,
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
}

impl<N> std::fmt::Debug for NVMInternalNode<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TODO: Karim.. fix this...")
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

impl<N: StaticSize> Size for NVMInternalNode<N> {
    fn size(&self) -> usize {
        self.meta_data.entries_size
    }

    fn actual_size(&self) -> Option<usize> {
        // FIXME: If not implementing ChildBuffers as separate buffer object, add their size calculation here
        Some(self.meta_data.pivot.iter().map(Size::size).sum::<usize>())
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
    fn idx(&self, key: &[u8]) -> usize {
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
        //assert!(!self.nvm_load_details.read().unwrap().need_to_load_data_from_nvm, "Some data for the NVMInternal node still has to be loaded into the cache.");
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
        let mut serializer_meta_data = rkyv::ser::serializers::AllocSerializer::<0>::default();
        serializer_meta_data
            .serialize_value(&self.meta_data)
            .unwrap();
        let bytes_meta_data = serializer_meta_data.into_serializer().into_inner();
        w.write_all(&(bytes_meta_data.len() as u32).to_le_bytes());
        w.write_all(&bytes_meta_data.as_ref())?;
        bincode::serialize_into(&mut w, &self.children)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // w.write_all(&(self.children.len() as u32).to_le_bytes())?;
        // let mut s = bincode::Serializer::new(&mut w, bincode::config::DefaultOptions::new());
        // for (ptr, buf_ptr) in self.child_ptrs.iter().zip(self.cbuf_ptrs.iter()) {
        //     ptr.read().serialize(&mut s).unwrap();
        //     buf_ptr.read().serialize(&mut s).unwrap();
        // }
        Ok(())
    }

    pub fn unpack<'a>(buf: &'a [u8]) -> Result<Self, std::io::Error>
    where
        N: serde::Deserialize<'a> + StaticSize,
    {
        let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
        // FIXME: useless copy in some cases, this can be replaced
        let archivedinternalnodemetadata: &ArchivedInternalNodeMetaData =
            unsafe { rkyv::archived_root::<InternalNodeMetaData>(&buf[4..4 + len]) };
        let meta_data: InternalNodeMetaData = {
            use rkyv::Deserialize;
            archivedinternalnodemetadata
                .deserialize(&mut rkyv::Infallible)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
        };

        let children = bincode::deserialize(&buf[4 + len..])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        Ok(NVMInternalNode {
            meta_data,
            children,
        })
    }
}

impl<N> NVMInternalNode<N> {
    pub fn get(&self, key: &[u8]) -> &ChildLink<N>
    where
        N: ObjectReference,
    {
        &self.children[self.idx(key)]
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
        all_msgs: &mut BTreeMap<CowBytes, Vec<(KeyInfo, SlicedCowBytes)>>,
    ) -> &RwLock<N> {
        let idx = self.idx(key);
        if idx > 0 {
            *left_pivot_key = Some(self.meta_data.pivot[idx - 1].clone());
        }
        if idx < self.meta_data.pivot.len() {
            *right_pivot_key = Some(self.meta_data.pivot[idx].clone());
        }
        &self.children[idx].ptr
        // for (key, msg) in child.get_all_messages() {
        //     all_msgs
        //         .entry(key.clone())
        //         .or_insert_with(Vec::new)
        //         .push(msg.clone());
        // }
    }

    pub fn get_next_node(&self, key: &[u8]) -> Option<&RwLock<N>> {
        let idx = self.idx(key) + 1;
        self.children.get(idx).map(|l| &l.ptr)
    }

    // FIXME: Since the Partitioned Node does not really handle request we might
    // want to consider taking another route for insertions in their buffers.
    //
    // For now we perform an add size after the buffer delta was given back to us in the node code :/
    //
    //  pub fn insert<Q, M>(
    //      &mut self,
    //      key: Q,
    //      keyinfo: KeyInfo,
    //      msg: SlicedCowBytes,
    //      msg_action: M,
    //  ) -> isize
    //  where
    //      Q: Borrow<[u8]> + Into<CowBytes>,
    //      M: MessageAction,
    //      N: ObjectReference,
    //  {
    //      self.meta_data.pref.invalidate();
    //      let idx = self.idx(key.borrow());

    //      let added_size = self
    //          .data
    //          .write()
    //          .as_mut()
    //          .unwrap()
    //          .as_mut()
    //          .unwrap()
    //          .children[idx]
    //          .as_mut()
    //          .unwrap()
    //          .insert(key, keyinfo, msg, msg_action);

    //      if added_size > 0 {
    //          self.meta_data.entries_size += added_size as usize;
    //      } else {
    //          self.meta_data.entries_size -= -added_size as usize;
    //      }
    //      added_size
    //  }

    // pub fn insert_msg_buffer<I, M>(&mut self, iter: I, msg_action: M) -> isize
    // where
    //     I: IntoIterator<Item = (CowBytes, (KeyInfo, SlicedCowBytes))>,
    //     M: MessageAction,
    //     N: ObjectReference,
    // {
    //     self.meta_data.pref.invalidate();
    //     let mut added_size = 0;

    //     for (k, (keyinfo, v)) in iter.into_iter() {
    //         let idx = self.idx(&k);
    //         added_size += self
    //             .data
    //             .write()
    //             .as_mut()
    //             .unwrap()
    //             .as_mut()
    //             .unwrap()
    //             .children[idx]
    //             .as_mut()
    //             .unwrap()
    //             .insert(k, keyinfo, v, &msg_action);
    //     }

    //     if added_size > 0 {
    //         self.meta_data.entries_size += added_size as usize;
    //     } else {
    //         self.meta_data.entries_size -= -added_size as usize;
    //     }
    //     added_size
    // }

    pub fn drain_children(&mut self) -> impl Iterator<Item = ChildLink<N>> + '_
    where
        N: ObjectReference,
    {
        self.meta_data.pref.invalidate();
        self.meta_data.entries_size = 0;
        self.children.drain(..)
    }
}

impl<N: ObjectReference> NVMInternalNode<N> {
    pub fn split(&mut self) -> (Self, CowBytes, isize, LocalPivotKey) {
        self.meta_data.pref.invalidate();
        let split_off_idx = self.fanout() / 2;
        let pivot = self.meta_data.pivot.split_off(split_off_idx);
        let pivot_key = self.meta_data.pivot.pop().unwrap();

        let mut children = self.children.split_off(split_off_idx);
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

        let entries_size =
            pivot.iter().map(Size::size).sum::<usize>() + 2 * children.len() * N::static_size();

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
    pub fn try_walk(&mut self, key: &[u8]) -> Option<NVMTakeChildBuffer<N>> {
        unimplemented!("Trying to walk, returning take child buffer required, empty check needs to be delayed to the caller.")
        // let child_idx = self.idx(key);

        // if self.cbuf_ptrs[child_idx].as_mut().unwrap().is_empty(key) {
        //     Some(NVMTakeChildBuffer {})
        // } else {
        //     None
        // }
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
            let fanout = self.fanout();

            let mut child_idx;
            let ref child: _;

            (child_idx, child) = self
                .meta_data
                .entries_sizes
                .iter()
                .enumerate()
                .max()
                .unwrap();

            debug!("Largest child's buffer size: {}", child);

            if *child >= min_flush_size
                && (size - *child <= max_node_size || fanout < 2 * min_fanout)
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
        let size_delta = sibling.size() + pivot_key.size();
        self.node.children.insert(
            self.child_idx + 1,
            ChildLink {
                buffer: RwLock::new(allocate(sibling)),
                ptr: RwLock::new(sibling_np),
            },
        );
        self.node.meta_data.pivot.insert(self.child_idx, pivot_key);
        self.node.meta_data.entries_size += size_delta;
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

    pub(super) fn load_and_prepare_merge<F>(&mut self, f: F) -> PrepareMergeChild<N>
    where
        F: Fn(&mut RwLock<N>) -> &mut super::Node<N>,
    {
        let (pivot_key_idx, other_child_idx) = if self.child_idx + 1 < self.node.children.len() {
            (self.child_idx, self.child_idx + 1)
        } else {
            (self.child_idx - 1, self.child_idx - 1)
        };

        unimplemented!()

        // let pivot_child: &'static mut NVMChildBuffer = unsafe { std::mem::transmute(f(&mut self.node.children[pivot_key_idx].buffer).assert_buffer()) };
        // let other_child = f(&mut self.node.children[other_child_idx].buffer).assert_buffer();

        // PrepareMergeChild {
        //     node: self.node,
        //     left_child: pivot_child,
        //     right_child: other_child,
        //     pivot_key_idx,
        //     other_child_idx,
        // }
    }
}

pub(super) struct PrepareMergeChild<'a, N: 'a + 'static> {
    node: &'a mut NVMInternalNode<N>,
    left_child: &'a mut NVMChildBuffer,
    right_child: &'a mut NVMChildBuffer,
    pivot_key_idx: usize,
    other_child_idx: usize,
}

impl<'a, N> PrepareMergeChild<'a, N> {
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

pub(super) struct MergeChildResult<NP> {
    pub(super) pivot_key: CowBytes,
    pub(super) old_np: NP,
    pub(super) size_delta: isize,
}

impl<'a, N: Size + HasStoragePreference> PrepareMergeChild<'a, N> {
    pub(super) fn merge_children<X>(mut self, dml: X) -> MergeChildResult<N>
    where
        N: ObjectReference,
        X: Dml,
    {
        // FIXME: Shouldn't this be other_idx instead of + 1

        let links = self.node.children.remove(self.pivot_key_idx + 1);

        let pivot_key = self.node.meta_data.pivot.remove(self.pivot_key_idx);
        // FIXME: size calculation
        let size_delta = pivot_key.size();
        self.node.meta_data.entries_size -= size_delta;

        self.left_child.append(&mut self.right_child);
        self.left_child
            .messages_preference
            .upgrade_atomic(&self.right_child.messages_preference);

        MergeChildResult {
            pivot_key,
            old_np: links.ptr.into_inner(),
            size_delta: -(size_delta as isize),
        }
    }
}

impl<'a, N: Size + HasStoragePreference> PrepareMergeChild<'a, N> {
    pub(super) fn rebalanced<F>(&mut self, new_pivot_key: CowBytes, load: F) -> isize
    where
        N: ObjectReference,
        F: Fn(&mut RwLock<N>) -> &mut super::Node<N>,
    {
        {
            let (left, right) = self.node.children[self.pivot_key_idx..].split_at_mut(1);
            // Move messages around
            let (left_child, right_child) = (
                load(&mut left[0].buffer).assert_buffer(),
                load(&mut right[0].buffer).assert_buffer(),
            );
            left_child.rebalance(right_child, &new_pivot_key);
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
    pub fn node_pointer_mut(&mut self) -> &mut RwLock<N>
    where
        N: ObjectReference,
    {
        &mut self.node.children[self.child_idx].ptr
    }

    pub fn child_buffer_pointer_mut(&mut self) -> &mut RwLock<N>
    where
        N: ObjectReference,
    {
        &mut self.node.children[self.child_idx].buffer
    }

    pub fn take_buffer(&mut self) -> (BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>, isize)
    where
        N: ObjectReference,
    {
        // let (buffer, size_delta) = self.node.cbuf_ptrs[self.child_idx].get_mut().take();
        // self.node.meta_data.entries_size -= size_delta;
        // (buffer, -(size_delta as isize))
        todo!()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        arbitrary::GenExt,
        data_management::Object,
        database::DatasetId,
        tree::default_message_action::{DefaultMessageAction, DefaultMessageActionMsg},
    };
    use bincode::serialized_size;

    use quickcheck::{Arbitrary, Gen, TestResult};
    use rand::Rng;
    //use serde::Serialize;
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

        fn serialize_unmodified(&self, w: &mut Vec<u8>) -> Result<(), std::io::Error> {
            bincode::serialize_into(w, self).map_err(|e| {
                debug!("Failed to serialize ObjectPointer.");
                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
            })
        }

        fn deserialize_and_set_unmodified(bytes: &[u8]) -> Result<Self, std::io::Error> {
            match bincode::deserialize::<()>(bytes) {
                Ok(_) => Ok(()),
                Err(e) => {
                    debug!("Failed to deserialize ObjectPointer.");
                    Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                }
            }
        }
    }

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
            let pivot_key_cnt = rng.gen_range(1..20);
            let mut entries_size = 0;

            let mut pivot = Vec::with_capacity(pivot_key_cnt);
            for _ in 0..pivot_key_cnt {
                let pivot_key = CowBytes::arbitrary(g);
                entries_size += pivot_key.size();
                pivot.push(pivot_key);
            }

            let mut children: Vec<Option<NVMChildBuffer>> = Vec::with_capacity(pivot_key_cnt + 1);
            for _ in 0..pivot_key_cnt + 1 {
                let child = NVMChildBuffer::new();
                entries_size += child.size();
                children.push(Some(child));
            }

            NVMInternalNode {
                meta_data: InternalNodeMetaData {
                    pivot,
                    entries_size,
                    level: 1,
                    system_storage_preference: AtomicSystemStoragePreference::from(
                        StoragePreference::NONE,
                    ),
                    pref: AtomicStoragePreference::unknown(),
                    entries_prefs: vec![],
                    entries_sizes: vec![],
                },
                children: vec![],
            }
        }
    }

    fn serialized_size_ex<T: ObjectReference>(nvminternal: &NVMInternalNode<T>) -> usize {
        unimplemented!()
    }

    fn check_size<T: Size + ObjectReference + std::cmp::PartialEq>(node: &mut NVMInternalNode<T>) {
        // // TODO: Fix it.. For the time being the code at the bottom is used to fullfil the task.
        // /* assert_eq!(
        //     node.size(),
        //     serialized_size_ex(node),
        //     "predicted size does not match serialized size"
        // );*/

        // let mut serializer_meta_data = rkyv::ser::serializers::AllocSerializer::<0>::default();
        // serializer_meta_data
        //     .serialize_value(&node.meta_data)
        //     .unwrap();
        // let bytes_meta_data = serializer_meta_data.into_serializer().into_inner();

        // let mut serializer_data = rkyv::ser::serializers::AllocSerializer::<0>::default();
        // serializer_data
        //     .serialize_value(node.data.read().as_ref().unwrap().as_ref().unwrap())
        //     .unwrap();
        // let bytes_data = serializer_data.into_serializer().into_inner();

        // let archivedinternalnodemetadata: &ArchivedInternalNodeMetaData =
        //     rkyv::check_archived_root::<InternalNodeMetaData>(&bytes_meta_data).unwrap();
        // let meta_data: InternalNodeMetaData = archivedinternalnodemetadata
        //     .deserialize(&mut rkyv::de::deserializers::SharedDeserializeMap::new())
        //     .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        //     .unwrap();

        // let archivedinternalnodedata: &ArchivedInternalNodeData<_> =
        //     rkyv::check_archived_root::<InternalNodeData<T>>(&bytes_data).unwrap();
        // let data: InternalNodeData<_> = archivedinternalnodedata
        //     .deserialize(&mut rkyv::de::deserializers::SharedDeserializeMap::new())
        //     .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        //     .unwrap();

        // assert_eq!(node.meta_data, meta_data);
        // assert_eq!(node.data.read().as_ref().unwrap().as_ref().unwrap(), &data);
    }

    #[quickcheck]
    fn check_serialize_size(mut node: NVMInternalNode<()>) {
        check_size(&mut node);
    }

    #[quickcheck]
    fn check_idx(node: NVMInternalNode<()>, key: Key) {
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

    #[quickcheck]
    fn check_size_insert_single(
        mut node: NVMInternalNode<()>,
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
        mut node: NVMInternalNode<()>,
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
        mut node: NVMInternalNode<()>,
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
            added_size_twin += node_twin
                .data
                .write()
                .as_mut()
                .unwrap()
                .as_mut()
                .unwrap()
                .children[idx]
                .as_mut()
                .unwrap()
                .insert(key, keyinfo, msg.0, DefaultMessageAction);
        }
        if added_size_twin > 0 {
            node_twin.meta_data.entries_size += added_size_twin as usize;
        } else {
            node_twin.meta_data.entries_size -= added_size_twin as usize;
        }

        assert_eq!(node.meta_data, node_twin.meta_data);
        assert_eq!(
            node.data.read().as_ref().unwrap().as_ref().unwrap(),
            node_twin.data.read().as_ref().unwrap().as_ref().unwrap()
        );
        assert_eq!(added_size, added_size_twin);
    }

    static mut PK: Option<PivotKey> = None;

    #[quickcheck]
    fn check_size_split(mut node: NVMInternalNode<()>) -> TestResult {
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
    fn check_split(mut node: NVMInternalNode<()>) -> TestResult {
        if node.fanout() < 4 {
            return TestResult::discard();
        }
        let twin = node.clone();
        let (mut right_sibling, pivot, _size_delta, _pivot_key) = node.split();

        assert!(node.fanout() >= 2);
        assert!(right_sibling.fanout() >= 2);

        node.meta_data.entries_size += pivot.size() + right_sibling.meta_data.entries_size;
        node.meta_data.pivot.push(pivot);
        node.meta_data
            .pivot
            .append(&mut right_sibling.meta_data.pivot);
        node.data
            .write()
            .as_mut()
            .unwrap()
            .as_mut()
            .unwrap()
            .children
            .append(
                &mut right_sibling
                    .data
                    .write()
                    .as_mut()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .children,
            );

        assert_eq!(node.meta_data, twin.meta_data);
        assert_eq!(
            node.data.read().as_ref().unwrap().as_ref().unwrap(),
            twin.data.read().as_ref().unwrap().as_ref().unwrap()
        );

        TestResult::passed()
    }

    #[quickcheck]
    fn check_split_key(mut node: NVMInternalNode<()>) -> TestResult {
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
    //     let node: NVMInternalNode<NVMChildBuffer<()>> = NVMInternalNode {
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
