//! Implementation of the generic node wrapper.
use self::Inner::*;
use super::{
    internal::{
        copyless_internal::{ChildLink, CopylessInternalNode, InternalNodeLink},
        packed_child_buffer::PackedChildBuffer,
        TakeChildBuffer,
    },
    FillUpResult, KeyInfo, PivotKey, StorageMap, MIN_FANOUT, MIN_FLUSH_RATIO,
};
use crate::{
    buffer::Buf, checksum::{Builder, Checksum}, compression::{CompressionConfiguration, DecompressionTag}, cow_bytes::{CowBytes, SlicedCowBytes}, data_management::{
        Dml, HasStoragePreference, IntegrityMode, Object, ObjectReference, PreparePack,
    }, database::DatasetId, size::{Size, SizeMut, StaticSize}, tree::{pivot_key::LocalPivotKey, MessageAction, StorageKind}, StoragePreference
};
use parking_lot::RwLock;
use std::{
    borrow::Borrow,
    collections::BTreeMap,
    io::{self, Write},
    mem::replace,
};
use bincode::deserialize;

use std::sync::{Arc, Mutex};

/// The tree node type.
#[derive(Debug)]
pub struct Node<N: 'static>(Inner<N>);

#[derive(Debug)]
pub(super) enum Inner<N: 'static> {
    MemLeaf(PackedChildBuffer),
    CopylessInternal(CopylessInternalNode<N>),
}

macro_rules! kib {
    ($n:expr) => {
        $n * 1024
    };
}

macro_rules! mib {
    ($n:expr) => {
        $n * 1024 * 1024
    };
}

// NOTE: This section is the main description of the properties of the chosen tree nodes.
//
// Essentially a mapping from node type and storage kind to min or max size is
// created. To be noted here is that the current representation of the leaf can
// change before it is actually written to the desired storage kind. So a block
// leaf might be changed to a memory leaf when written to memory.
impl StorageMap {
    pub fn node_is_too_large<N: HasStoragePreference + StaticSize>(&self, node: &Node<N>) -> bool {
        self.max_size(node)
            .map(|max_size| node.inner_size() > max_size || node.has_too_high_fanout(max_size))
            .unwrap_or(false)
    }

    pub fn leaf_is_too_large<N: HasStoragePreference + StaticSize>(
        &self,
        node: &mut Node<N>,
    ) -> bool {
        node.is_leaf() && self.node_is_too_large(node)
    }

    pub fn leaf_is_too_small<N: HasStoragePreference + StaticSize>(&self, node: &Node<N>) -> bool {
        node.is_leaf()
            && self
                .min_size(node)
                .map(|min_size| node.inner_size() < min_size)
                .unwrap_or(false)
    }

    pub fn min_size<N: HasStoragePreference + StaticSize>(&self, node: &Node<N>) -> Option<usize> {
        let pref = node.correct_preference();
        Some(match (&node.0, self.get(pref)) {
            (MemLeaf(_), StorageKind::Hdd) => mib!(1),
            (MemLeaf(_), StorageKind::Ssd) => mib!(1),
            (MemLeaf(_), StorageKind::Memory) => mib!(1),
            (CopylessInternal(_), _) => return None,
            (_, StorageKind::Hdd) => mib!(1),
            (_, StorageKind::Ssd) => kib!(512),
            (_, StorageKind::Memory) => kib!(128),
        })
    }

    pub fn max_size<N: HasStoragePreference + StaticSize>(&self, node: &Node<N>) -> Option<usize> {
        let pref = node.correct_preference();
        Some(match (&node.0, self.get(pref)) {
            (_, StorageKind::Hdd) => mib!(4),
            (_, StorageKind::Ssd) => mib!(2),
            (_, StorageKind::Memory) => kib!(512),
        })
    }
}

trait ChildBufferIteratorTrait<'a, N> {
    fn cb_iter_mut(&'a mut self) -> Box<dyn Iterator<Item = &'a mut N> + 'a>;
    fn cb_iter_ref(&'a self) -> Box<dyn Iterator<Item = &'a N> + 'a>;
    fn cb_iter(self) -> Box<dyn Iterator<Item = N> + 'a>;
}

impl<'a> ChildBufferIteratorTrait<'a, Option<PackedChildBuffer>>
    for Vec<Option<PackedChildBuffer>>
{
    fn cb_iter_mut(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Option<PackedChildBuffer>> + 'a> {
        Box::new(self.iter_mut())
    }

    fn cb_iter_ref(&'a self) -> Box<dyn Iterator<Item = &'a Option<PackedChildBuffer>> + 'a> {
        Box::new(self.iter())
    }

    fn cb_iter(self) -> Box<dyn Iterator<Item = Option<PackedChildBuffer>> + 'a> {
        Box::new(self.into_iter())
    }
}

pub(super) enum ChildrenObjects<'a, N> {
    ChildBuffer(Box<dyn Iterator<Item = N> + 'a>),
    NVMChildBuffer(Box<dyn Iterator<Item = ChildLink<N>> + 'a>),
}

#[derive(Debug)]
enum NodeInnerType {
    CopylessLeaf = 1,
    CopylessInternal,
}

pub(super) const NODE_PREFIX_LEN: usize = std::mem::size_of::<u32>();

impl<R: HasStoragePreference + StaticSize> HasStoragePreference for Node<R> {
    fn current_preference(&self) -> Option<StoragePreference> {
        match self.0 {
            MemLeaf(ref nvmleaf) => nvmleaf.current_preference(),
            CopylessInternal(ref nvminternal) => nvminternal.current_preference(),
        }
    }

    fn recalculate(&self) -> StoragePreference {
        match self.0 {
            MemLeaf(ref nvmleaf) => nvmleaf.recalculate(),
            CopylessInternal(ref nvminternal) => nvminternal.recalculate(),
        }
    }

    fn system_storage_preference(&self) -> StoragePreference {
        match self.0 {
            MemLeaf(ref nvmleaf) => nvmleaf.system_storage_preference(),
            CopylessInternal(ref nvminternal) => nvminternal.system_storage_preference(),
        }
    }

    fn set_system_storage_preference(&mut self, pref: StoragePreference) {
        // NOTE: This generally has a greater impact as leafs need to be
        // unpacked asap. Another solution as proposed by similar approaches is
        // waiting for the next read operation for this leaf.
        self.ensure_unpacked();
        match self.0 {
            MemLeaf(ref mut nvmleaf) => nvmleaf.set_system_storage_preference(pref),
            CopylessInternal(ref mut nvminternal) => {
                nvminternal.set_system_storage_preference(pref)
            }
        }
    }
}

impl<R: ObjectReference + HasStoragePreference + StaticSize> Object<R> for Node<R> {
    fn pack<W: Write, F: Fn(&[u8]) -> C, C: Checksum>(
        &self,
        mut writer: W,
        prepare_pack: PreparePack,
        csum_builder: F, 
        compressor: &CompressionConfiguration
    ) -> Result<IntegrityMode<C>, io::Error> {
        match self.0 {
            MemLeaf(ref leaf) => {
                writer.write_all((NodeInnerType::CopylessLeaf as u32).to_be_bytes().as_ref())?;
                leaf.pack(writer, prepare_pack, csum_builder, compressor)
            }
            CopylessInternal(ref cpl_internal) => {
                writer.write_all(
                    (NodeInnerType::CopylessInternal as u32)
                        .to_be_bytes()
                        .as_ref(),
                )?;
                cpl_internal.pack(writer, prepare_pack, csum_builder, compressor)
            }
        }
    }

    #[cfg(feature = "memory_metrics")]
    fn unpack_at<C: Checksum>(
        d_id: DatasetId,
        data: Buf,
        integrity_mode: IntegrityMode<C>,
        decompressor: DecompressionTag,
        vdev_stats: Option<std::sync::Arc<crate::vdev::AtomicStatistics>>,
    ) -> Result<Self, io::Error> {
        if data[0..4] == (NodeInnerType::CopylessInternal as u32).to_be_bytes() {
            //println!("a..");
            Ok(Node(CopylessInternal(
                CopylessInternalNode::unpack(data, integrity_mode, decompressor, vdev_stats)?
                    .complete_object_refs(d_id),
            )))
        } else if data[0..4] == (NodeInnerType::CopylessLeaf as u32).to_be_bytes() {
            Ok(Node(MemLeaf(PackedChildBuffer::unpack(
                data.into_sliced_cow_bytes().slice_from(4),
                integrity_mode,
                decompressor,
                vdev_stats,
            )?)))
        } else {
            panic!(
                "Unkown bytes to unpack. [0..4]: {}",
                u32::from_be_bytes(data[..4].try_into().unwrap())
            );
        }
    }

    #[cfg(not(feature = "memory_metrics"))]
    fn unpack_at<C: Checksum>(
        d_id: DatasetId,
        data: Buf,
        integrity_mode: IntegrityMode<C>,
        decompressor: DecompressionTag
    ) -> Result<Self, io::Error> {
        if data[0..4] == (NodeInnerType::CopylessInternal as u32).to_be_bytes() {
            //println!("a..");
            Ok(Node(CopylessInternal(
                CopylessInternalNode::unpack(data, integrity_mode, decompressor)?
                    .complete_object_refs(d_id),
            )))
        } else if data[0..4] == (NodeInnerType::CopylessLeaf as u32).to_be_bytes() {
            Ok(Node(MemLeaf(PackedChildBuffer::unpack(
                data.into_sliced_cow_bytes().slice_from(4),
                integrity_mode,
                decompressor,
            )?)))
        } else {
            panic!(
                "Unkown bytes to unpack. [0..4]: {}",
                u32::from_be_bytes(data[..4].try_into().unwrap())
            );
        }
    }

    fn debug_info(&self) -> String {
        format!(
            "{}: {}, {:?}, {}, {:?}",
            self.kind(),
            self.level(),
            self.fanout(),
            self.size(),
            self.actual_size()
        )
    }

    fn for_each_child<E, F>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&mut R) -> Result<(), E>,
    {
        if let Some(iter) = self.child_pointer_iter_mut() {
            for np in iter {
                f(np)?;
            }
        }
        Ok(())
    }

    fn prepare_pack(
        &mut self,
        _storage_kind: StorageKind,
        _pivot_key: &PivotKey,
    ) -> Result<crate::data_management::PreparePack, crate::data_management::Error>
    where
        R: ObjectReference,
    {
        // NOTE: Only necessary transitions are represented here, all others are no-op. Can be improved.
        // self.0 = match (
        //     std::mem::replace(&mut self.0, unsafe { std::mem::zeroed() }),
        //     storage_kind,
        // ) {
        //     // (Internal(internal), StorageKind::Memory) | (Internal(internal), StorageKind::Ssd) => {
        //     //     // Spawn new child buffers from one internal node.
        //     //     Inner::DisjointInternal(internal.to_disjoint_node(|new_cbuf| {
        //     //         dmu.insert(
        //     //             Node(Inner::ChildBuffer(new_cbuf)),
        //     //             pivot_key.d_id(),
        //     //             pivot_key.clone(),
        //     //         )
        //     //     }))
        //     // }
        //     (CopylessInternal(_internal), StorageKind::Hdd) => {
        //         // Fetch children and pipe them into one node.
        //         unimplemented!();
        //         // let mut cbufs = Vec::with_capacity(internal.children.len());
        //         // Inner::Internal(InternalNode::from_disjoint_node(internal, cbufs))
        //     }
        //     (Leaf(leaf), StorageKind::Memory) => Inner::MemLeaf(leaf.to_memory_leaf()),
        //     (MemLeaf(leaf), StorageKind::Ssd) | (MemLeaf(leaf), StorageKind::Hdd) => {
        //         Inner::Leaf(leaf.to_block_leaf())
        //     }
        //     (default, _) => default,
        // };
        
        // Sync metadata with actual buffer sizes before packing
        if let CopylessInternal(ref mut internal) = self.0 {
            for (idx, child) in internal.children.iter().enumerate() {
                internal.meta_data.entries_sizes[idx] = child.buffer().size();
            }
        }
        
        Ok(PreparePack { storage_kind: _storage_kind })
    }
}

impl<N: StaticSize> Size for Node<N> {
    fn size(&self) -> usize {
        match self.0 {
            MemLeaf(ref nvmleaf) => 4 + nvmleaf.size(),
            CopylessInternal(ref nvminternal) => 4 + nvminternal.size(),
        }
    }

    fn actual_size(&self) -> Option<usize> {
        match self.0 {
            MemLeaf(ref nvmleaf) => nvmleaf.actual_size().map(|size| 4 + size),
            CopylessInternal(ref nvminternal) => nvminternal.actual_size().map(|size| 4 + size),
        }
    }

    fn cache_size(&self) -> usize {
        match &self.0 {
            MemLeaf(l) => l.cache_size(),
            CopylessInternal(i) => i.cache_size(),
        }
    }
}

impl<N: StaticSize + HasStoragePreference> Node<N> {
    pub(super) fn try_walk(&mut self, key: &[u8]) -> Option<TakeChildBuffer<N>>
    where
        N: ObjectReference,
    {
        match self.0 {
            MemLeaf(_) => None,
            CopylessInternal(ref mut nvminternal) => Some(nvminternal.try_walk(key)),
        }
    }

    pub(super) fn try_find_flush_candidate(
        &mut self,
        storage_map: &StorageMap,
    ) -> Option<TakeChildBuffer<N>>
    where
        N: ObjectReference,
    {
        let max_size = storage_map.max_size(&self).unwrap();
        match self.0 {
            MemLeaf(_) => None,
            CopylessInternal(ref mut nvminternal) => nvminternal.try_find_flush_candidate(
                max_size / MIN_FLUSH_RATIO,
                max_size,
                MIN_FANOUT,
            ),
        }
    }

    /// This method actually checks the size of the pivots compared to the
    /// maximum size allowed. Pivots should always fill up less than B^epsilon
    /// space.
    fn has_too_high_fanout(&self, max_size: usize) -> bool {
        match &self.0 {
            CopylessInternal(copyless_internal_node) => {
                copyless_internal_node.has_too_high_fanout(max_size)
            }
            _ => false,
        }
    }
}

impl<N: HasStoragePreference + StaticSize> Node<N> {
    pub(super) fn kind(&self) -> &str {
        match self.0 {
            MemLeaf(_) => "nvmleaf",
            CopylessInternal(_) => "nvminternal",
        }
    }
    pub(super) fn fanout(&self) -> Option<usize>
    where
        N: ObjectReference,
    {
        match self.0 {
            MemLeaf(_) => None,
            CopylessInternal(ref nvminternal) => Some(nvminternal.fanout()),
        }
    }

    fn ensure_unpacked(&mut self) -> isize {
        let before = self.cache_size();

        match &mut self.0 {
            MemLeaf(mleaf) => {
                let before = mleaf.cache_size();
                mleaf.unpack_data();
                let after = mleaf.cache_size();
                after as isize - before as isize
            }
            _ => 0,
        }
    }

    fn take(&mut self) -> Self {
        replace(self, Self::empty_leaf())
    }

    pub(super) fn has_too_low_fanout(&self) -> bool
    where
        N: ObjectReference,
    {
        match self.0 {
            MemLeaf(_) => false,
            CopylessInternal(ref nvminternal) => nvminternal.fanout() < MIN_FANOUT,
        }
    }

    pub(super) fn is_leaf(&self) -> bool {
        match self.0 {
            MemLeaf(_) => true,
            CopylessInternal(_) => false,
        }
    }

    pub(super) fn is_disjoint(&self) -> bool {
        match self.0 {
            MemLeaf(_) => false,
            CopylessInternal(_) => true,
        }
    }

    pub(super) fn empty_leaf() -> Self {
        Node(MemLeaf(PackedChildBuffer::new(true)))
    }

    pub(super) fn level(&self) -> u32 {
        match self.0 {
            MemLeaf(_) => 0,
            CopylessInternal(ref nvminternal) => nvminternal.level(),
        }
    }

    pub(super) fn root_needs_merge(&self) -> bool
    where
        N: ObjectReference,
    {
        match self.0 {
            MemLeaf(_) => false,
            CopylessInternal(ref nvminternal) => nvminternal.fanout() == 1,
        }
    }

    fn inner_size(&self) -> usize {
        match &self.0 {
            MemLeaf(m) => m.size(),
            CopylessInternal(d) => d.size(),
        }
    }
}

impl<N: ObjectReference + StaticSize + HasStoragePreference> Node<N> {
    pub(super) fn split_root_mut<F>(&mut self, storage_map: &StorageMap, allocate_obj: F) -> isize
    where
        F: Fn(Self, LocalPivotKey) -> N,
    {
        let can_be_copyless = match storage_map.get(self.correct_preference()) {
            StorageKind::Memory => true,
            _ => false,
        };

        let size_before = self.cache_size();
        self.ensure_unpacked();
        let mut left_sibling = self.take();

        let min_size = storage_map.min_size(&left_sibling);
        let max_size = storage_map.max_size(&left_sibling);
        let (right_sibling, pivot_key, cur_level) = match left_sibling.0 {
            MemLeaf(ref mut nvmleaf) => {
                let (right_sibling, pivot_key, _pk) =
                    nvmleaf.split(min_size.unwrap(), max_size.unwrap()).take().0;
                (Node(MemLeaf(right_sibling)), pivot_key, 0)
            }
            CopylessInternal(ref mut nvminternal) => {
                let (right_sibling, pivot_key, _, _pk) = nvminternal.split();
                (
                    Node(CopylessInternal(right_sibling)),
                    pivot_key,
                    nvminternal.level(),
                )
            }
        };
        debug!("Root split pivot key: {:?}", pivot_key);

        debug_assert!(!left_sibling.has_too_low_fanout());
        debug_assert!(!right_sibling.has_too_low_fanout());

        let left_child = allocate_obj(left_sibling, LocalPivotKey::LeftOuter(pivot_key.clone()));
        let right_child = allocate_obj(right_sibling, LocalPivotKey::Right(pivot_key.clone()));

        let left_buffer = PackedChildBuffer::new(false);
        let right_buffer = PackedChildBuffer::new(false);

        let left_link = InternalNodeLink {
            buffer_size: left_buffer.size(),
            buffer: left_buffer,
            ptr: left_child,
        };

        let right_link = InternalNodeLink {
            buffer_size: right_buffer.size(),
            buffer: right_buffer,
            ptr: right_child,
        };
        *self = Node(CopylessInternal(CopylessInternalNode::new(
            left_link,
            right_link,
            pivot_key,
            cur_level + 1,
        )));

        let size_after = self.cache_size();
        size_after as isize - size_before as isize
    }
}

pub(super) enum GetResult<'a, N: 'a + 'static> {
    Data(Option<(KeyInfo, SlicedCowBytes)>),
    NextNode(&'a RwLock<N>),
}

pub(super) enum ApplyResult<'a, N: 'a + 'static> {
    Leaf(Option<KeyInfo>),
    NextNode(&'a mut N),
}

pub(super) enum PivotGetResult<'a, N: 'a + 'static> {
    Target(Option<&'a RwLock<N>>),
    NextNode(&'a RwLock<N>),
}

pub(super) enum PivotGetMutResult<'a, N: 'a + 'static> {
    Target(Option<&'a mut N>),
    NextNode(&'a mut N),
}

/// Return type of range query fetching all children to the lowest nodes.
pub(super) enum GetRangeResult<'a, T, N: 'a + 'static> {
    Data(T),
    NextNode {
        np: &'a RwLock<N>,
        prefetch_option_node: Option<&'a RwLock<N>>,
    },
}

impl<N: HasStoragePreference> Node<N> {
    pub(super) fn get(&self, key: &[u8], msgs: &mut Vec<(KeyInfo, SlicedCowBytes)>) -> GetResult<N>
    where
        N: ObjectReference,
    {
        match self.0 {
            MemLeaf(ref nvmleaf) => GetResult::Data(nvmleaf.get(key)),
            CopylessInternal(ref nvminternal) => {
                let (child_np, msg) = nvminternal.get(key);
                if let Some(msg) = msg {
                    msgs.push(msg);
                }
                GetResult::NextNode(child_np)
            }
        }
    }

    pub(super) fn get_range<'a>(
        &'a self,
        key: &[u8],
        left_pivot_key: &mut Option<CowBytes>,
        right_pivot_key: &mut Option<CowBytes>,
        all_msgs: &mut BTreeMap<CowBytes, Vec<(KeyInfo, SlicedCowBytes)>>,
    ) -> GetRangeResult<Box<dyn Iterator<Item = (&'a [u8], (KeyInfo, SlicedCowBytes))> + 'a>, N>
    where
        N: ObjectReference,
    {
        match self.0 {
            MemLeaf(ref nvmleaf) => GetRangeResult::Data(Box::new(nvmleaf.get_all_messages())),
            CopylessInternal(ref nvminternal) => {
                let prefetch_option = if nvminternal.level() == 1 && false {
                    nvminternal.get_next_node(key)
                } else {
                    None
                };

                let cl = nvminternal.get_range(key, left_pivot_key, right_pivot_key, all_msgs);

                for (key, msg) in cl.buffer().get_all_messages() {
                    all_msgs
                        .entry(CowBytes::from(key))
                        .or_insert_with(Vec::new)
                        .push(msg.clone());
                }

                GetRangeResult::NextNode {
                    np: cl.ptr(),
                    prefetch_option_node: prefetch_option.map(|l| l.ptr()),
                }
            }
        }
    }

    pub(super) fn pivot_get(&self, pk: &PivotKey) -> Option<PivotGetResult<N>>
    where
        N: ObjectReference,
    {
        if pk.is_root() {
            return Some(PivotGetResult::Target(None));
        }
        match self.0 {
            MemLeaf(_) => None,
            CopylessInternal(ref nvminternal) => Some(nvminternal.pivot_get(pk)),
        }
    }

    pub(super) fn pivot_get_mut(&mut self, pk: &PivotKey) -> Option<PivotGetMutResult<N>>
    where
        N: ObjectReference,
    {
        if pk.is_root() {
            return Some(PivotGetMutResult::Target(None));
        }
        match self.0 {
            MemLeaf(_) => None,
            CopylessInternal(ref mut nvminternal) => Some(nvminternal.pivot_get_mut(pk)),
        }
    }
}

impl<N: HasStoragePreference + StaticSize> Node<N> {
    pub(super) fn insert<K, M>(
        &mut self,
        key: K,
        msg: SlicedCowBytes,
        msg_action: M,
        storage_preference: StoragePreference,
    ) -> isize
    where
        K: Borrow<[u8]> + Into<CowBytes>,
        M: MessageAction,
        N: ObjectReference,
    {
        let size_delta = self.ensure_unpacked();
        let keyinfo = KeyInfo { storage_preference };
        size_delta
            + (match self.0 {
                MemLeaf(ref mut nvmleaf) => nvmleaf.insert(key, keyinfo, msg, msg_action).take().1,
                CopylessInternal(ref mut nvminternal) => {
                    // This is a remainder from the version in which we
                    // wroteback child buffers separately.
                    let child_idx = nvminternal.idx(key.borrow());
                    let link = nvminternal.get_mut(key.borrow());
                    let buffer_node = link.buffer_mut();
                    let size_delta = buffer_node.insert(key, keyinfo, msg, msg_action).take().1;
                    nvminternal.after_insert_size_delta(child_idx, size_delta);
                    size_delta
                }
            })
    }

    pub(super) fn insert_msg_buffer<I, M>(&mut self, msg_buffer: I, msg_action: M) -> isize
    where
        I: IntoIterator<Item = (CowBytes, (KeyInfo, SlicedCowBytes))>,
        M: MessageAction,
        N: ObjectReference,
    {
        let size_delta = self.ensure_unpacked();
        size_delta
            + (match self.0 {
                MemLeaf(ref mut nvmleaf) => {
                    nvmleaf.insert_msg_buffer(msg_buffer, msg_action).take().1
                }
                CopylessInternal(ref mut nvminternal) => {
                    // This is a remainder from the version in which we
                    // wroteback child buffers separately.
                    let mut size_delta = 0;
                    for (k, (kinfo, v)) in msg_buffer {
                        let idx = nvminternal.idx(&k);
                        let link = nvminternal.get_mut(&k);
                        let buffer_node = link.buffer_mut();
                        let delta = buffer_node.insert(k, kinfo, v, msg_action.clone()).take().1;
                        nvminternal.after_insert_size_delta(idx, delta);
                        size_delta += delta;
                    }
                    size_delta
                }
            })
    }

    pub(super) fn apply_with_info(&mut self, key: &[u8], pref: StoragePreference) -> ApplyResult<N>
    where
        N: ObjectReference,
    {
        // FIXME: This is bad for performance, what we want to do here is modify
        // the preference in place determine the new preference and write the
        // PACKED leaf as is again. This violates the restriction that they may
        // never be written again, therefore we need a new interface preparing
        // packed leafs for this exact and only purpose.
        //
        // FIXME: When we unpack this the cache size changes, we need to update
        // the cache entry.
        let size_delta = self.ensure_unpacked();
        match self.0 {
            // FIXME: see above
            MemLeaf(ref mut nvmleaf) => {
                ApplyResult::Leaf(nvmleaf.apply_with_info(key, pref).take().0)
            }
            CopylessInternal(ref mut nvminternal) => {
                ApplyResult::NextNode(nvminternal.apply_with_info(key, pref))
            }
        }
    }
}

impl<N: HasStoragePreference> Node<N> {
    pub(super) fn child_pointer_iter_mut(&mut self) -> Option<Box<dyn Iterator<Item = &mut N> + '_>>
    where
        N: ObjectReference,
    {
        match self.0 {
            MemLeaf(_) => None,
            CopylessInternal(ref mut nvminternal) => Some(Box::new(
                nvminternal
                    .iter_mut()
                    .map(|child| child.ptr_mut().get_mut()),
            )),
            // NOTE: This returns none as it is not necessarily harmful to write
            // it back as no consistency constraints have to be met.
        }
    }

    pub(super) fn child_pointer_iter(&self) -> Option<Box<dyn Iterator<Item = &RwLock<N>> + '_>>
    where
        N: ObjectReference,
    {
        match self.0 {
            MemLeaf(_) => None,
            CopylessInternal(ref nvminternal) => {
                Some(Box::new(nvminternal.iter().map(|link| link.ptr())))
            }
        }
    }

    pub(super) fn drain_children(&mut self) -> Option<ChildrenObjects<'_, N>>
    where
        N: ObjectReference,
    {
        match self.0 {
            MemLeaf(_) => None,
            CopylessInternal(ref mut nvminternal) => Some(ChildrenObjects::NVMChildBuffer(
                Box::new(nvminternal.drain_children()),
            )),
        }
    }
}

impl<N: ObjectReference + StaticSize + HasStoragePreference> Node<N> {
    pub(super) fn split(
        &mut self,
        storage_map: &StorageMap,
    ) -> (Self, CowBytes, isize, LocalPivotKey) {
        self.ensure_unpacked();

        let min_size = storage_map.min_size(self);
        let max_size = storage_map.min_size(self);
        match self.0 {
            MemLeaf(ref mut nvmleaf) => {
                let ((node, pivot_key, pk), size_delta) =
                    nvmleaf.split(min_size.unwrap(), max_size.unwrap()).take();
                (Node(MemLeaf(node)), pivot_key, size_delta, pk)
            }
            CopylessInternal(ref mut nvminternal) => {
                assert!(
                    nvminternal.fanout() >= 2 * MIN_FANOUT,
                    "internal split failed due to low fanout: {}, size: {}, actual_size: {:?}",
                    nvminternal.fanout(),
                    nvminternal.size(),
                    nvminternal.actual_size()
                );
                let (node, pivot_key, size_delta, pk) = nvminternal.split();
                (Node(CopylessInternal(node)), pivot_key, size_delta, pk)
            }
        }
    }

    pub(super) fn merge(&mut self, right_sibling: &mut Self, pivot_key: CowBytes) -> isize {
        // FIXME: Propagate isize change completely
        let d0 = self.ensure_unpacked();
        let _ = right_sibling.ensure_unpacked();
        d0 + match (&mut self.0, &mut right_sibling.0) {
            (&mut MemLeaf(ref mut left), &mut MemLeaf(ref mut right)) => {
                left.append(right).take().1
            }
            (&mut CopylessInternal(ref mut left), &mut CopylessInternal(ref mut right)) => {
                left.merge(right, pivot_key)
            }
            _ => {
                let bt = std::backtrace::Backtrace::force_capture();
                println!("{}", bt);
                println!(
                    "Left is {} \n Right is {}",
                    self.debug_info(),
                    right_sibling.debug_info()
                );
                unreachable!()
            }
        }
    }

    pub(super) fn leaf_rebalance(
        &mut self,
        right_sibling: &mut Self,
        storage_map: &StorageMap,
    ) -> FillUpResult {
        self.ensure_unpacked();
        right_sibling.ensure_unpacked();

        let min_size = storage_map.min_size(self);
        let max_size = storage_map.min_size(self);
        match (&mut self.0, &mut right_sibling.0) {
            (&mut MemLeaf(ref mut left), &mut MemLeaf(ref mut right)) => {
                left.rebalance_size(right, min_size.unwrap(), max_size.unwrap())
            }
            _ => unreachable!(),
        }
    }

    /*pub(super) fn range_delete(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> (isize, Option<(&mut N, Option<&mut N>, Vec<N>)>) {
        self.ensure_unpacked();
        match self.0 {
            PackedLeaf(_) => unreachable!(),
            Leaf(ref mut leaf) => {
                let size_delta = leaf.range_delete(start, end);
                (-(size_delta as isize), None)
            }
            Internal(ref mut internal) => {
                let mut dead = Vec::new();
                let (size_delta, l, r) = internal.range_delete(start, end, &mut dead);
                (-(size_delta as isize), Some((l, r, dead)))
            }
        }
    }*/
}

#[derive(serde::Serialize)]
pub struct ChildInfo {
    from: Option<ByteString>,
    to: Option<ByteString>,
    storage: StoragePreference,
    pub pivot_key: PivotKey,
    pub child: NodeInfo,
}

#[derive(serde::Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
#[allow(missing_docs)]
pub enum NodeInfo {
    Leaf {
        level: u32,
        storage: StoragePreference,
        system_storage: StoragePreference,
        entry_count: usize,
    },
    Internal {
        level: u32,
        storage: StoragePreference,
        system_storage: StoragePreference,
        children: Vec<ChildInfo>,
    },
}

pub struct ByteString(Vec<u8>);

impl serde::Serialize for ByteString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        static CHARS: &[u8] = b"0123456789ABCDEF";
        let mut s = String::with_capacity(self.0.len() * 2 + self.0.len() / 4);
        for chunk in self.0.chunks(2) {
            for b in chunk {
                let (upper, lower) = (b >> 4, b & 0x0F);
                s.push(CHARS[upper as usize] as char);
                s.push(CHARS[lower as usize] as char);
            }
            s.push(' ');
        }

        serializer.serialize_str(s.trim_end())
    }
}

impl<N: HasStoragePreference + ObjectReference> Node<N> {
    pub(crate) fn node_info<D>(&self, dml: &D) -> NodeInfo
    where
        D: Dml<Object = Node<N>, ObjectRef = N>,
        N: ObjectReference<ObjectPointer = D::ObjectPointer>,
    {
        match &self.0 {
            Inner::MemLeaf(ref nvmleaf) => NodeInfo::Leaf {
                storage: self.correct_preference(),
                system_storage: self.system_storage_preference(),
                level: self.level(),
                entry_count: nvmleaf.len(),
            },
            Inner::CopylessInternal(ref nvminternal) => NodeInfo::Internal {
                storage: self.correct_preference(),
                system_storage: self.system_storage_preference(),
                level: self.level(),
                children: {
                    let itr = nvminternal
                        .children
                        .iter()
                        .enumerate()
                        .map(move |(idx, child)| {
                            let maybe_left = if idx == 0 {
                                None
                            } else {
                                nvminternal.meta_data.pivot.get(idx - 1)
                            };

                            let maybe_right = nvminternal.meta_data.pivot.get(idx);

                            (maybe_left, child, maybe_right)
                        });

                    itr.map(|(maybe_left, child_buf, maybe_right)| {
                        let (child, storage_preference, pivot_key) = {
                            let mut np = child_buf.ptr().write();
                            let pivot_key = np.index().clone();
                            let storage_preference = np.correct_preference();
                            let child = dml.get(&mut np).unwrap();
                            (child, storage_preference, pivot_key)
                        };

                        let node_info = child.node_info(dml);
                        drop(child);

                        dml.evict().unwrap();

                        ChildInfo {
                            from: maybe_left.map(|cow| ByteString(cow.to_vec())),
                            to: maybe_right.map(|cow| ByteString(cow.to_vec())),
                            storage: storage_preference,
                            pivot_key,
                            child: node_info,
                        }
                    })
                    .collect()
                },
            },
        }
    }
}
