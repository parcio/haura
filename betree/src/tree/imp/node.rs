//! Implementation of the generic node wrapper.
use self::Inner::*;
use super::{
    child_buffer::ChildBuffer,
    disjoint_internal::{ChildLink, DisjointInternalNode},
    internal::InternalNode,
    leaf::LeafNode,
    nvm_child_buffer::NVMChildBuffer,
    nvmleaf::{NVMFillUpResult, NVMLeafNode},
    packed::PackedMap,
    take_child_buffer::TakeChildBufferWrapper,
    FillUpResult, KeyInfo, PivotKey, StorageMap, MAX_INTERNAL_NODE_SIZE, MAX_LEAF_NODE_SIZE,
    MIN_FANOUT, MIN_FLUSH_SIZE, MIN_LEAF_NODE_SIZE,
};
use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::{Dml, HasStoragePreference, Object, ObjectReference, PreparePack},
    database::DatasetId,
    size::{Size, SizeMut, StaticSize},
    storage_pool::{DiskOffset, StoragePoolLayer},
    tree::{pivot_key::LocalPivotKey, MessageAction, StorageKind},
    vdev::Block,
    StoragePreference,
};
use bincode::{deserialize, serialize_into};
use parking_lot::RwLock;
use std::{
    borrow::Borrow,
    collections::BTreeMap,
    io::{self, Write},
    mem::replace,
};

/// The tree node type.
#[derive(Debug)]
pub struct Node<N: 'static>(Inner<N>);

#[derive(Debug)]
pub(super) enum Inner<N: 'static> {
    PackedLeaf(PackedMap),
    Leaf(LeafNode),
    MemLeaf(NVMLeafNode),
    Internal(InternalNode<N>),
    DisjointInternal(DisjointInternalNode<N>),
    ChildBuffer(NVMChildBuffer),
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
            .map(|max_size| node.inner_size() > max_size)
            .unwrap_or(false)
    }

    pub fn leaf_is_too_large<N: HasStoragePreference + StaticSize>(&self, node: &Node<N>) -> bool {
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
        Some(match (&node.0, self.get(node.correct_preference())) {
            (PackedLeaf(_), StorageKind::Hdd)
            | (Leaf(_), StorageKind::Hdd)
            | (MemLeaf(_), StorageKind::Hdd) => mib!(1),
            (PackedLeaf(_), StorageKind::Ssd)
            | (Leaf(_), StorageKind::Ssd)
            | (MemLeaf(_), StorageKind::Ssd) => kib!(128),
            (PackedLeaf(_), StorageKind::Memory)
            | (Leaf(_), StorageKind::Memory)
            | (MemLeaf(_), StorageKind::Memory) => mib!(1),
            (Internal(_), _) => return None,
            (DisjointInternal(_), _) => return None,
            (Inner::ChildBuffer(_), _) => return None,
        })
    }

    pub fn max_size<N: HasStoragePreference + StaticSize>(&self, node: &Node<N>) -> Option<usize> {
        Some(match (&node.0, self.get(node.correct_preference())) {
            (PackedLeaf(_), StorageKind::Hdd) | (Leaf(_), StorageKind::Hdd) => mib!(4),
            (PackedLeaf(_), StorageKind::Ssd) | (Leaf(_), StorageKind::Ssd) => mib!(2),
            (PackedLeaf(_), StorageKind::Memory)
            | (Leaf(_), StorageKind::Memory)
            | (MemLeaf(_), _) => mib!(2),
            (Internal(_), _) => mib!(4),
            (DisjointInternal(_), _) => mib!(4),
            (Inner::ChildBuffer(_), _) => return None,
        })
    }
}

trait ChildBufferIteratorTrait<'a, N> {
    fn cb_iter_mut(&'a mut self) -> Box<dyn Iterator<Item = &'a mut N> + 'a>;
    fn cb_iter_ref(&'a self) -> Box<dyn Iterator<Item = &'a N> + 'a>;
    fn cb_iter(self) -> Box<dyn Iterator<Item = N> + 'a>;
}

impl<'a, N> ChildBufferIteratorTrait<'a, ChildBuffer<N>> for Vec<ChildBuffer<N>> {
    fn cb_iter_mut(&'a mut self) -> Box<dyn Iterator<Item = &'a mut ChildBuffer<N>> + 'a> {
        Box::new(self.iter_mut())
    }

    fn cb_iter_ref(&'a self) -> Box<dyn Iterator<Item = &'a ChildBuffer<N>> + 'a> {
        Box::new(self.iter())
    }

    fn cb_iter(self) -> Box<dyn Iterator<Item = ChildBuffer<N>> + 'a> {
        Box::new(self.into_iter())
    }
}

impl<'a> ChildBufferIteratorTrait<'a, Option<NVMChildBuffer>> for Vec<Option<NVMChildBuffer>> {
    fn cb_iter_mut(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Option<NVMChildBuffer>> + 'a> {
        Box::new(self.iter_mut())
    }

    fn cb_iter_ref(&'a self) -> Box<dyn Iterator<Item = &'a Option<NVMChildBuffer>> + 'a> {
        Box::new(self.iter())
    }

    fn cb_iter(self) -> Box<dyn Iterator<Item = Option<NVMChildBuffer>> + 'a> {
        Box::new(self.into_iter())
    }
}

pub(super) enum ChildrenObjects<'a, N> {
    ChildBuffer(Box<dyn Iterator<Item = N> + 'a>),
    NVMChildBuffer(Box<dyn Iterator<Item = ChildLink<N>> + 'a>),
}

#[derive(Debug)]
enum NodeInnerType {
    Packed = 1,
    Leaf,
    Internal,
    NVMLeaf,
    NVMInternal,
    ChildBuffer,
}

pub(super) const NODE_PREFIX_LEN: usize = std::mem::size_of::<u32>();

impl<R: HasStoragePreference + StaticSize> HasStoragePreference for Node<R> {
    fn current_preference(&self) -> Option<StoragePreference> {
        match self.0 {
            PackedLeaf(_) => None,
            Leaf(ref leaf) => leaf.current_preference(),
            Internal(ref internal) => internal.current_preference(),
            MemLeaf(ref nvmleaf) => nvmleaf.current_preference(),
            DisjointInternal(ref nvminternal) => nvminternal.current_preference(),
            ChildBuffer(ref cbuf) => cbuf.current_preference(),
        }
    }

    fn recalculate(&self) -> StoragePreference {
        match self.0 {
            PackedLeaf(_) => {
                unreachable!("packed leaves are never written back, have no preference")
            }
            Leaf(ref leaf) => leaf.recalculate(),
            Internal(ref internal) => internal.recalculate(),
            MemLeaf(ref nvmleaf) => nvmleaf.recalculate(),
            DisjointInternal(ref nvminternal) => nvminternal.recalculate(),
            ChildBuffer(ref cbuf) => cbuf.recalculate(),
        }
    }

    fn system_storage_preference(&self) -> StoragePreference {
        match self.0 {
            // A packed leaf does not have a storage preference
            PackedLeaf(_) => unreachable!("packed leaf preference cannot be determined"),
            Leaf(ref leaf) => leaf.system_storage_preference(),
            Internal(ref int) => int.system_storage_preference(),
            MemLeaf(ref nvmleaf) => nvmleaf.system_storage_preference(),
            DisjointInternal(ref nvminternal) => nvminternal.system_storage_preference(),
            ChildBuffer(ref cbuf) => cbuf.system_storage_preference(),
        }
    }

    fn set_system_storage_preference(&mut self, pref: StoragePreference) {
        // NOTE: This generally has a greater impact as leafs need to be
        // unpacked asap. Another solution as proposed by similar approaches is
        // waiting for the next read operation for this leaf.
        self.ensure_unpacked();
        match self.0 {
            PackedLeaf(_) => unreachable!("packed leaves cannot have their preference updated"),
            Leaf(ref mut leaf) => leaf.set_system_storage_preference(pref),
            Internal(ref mut int) => int.set_system_storage_preference(pref),
            MemLeaf(ref mut nvmleaf) => nvmleaf.set_system_storage_preference(pref),
            DisjointInternal(ref mut nvminternal) => {
                nvminternal.set_system_storage_preference(pref)
            }
            ChildBuffer(ref mut cbuf) => cbuf.set_system_storage_preference(pref),
        }
    }
}

impl<R: ObjectReference + HasStoragePreference + StaticSize> Object<R> for Node<R> {
    fn pack<W: Write>(
        &self,
        mut writer: W,
        _: PreparePack,
    ) -> Result<Option<Block<u32>>, io::Error> {
        match self.0 {
            PackedLeaf(ref map) => writer.write_all(map.inner()).map(|_| None),
            Leaf(ref leaf) => {
                writer.write_all((NodeInnerType::Leaf as u32).to_be_bytes().as_ref())?;
                PackedMap::pack(leaf, writer).map(|_| None)
            }
            Internal(ref internal) => {
                writer.write_all((NodeInnerType::Internal as u32).to_be_bytes().as_ref())?;
                serialize_into(writer, internal)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
                    .map(|_| None)
            }
            MemLeaf(ref leaf) => {
                writer.write_all((NodeInnerType::NVMLeaf as u32).to_be_bytes().as_ref())?;
                leaf.pack(writer)
            }
            DisjointInternal(ref nvminternal) => {
                debug!("NVMInternal node packed successfully");
                writer.write_all((NodeInnerType::NVMInternal as u32).to_be_bytes().as_ref())?;
                nvminternal.pack(writer).map(|_| None)
            }
            ChildBuffer(ref cbuf) => {
                writer.write_all((NodeInnerType::ChildBuffer as u32).to_be_bytes().as_ref())?;
                cbuf.pack(writer).map(|_| None)
            }
        }
    }

    fn unpack_at<SPL: StoragePoolLayer>(
        size: crate::vdev::Block<u32>,
        pool: Box<SPL>,
        offset: DiskOffset,
        d_id: DatasetId,
        data: Box<[u8]>,
    ) -> Result<Self, io::Error> {
        if data[0..4] == (NodeInnerType::Internal as u32).to_be_bytes() {
            match deserialize::<InternalNode<_>>(&data[4..]) {
                Ok(internal) => Ok(Node(Internal(internal.complete_object_refs(d_id)))),
                Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            }
        } else if data[0..4] == (NodeInnerType::Leaf as u32).to_be_bytes() {
            // storage_preference is not preserved for packed leaves,
            // because they will not be written back to disk until modified,
            // and every modification requires them to be unpacked.
            // The leaf contents are scanned cheaply during unpacking, which
            // recalculates the correct storage_preference for the contained keys.
            Ok(Node(PackedLeaf(PackedMap::new(data))))
        } else if data[0..4] == (NodeInnerType::NVMInternal as u32).to_be_bytes() {
            Ok(Node(DisjointInternal(
                DisjointInternalNode::unpack(&data[4..])?.complete_object_refs(d_id),
            )))
        } else if data[0..4] == (NodeInnerType::NVMLeaf as u32).to_be_bytes() {
            Ok(Node(MemLeaf(NVMLeafNode::unpack(
                data, pool, offset, size,
            )?)))
        } else if data[0..4] == (NodeInnerType::ChildBuffer as u32).to_be_bytes() {
            Ok(Node(ChildBuffer(NVMChildBuffer::unpack(data)?)))
        } else {
            panic!(
                "Unkown bytes to unpack. [0..4]: {}",
                u32::from_be_bytes(data[..4].try_into().unwrap())
            );
        }
    }

    fn debug_info(&self) -> String {
        format!(
            "{}: {:?}, {}, {:?}",
            self.kind(),
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

    fn prepare_pack<X>(
        &mut self,
        storage_kind: StorageKind,
        dmu: &X,
        pivot_key: &PivotKey,
    ) -> Result<crate::data_management::PreparePack, crate::data_management::Error>
    where
        R: ObjectReference,
        X: Dml<Object = Node<R>, ObjectRef = R>,
    {
        // NOTE: Only necessary transitions are represented here, all others are no-op. Can be improved.
        self.0 = match (
            std::mem::replace(&mut self.0, unsafe { std::mem::zeroed() }),
            storage_kind,
        ) {
            (Internal(internal), StorageKind::Memory) | (Internal(internal), StorageKind::Ssd) => {
                // Spawn new child buffers from one internal node.
                Inner::DisjointInternal(internal.to_disjoint_node(|new_cbuf| {
                    dmu.insert(
                        Node(Inner::ChildBuffer(new_cbuf)),
                        pivot_key.d_id(),
                        pivot_key.clone(),
                    )
                }))
            }
            (DisjointInternal(mut internal), StorageKind::Hdd) => {
                // Fetch children and pipe them into one node.
                let mut cbufs = Vec::with_capacity(internal.children.len());
                for link in internal.children.iter_mut() {
                    let buf_ptr = std::mem::replace(link.buffer_mut().get_mut(), unsafe {
                        std::mem::zeroed()
                    });
                    cbufs.push(match dmu.get_and_remove(buf_ptr)?.0 {
                        Inner::ChildBuffer(buf) => buf,
                        _ => unreachable!(),
                    });
                }
                Inner::Internal(InternalNode::from_disjoint_node(internal, cbufs))
            }
            (Leaf(leaf), StorageKind::Memory) => Inner::MemLeaf(leaf.to_memory_leaf()),
            (MemLeaf(leaf), StorageKind::Ssd) | (MemLeaf(leaf), StorageKind::Hdd) => {
                Inner::Leaf(leaf.to_block_leaf())
            }
            (default, _) => default,
        };
        Ok(PreparePack())
    }
}

impl<N: StaticSize> Size for Node<N> {
    fn size(&self) -> usize {
        match self.0 {
            PackedLeaf(ref map) => map.size(),
            Leaf(ref leaf) => leaf.size(),
            Internal(ref internal) => 4 + internal.size(),
            MemLeaf(ref nvmleaf) => 4 + nvmleaf.size(),
            DisjointInternal(ref nvminternal) => 4 + nvminternal.size(),
            Inner::ChildBuffer(ref buffer) => 4 + buffer.size(),
        }
    }

    fn actual_size(&self) -> Option<usize> {
        match self.0 {
            PackedLeaf(ref map) => map.actual_size(),
            Leaf(ref leaf) => leaf.actual_size(),
            Internal(ref internal) => internal.actual_size().map(|size| 4 + size),
            MemLeaf(ref nvmleaf) => nvmleaf.actual_size().map(|size| 4 + size),
            DisjointInternal(ref nvminternal) => nvminternal.actual_size().map(|size| 4 + size),
            Inner::ChildBuffer(ref buffer) => buffer.actual_size().map(|size| 4 + size),
        }
    }
}

impl<N: StaticSize + HasStoragePreference> Node<N> {
    pub(super) fn try_walk(&mut self, key: &[u8]) -> Option<TakeChildBufferWrapper<N>>
    where
        N: ObjectReference,
    {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref mut internal) => internal
                .try_walk(key)
                .map(TakeChildBufferWrapper::TakeChildBuffer),
            MemLeaf(_) => None,
            DisjointInternal(ref mut nvminternal) => Some(
                TakeChildBufferWrapper::NVMTakeChildBuffer(nvminternal.try_walk_incomplete(key)),
            ),
            Inner::ChildBuffer(_) => todo!(),
        }
    }

    pub(super) fn try_find_flush_candidate(&mut self) -> Option<TakeChildBufferWrapper<N>>
    where
        N: ObjectReference,
    {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref mut internal) => internal.try_find_flush_candidate(
                MIN_FLUSH_SIZE,
                MAX_INTERNAL_NODE_SIZE,
                MIN_FANOUT,
            ),
            MemLeaf(_) => None,
            DisjointInternal(ref mut nvminternal) => nvminternal.try_find_flush_candidate(
                MIN_FLUSH_SIZE,
                MAX_INTERNAL_NODE_SIZE,
                MIN_FANOUT,
            ),
            Inner::ChildBuffer(_) => unreachable!(),
        }
    }
}

impl<N: HasStoragePreference + StaticSize> Node<N> {
    pub(super) fn kind(&self) -> &str {
        match self.0 {
            PackedLeaf(_) => "packed leaf",
            Leaf(_) => "leaf",
            Internal(_) => "internal",
            MemLeaf(_) => "nvmleaf",
            DisjointInternal(_) => "nvminternal",
            Inner::ChildBuffer(_) => "child buffer",
        }
    }
    pub(super) fn fanout(&self) -> Option<usize>
    where
        N: ObjectReference,
    {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref internal) => Some(internal.fanout()),
            MemLeaf(_) => None,
            DisjointInternal(ref nvminternal) => Some(nvminternal.fanout()),
            Inner::ChildBuffer(_) => None,
        }
    }

    fn ensure_unpacked(&mut self) -> isize {
        let before = self.size();

        let leaf = if let PackedLeaf(ref mut map) = self.0 {
            map.unpack_leaf()
        } else {
            return 0;
        };

        self.0 = Leaf(leaf);
        let after = self.size();
        after as isize - before as isize
    }

    fn take(&mut self) -> Self {
        let kind = match self.0 {
            PackedLeaf(_) | Leaf(_) | Internal(_) => StorageKind::Hdd,
            MemLeaf(_) | DisjointInternal(_) => StorageKind::Memory,
            Inner::ChildBuffer(_) => unreachable!(),
        };
        replace(self, Self::empty_leaf(kind))
    }

    pub(super) fn has_too_low_fanout(&self) -> bool
    where
        N: ObjectReference,
    {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => false,
            Internal(ref internal) => internal.fanout() < MIN_FANOUT,
            MemLeaf(_) => false,
            DisjointInternal(ref nvminternal) => nvminternal.fanout() < MIN_FANOUT,
            Inner::ChildBuffer(_) => unreachable!(),
        }
    }

    pub(super) fn is_leaf(&self) -> bool {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => true,
            Internal(_) => false,
            MemLeaf(_) => true,
            DisjointInternal(_) => false,
            Inner::ChildBuffer(_) => unreachable!(),
        }
    }

    pub(super) fn empty_leaf(kind: StorageKind) -> Self {
        match kind {
            StorageKind::Hdd => Node(Leaf(LeafNode::new())),
            StorageKind::Memory => Node(MemLeaf(NVMLeafNode::new())),
            StorageKind::Ssd => Node(Leaf(LeafNode::new())),
        }
    }

    pub(super) fn level(&self) -> u32 {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => 0,
            Internal(ref internal) => internal.level(),
            MemLeaf(_) => 0,
            DisjointInternal(ref nvminternal) => nvminternal.level(),
            Inner::ChildBuffer(_) => unreachable!(),
        }
    }

    pub(super) fn root_needs_merge(&self) -> bool
    where
        N: ObjectReference,
    {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => false,
            Internal(ref internal) => internal.fanout() == 1,
            MemLeaf(_) => false,
            DisjointInternal(ref nvminternal) => nvminternal.fanout() == 1,
            Inner::ChildBuffer(_) => unreachable!(),
        }
    }

    fn inner_size(&self) -> usize {
        match &self.0 {
            PackedLeaf(p) => p.size(),
            Leaf(l) => l.size(),
            MemLeaf(m) => m.size(),
            Internal(i) => i.size(),
            DisjointInternal(d) => d.size(),
            Inner::ChildBuffer(c) => c.size(),
        }
    }
}

impl<N: ObjectReference + StaticSize + HasStoragePreference> Node<N> {
    pub(super) fn split_root_mut<F>(&mut self, storage_map: &StorageMap, allocate_obj: F) -> isize
    where
        F: Fn(Self, LocalPivotKey) -> N,
    {
        let is_disjoint = match storage_map.get(self.correct_preference()) {
            StorageKind::Memory | StorageKind::Ssd => true,
            _ => false,
        };

        let size_before = self.size();
        self.ensure_unpacked();
        let mut left_sibling = self.take();

        let min_size = storage_map.min_size(&left_sibling);
        let max_size = storage_map.min_size(&left_sibling);
        let (right_sibling, pivot_key, cur_level) = match left_sibling.0 {
            PackedLeaf(_) => unreachable!(),
            Leaf(ref mut leaf) => {
                let (right_sibling, pivot_key, _, _pk) =
                    leaf.split(min_size.unwrap(), max_size.unwrap());
                (Node(Leaf(right_sibling)), pivot_key, 0)
            }
            Internal(ref mut internal) => {
                let (right_sibling, pivot_key, _, _pk) = internal.split();
                (Node(Internal(right_sibling)), pivot_key, internal.level())
            }
            MemLeaf(ref mut nvmleaf) => {
                let (right_sibling, pivot_key, _, _pk) =
                    nvmleaf.split(min_size.unwrap(), max_size.unwrap());
                (Node(MemLeaf(right_sibling)), pivot_key, 0)
            }
            DisjointInternal(ref mut nvminternal) => {
                let (right_sibling, pivot_key, _, _pk) = nvminternal.split();
                (
                    Node(DisjointInternal(right_sibling)),
                    pivot_key,
                    nvminternal.level(),
                )
            }
            Inner::ChildBuffer(_) => unreachable!(),
        };
        debug!("Root split pivot key: {:?}", pivot_key);

        if is_disjoint {
            let left_child =
                allocate_obj(left_sibling, LocalPivotKey::LeftOuter(pivot_key.clone()));
            let right_child = allocate_obj(right_sibling, LocalPivotKey::Right(pivot_key.clone()));

            let left_buffer = NVMChildBuffer::new();
            let right_buffer = NVMChildBuffer::new();

            let left_link = crate::tree::imp::disjoint_internal::InternalNodeLink {
                buffer_size: left_buffer.size(),
                buffer_ptr: allocate_obj(
                    Node(Inner::ChildBuffer(left_buffer)),
                    LocalPivotKey::LeftOuter(pivot_key.clone()),
                ),
                ptr: left_child,
            };

            let right_link = crate::tree::imp::disjoint_internal::InternalNodeLink {
                buffer_size: right_buffer.size(),
                buffer_ptr: allocate_obj(
                    Node(Inner::ChildBuffer(right_buffer)),
                    LocalPivotKey::LeftOuter(pivot_key.clone()),
                ),
                ptr: right_child,
            };
            *self = Node(DisjointInternal(DisjointInternalNode::new(
                left_link,
                right_link,
                pivot_key,
                cur_level + 1,
            )));
        } else {
            *self = Node(Internal(InternalNode::new(
                ChildBuffer::new(allocate_obj(
                    left_sibling,
                    LocalPivotKey::LeftOuter(pivot_key.clone()),
                )),
                ChildBuffer::new(allocate_obj(
                    right_sibling,
                    LocalPivotKey::Right(pivot_key.clone()),
                )),
                pivot_key,
                cur_level + 1,
            )));
        }

        let size_after = self.size();
        size_after as isize - size_before as isize
    }
}

pub(super) enum GetResult<'a, N: 'a + 'static> {
    Data(Option<(KeyInfo, SlicedCowBytes)>),
    NextNode(&'a RwLock<N>),
    NVMNextNode {
        child: &'a RwLock<N>,
        buffer: &'a RwLock<N>,
    },
    ChildBuffer,
}

pub(super) enum ApplyResult<'a, N: 'a + 'static> {
    Leaf(Option<KeyInfo>),
    NextNode(&'a mut N),
    NVMNextNode { child: &'a mut N, buffer: &'a mut N },
    NVMLeaf(Option<KeyInfo>),
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
        /// If a node is only partially present in storage we might need to
        /// fetch some additional object to complete the buffered messages.
        child_buffer: Option<&'a RwLock<N>>,
        prefetch_option: Option<&'a RwLock<N>>,
    },
}

impl<N> Node<N> {
    pub(super) fn new_buffer(buffer: NVMChildBuffer) -> Self {
        Node(Inner::ChildBuffer(buffer))
    }

    /// Unpack the node to the internal [NVMChildBuffer] type. Panicks if the
    /// node is not instance of variant [Inner::ChildBuffer].
    pub(super) fn assert_buffer(&self) -> &NVMChildBuffer {
        match self.0 {
            Inner::ChildBuffer(ref cbuf) => cbuf,
            _ => panic!(),
        }
    }

    /// Unpack the node to the internal [NVMChildBuffer] type. Panicks if the
    /// node is not instance of variant [Inner::ChildBuffer].
    pub(super) fn assert_buffer_mut(&mut self) -> &mut NVMChildBuffer {
        match self.0 {
            Inner::ChildBuffer(ref mut cbuf) => cbuf,
            _ => panic!(),
        }
    }

    pub(super) fn is_buffer(&self) -> bool {
        match self.0 {
            PackedLeaf(_) | Leaf(_) | MemLeaf(_) | Internal(_) | DisjointInternal(_) => false,
            Inner::ChildBuffer(_) => true,
        }
    }
}

impl<N: HasStoragePreference> Node<N> {
    pub(super) fn get(&self, key: &[u8], msgs: &mut Vec<(KeyInfo, SlicedCowBytes)>) -> GetResult<N>
    where
        N: ObjectReference,
    {
        match self.0 {
            PackedLeaf(ref map) => GetResult::Data(map.get(key)),
            Leaf(ref leaf) => GetResult::Data(leaf.get_with_info(key)),
            Internal(ref internal) => {
                let (child_np, msg) = internal.get(key);
                if let Some(msg) = msg {
                    msgs.push(msg);
                }
                GetResult::NextNode(child_np)
            }
            MemLeaf(ref nvmleaf) => GetResult::Data(nvmleaf.get_with_info(key)),
            DisjointInternal(ref nvminternal) => {
                let child_link = nvminternal.get(key);

                GetResult::NVMNextNode {
                    child: child_link.ptr(),
                    buffer: child_link.buffer(),
                }
            }
            Inner::ChildBuffer(ref buf) => {
                if let Some(msg) = buf.get(key) {
                    msgs.push(msg.clone());
                }
                GetResult::ChildBuffer
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
            PackedLeaf(ref map) => GetRangeResult::Data(Box::new(map.get_all())),
            Leaf(ref leaf) => GetRangeResult::Data(Box::new(
                leaf.entries().iter().map(|(k, v)| (&k[..], v.clone())),
            )),
            Internal(ref internal) => {
                let prefetch_option = if internal.level() == 1 {
                    internal.get_next_node(key)
                } else {
                    None
                };
                let np = internal.get_range(key, left_pivot_key, right_pivot_key, all_msgs);
                GetRangeResult::NextNode {
                    prefetch_option,
                    child_buffer: None,
                    np,
                }
            }
            MemLeaf(ref nvmleaf) => {
                GetRangeResult::Data(Box::new(nvmleaf.range().map(|(k, v)| (&k[..], v.clone()))))
            }
            DisjointInternal(ref nvminternal) => {
                let prefetch_option = if nvminternal.level() == 1 {
                    nvminternal.get_next_node(key)
                } else {
                    None
                };

                let cl = nvminternal.get_range(key, left_pivot_key, right_pivot_key, all_msgs);
                GetRangeResult::NextNode {
                    np: cl.ptr(),
                    child_buffer: Some(cl.buffer()),
                    prefetch_option,
                }
            }
            Inner::ChildBuffer(_) => unreachable!(),
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
            PackedLeaf(_) | Leaf(_) => None,
            Internal(ref internal) => Some(internal.pivot_get(pk)),
            MemLeaf(_) => None,
            DisjointInternal(ref nvminternal) => Some(nvminternal.pivot_get(pk)),
            Inner::ChildBuffer(_) => unreachable!(),
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
            PackedLeaf(_) | Leaf(_) => None,
            Internal(ref mut internal) => Some(internal.pivot_get_mut(pk)),
            MemLeaf(_) => None,
            DisjointInternal(ref mut nvminternal) => Some(nvminternal.pivot_get_mut(pk)),
            Inner::ChildBuffer(_) => unreachable!(),
        }
    }
}

impl<N: HasStoragePreference + StaticSize> Node<N> {
    pub(super) fn insert<K, M, X>(
        &mut self,
        key: K,
        msg: SlicedCowBytes,
        msg_action: M,
        storage_preference: StoragePreference,
        dml: &X,
        d_id: DatasetId,
    ) -> isize
    where
        K: Borrow<[u8]> + Into<CowBytes>,
        M: MessageAction,
        N: ObjectReference,
        X: Dml<Object = Node<N>, ObjectRef = N>,
    {
        let size_delta = self.ensure_unpacked();
        let keyinfo = KeyInfo { storage_preference };
        size_delta
            + (match self.0 {
                PackedLeaf(_) => unreachable!(),
                Leaf(ref mut leaf) => leaf.insert(key, keyinfo, msg, msg_action),
                Internal(ref mut internal) => internal.insert(key, keyinfo, msg, msg_action),
                MemLeaf(ref mut nvmleaf) => nvmleaf.insert(key, keyinfo, msg, msg_action),
                DisjointInternal(ref mut nvminternal) => {
                    let link = nvminternal.get_mut(key.borrow());
                    // FIXME: Treat this error, this may happen if the database
                    // is in an invalid state for example when nodes are moved
                    // around. It shouldn't happen in theory at this point, but
                    // there is the possibility of bugs.
                    let mut buffer_node = dml.get_mut(link.buffer_mut().get_mut(), d_id).unwrap();
                    let child_idx = nvminternal.idx(key.borrow());
                    let size_delta =
                        buffer_node.insert(key, msg, msg_action, storage_preference, dml, d_id);
                    nvminternal.after_insert_size_delta(child_idx, size_delta);
                    size_delta
                }
                Inner::ChildBuffer(ref mut buffer) => buffer.insert(key, keyinfo, msg, msg_action),
            })
    }

    pub(super) fn insert_msg_buffer<I, M, X>(
        &mut self,
        msg_buffer: I,
        msg_action: M,
        dml: &X,
        d_id: DatasetId,
    ) -> isize
    where
        I: IntoIterator<Item = (CowBytes, (KeyInfo, SlicedCowBytes))>,
        M: MessageAction,
        N: ObjectReference,
        X: Dml<Object = Node<N>, ObjectRef = N>,
    {
        let size_delta = self.ensure_unpacked();
        size_delta
            + (match self.0 {
                PackedLeaf(_) => unreachable!(),
                Leaf(ref mut leaf) => leaf.insert_msg_buffer(msg_buffer, msg_action),
                Internal(ref mut internal) => internal.insert_msg_buffer(msg_buffer, msg_action),
                MemLeaf(ref mut nvmleaf) => nvmleaf.insert_msg_buffer(msg_buffer, msg_action),
                DisjointInternal(ref mut nvminternal) => {
                    // This might take some time and fills the cache considerably.
                    let mut size_delta = 0;
                    for (k, (kinfo, v)) in msg_buffer {
                        let idx = nvminternal.idx(&k);
                        let link = nvminternal.get_mut(&k);
                        let mut buffer_node =
                            dml.get_mut(link.buffer_mut().get_mut(), d_id).unwrap();
                        let delta = buffer_node.insert(
                            k,
                            v,
                            msg_action.clone(),
                            kinfo.storage_preference,
                            dml,
                            d_id,
                        );
                        nvminternal.after_insert_size_delta(idx, delta);
                        size_delta += delta;
                    }
                    size_delta
                }
                Inner::ChildBuffer(_) => todo!(),
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
        self.ensure_unpacked();
        match self.0 {
            // FIXME: see above
            PackedLeaf(_) => unreachable!(),
            Leaf(ref mut leaf) => ApplyResult::Leaf(leaf.apply(key, pref)),
            Internal(ref mut internal) => {
                ApplyResult::NextNode(internal.apply_with_info(key, pref))
            }
            MemLeaf(ref mut nvmleaf) => ApplyResult::NVMLeaf(nvmleaf.apply(key, pref)),
            DisjointInternal(ref mut nvminternal) => {
                ApplyResult::NextNode(nvminternal.apply_with_info(key, pref))
            }
            Inner::ChildBuffer(ref mut buffer) => {
                buffer.apply_with_info(key, pref);
                ApplyResult::NVMLeaf(None)
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
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref mut internal) => Some(Box::new(
                internal
                    .iter_mut()
                    .map(|child| child.node_pointer.get_mut()),
            )),
            MemLeaf(_) => None,
            DisjointInternal(ref mut nvminternal) => Some(Box::new(
                nvminternal
                    .iter_mut()
                    .flat_map(|child| child.iter_mut().map(|p| p.get_mut())),
            )),
            // NOTE: This returns none as it is not necessarily harmful to write
            // it back as no consistency constraints have to be met.
            Inner::ChildBuffer(_) => None,
        }
    }

    pub(super) fn child_pointer_iter(&self) -> Option<Box<dyn Iterator<Item = &RwLock<N>> + '_>>
    where
        N: ObjectReference,
    {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref internal) => {
                Some(Box::new(internal.iter().map(|child| &child.node_pointer)))
            }
            MemLeaf(_) => None,
            DisjointInternal(ref nvminternal) => {
                Some(Box::new(nvminternal.iter().map(|link| link.ptr())))
            }
            Inner::ChildBuffer(_) => todo!(),
        }
    }

    pub(super) fn drain_children(&mut self) -> Option<ChildrenObjects<'_, N>>
    where
        N: ObjectReference,
    {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref mut internal) => Some(ChildrenObjects::ChildBuffer(Box::new(
                internal.drain_children(),
            ))),
            MemLeaf(_) => None,
            DisjointInternal(ref mut nvminternal) => Some(ChildrenObjects::NVMChildBuffer(
                Box::new(nvminternal.drain_children()),
            )),
            Inner::ChildBuffer(_) => unreachable!(),
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
            PackedLeaf(_) => unreachable!(),
            Leaf(ref mut leaf) => {
                let (node, pivot_key, size_delta, pk) =
                    leaf.split(min_size.unwrap(), max_size.unwrap());
                (Node(Leaf(node)), pivot_key, size_delta, pk)
            }
            Internal(ref mut internal) => {
                debug_assert!(
                    internal.fanout() >= 2 * MIN_FANOUT,
                    "internal split failed due to low fanout: {}, size: {}, actual_size: {:?}",
                    internal.fanout(),
                    internal.size(),
                    internal.actual_size()
                );
                let (node, pivot_key, size_delta, pk) = internal.split();
                (Node(Internal(node)), pivot_key, size_delta, pk)
            }
            MemLeaf(ref mut nvmleaf) => {
                let (node, pivot_key, size_delta, pk) =
                    nvmleaf.split(min_size.unwrap(), max_size.unwrap());
                (Node(MemLeaf(node)), pivot_key, size_delta, pk)
            }
            DisjointInternal(ref mut nvminternal) => {
                debug_assert!(
                    nvminternal.fanout() >= 2 * MIN_FANOUT,
                    "internal split failed due to low fanout: {}, size: {}, actual_size: {:?}",
                    nvminternal.fanout(),
                    nvminternal.size(),
                    nvminternal.actual_size()
                );
                let (node, pivot_key, size_delta, pk) = nvminternal.split();
                (Node(DisjointInternal(node)), pivot_key, size_delta, pk)
            }
            Inner::ChildBuffer(_) => unreachable!(),
        }
    }

    pub(super) fn merge(&mut self, right_sibling: &mut Self, pivot_key: CowBytes) -> isize {
        self.ensure_unpacked();
        right_sibling.ensure_unpacked();
        match (&mut self.0, &mut right_sibling.0) {
            (&mut Leaf(ref mut left), &mut Leaf(ref mut right)) => left.merge(right),
            (&mut Internal(ref mut left), &mut Internal(ref mut right)) => {
                left.merge(right, pivot_key)
            }
            (&mut MemLeaf(ref mut left), &mut MemLeaf(ref mut right)) => left.merge(right),
            (&mut DisjointInternal(ref mut left), &mut DisjointInternal(ref mut right)) => {
                left.merge(right, pivot_key)
            }
            _ => unreachable!(),
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
            (&mut Leaf(ref mut left), &mut Leaf(ref mut right)) => {
                left.rebalance(right, min_size.unwrap(), max_size.unwrap())
            }
            _ => unreachable!(),
        }
    }

    pub(super) fn nvmleaf_rebalance(
        &mut self,
        right_sibling: &mut Self,
        storage_map: &StorageMap,
    ) -> NVMFillUpResult {
        self.ensure_unpacked();
        right_sibling.ensure_unpacked();
        let min_size = storage_map.min_size(self);
        let max_size = storage_map.min_size(self);
        match (&mut self.0, &mut right_sibling.0) {
            (&mut MemLeaf(ref mut left), &mut MemLeaf(ref mut right)) => {
                left.rebalance(right, min_size.unwrap(), max_size.unwrap())
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
    Internal {
        level: u32,
        storage: StoragePreference,
        system_storage: StoragePreference,
        children: Vec<ChildInfo>,
    },
    Leaf {
        level: u32,
        storage: StoragePreference,
        system_storage: StoragePreference,
        entry_count: usize,
    },
    Packed {
        entry_count: u32,
        range: Vec<ByteString>,
    },
    NVMLeaf {
        level: u32,
        storage: StoragePreference,
        system_storage: StoragePreference,
        entry_count: usize,
    },
    NVMInternal {
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
            Inner::Internal(int) => NodeInfo::Internal {
                storage: self.correct_preference(),
                system_storage: self.system_storage_preference(),
                level: self.level(),
                children: {
                    int.iter_with_bounds()
                        .map(|(maybe_left, child_buf, maybe_right)| {
                            let (child, storage_preference, pivot_key) = {
                                let mut np = child_buf.node_pointer.write();
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
            Inner::Leaf(leaf) => NodeInfo::Leaf {
                storage: self.correct_preference(),
                system_storage: self.system_storage_preference(),
                level: self.level(),
                entry_count: leaf.entries().len(),
            },
            Inner::PackedLeaf(packed) => {
                let len = packed.entry_count();
                NodeInfo::Packed {
                    entry_count: len,
                    range: if len == 0 {
                        Vec::new()
                    } else {
                        [
                            packed.get_full_by_index(0),
                            packed.get_full_by_index(len - 1),
                        ]
                        .iter()
                        .filter_map(|opt| {
                            if let Some((key, _)) = opt {
                                Some(ByteString(key.to_vec()))
                            } else {
                                None
                            }
                        })
                        .collect()
                    },
                }
            }
            Inner::MemLeaf(ref nvmleaf) => NodeInfo::NVMLeaf {
                storage: self.correct_preference(),
                system_storage: self.system_storage_preference(),
                level: self.level(),
                entry_count: nvmleaf.len(),
            },
            Inner::DisjointInternal(ref nvminternal) => NodeInfo::NVMInternal {
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
            Inner::ChildBuffer(_) => unreachable!(),
        }
    }
}
