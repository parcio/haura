//! Implementation of tree structures.
use self::{
    derivate_ref::DerivateRefNVM,
    node::{ApplyResult, GetResult, PivotGetMutResult, PivotGetResult},
};
use super::{
    errors::*,
    layer::{ErasedTreeSync, TreeLayer},
    PivotKey, StorageKind,
};
use crate::{
    cache::AddSize,
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::{Dml, HasStoragePreference, ObjectReference},
    database::DatasetId,
    range_validation::is_inclusive_non_empty,
    size::StaticSize,
    storage_pool::{StoragePoolLayer, NUM_STORAGE_CLASSES},
    tree::MessageAction,
    StoragePreference,
};
use leaf::FillUpResult;
use owning_ref::OwningRef;
use parking_lot::{RwLock, RwLockWriteGuard};
use std::{borrow::Borrow, marker::PhantomData, mem, ops::RangeBounds};

use internal::TakeChildBuffer;

/// Additional information for a single entry. Concerns meta information like
/// the desired storage level of a key.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive(check_bytes)]
pub struct KeyInfo {
    storage_preference: StoragePreference,
}

impl Default for KeyInfo {
    fn default() -> Self {
        Self {
            storage_preference: StoragePreference::NONE,
        }
    }
}

impl StaticSize for KeyInfo {
    fn static_size() -> usize {
        mem::size_of::<StoragePreference>()
    }
}

impl KeyInfo {
    pub(crate) fn merge_with_upper(self, upper: KeyInfo) -> KeyInfo {
        KeyInfo {
            storage_preference: StoragePreference::choose_faster(
                self.storage_preference,
                upper.storage_preference,
            ),
        }
    }

    pub(crate) fn storage_preference(&self) -> &StoragePreference {
        &self.storage_preference
    }
}

// The ratio determining the MIN_FLUSH_SIZE = MAX_SIZE / MIN_FLUSH_RATIO
const MIN_FLUSH_RATIO: usize = 16;
const MIN_FANOUT: usize = 2;
pub(crate) const MAX_MESSAGE_SIZE: usize = 512 * 1024;

/// The actual tree type.
pub struct Tree<X: Dml, M, I: Borrow<Inner<X::ObjectRef, M>>> {
    inner: I,
    dml: X,
    evict: bool,
    marker: PhantomData<M>,
    storage_preference: StoragePreference,
    /// A 1-to-1 map of each storage class to the desired data representation.
    storage_map: StorageMap,
}

#[derive(Clone, Debug)]
pub(crate) struct StorageMap {
    map: [StorageKind; NUM_STORAGE_CLASSES],
    default: StorageKind,
}

impl StorageMap {
    pub fn get(&self, pref: StoragePreference) -> StorageKind {
        self.map
            .get(pref.as_u8() as usize)
            .cloned()
            .unwrap_or(self.default)
    }
}

impl<X: Clone + Dml, M, I: Clone + Borrow<Inner<X::ObjectRef, M>>> Clone for Tree<X, M, I> {
    fn clone(&self) -> Self {
        Tree {
            inner: self.inner.clone(),
            dml: self.dml.clone(),
            evict: self.evict,
            marker: PhantomData,
            storage_preference: self.storage_preference,
            storage_map: self.storage_map.clone(),
        }
    }
}

/// The inner tree type that does not contain the DML object.
pub struct Inner<R, M> {
    root_node: RwLock<R>,
    tree_id: Option<DatasetId>,
    msg_action: M,
}

impl<R, M> Inner<R, M> {
    fn new(tree_id: DatasetId, root_node: R, msg_action: M) -> Self {
        Inner {
            tree_id: Some(tree_id),
            root_node: RwLock::new(root_node),
            msg_action,
        }
    }

    /// Returns a new read-only tree.
    pub fn new_ro(root_node: R, msg_action: M) -> Self {
        Inner {
            tree_id: None,
            root_node: RwLock::new(root_node),
            msg_action,
        }
    }

    /// Sets a new root node.
    pub fn update_root_node(&mut self, root_node: R) {
        *self.root_node.get_mut() = root_node;
    }
}

impl<X, R, M, I> Tree<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectReference<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, M>> + From<Inner<X::ObjectRef, M>>,
{
    /// Returns a new, empty tree.
    pub fn empty_tree(
        tree_id: DatasetId,
        msg_action: M,
        dml: X,
        storage_preference: StoragePreference,
    ) -> Self {
        let root_node = dml.insert(Node::empty_leaf(), tree_id, PivotKey::Root(tree_id));
        Tree::new(root_node, tree_id, msg_action, dml, storage_preference)
    }

    /// Opens a tree identified by the given root node.
    pub fn open(
        tree_id: DatasetId,
        root_node_ptr: X::ObjectPointer,
        msg_action: M,
        dml: X,
        storage_preference: StoragePreference,
    ) -> Self {
        Tree::new(
            X::root_ref_from_ptr(root_node_ptr),
            tree_id,
            msg_action,
            dml,
            storage_preference,
        )
    }

    fn new(
        root_node: R,
        tree_id: DatasetId,
        msg_action: M,
        dml: X,
        storage_preference: StoragePreference,
    ) -> Self {
        Tree {
            inner: I::from(Inner::new(tree_id, root_node, msg_action)),
            storage_map: StorageMap {
                map: dml.spl().storage_kind_map(),
                default: dml.spl().storage_kind_map()[dml.spl().default_storage_class() as usize],
            },
            dml,
            evict: true,
            marker: PhantomData,
            storage_preference,
        }
    }
}

impl<X, R, M, I> Tree<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectReference<ObjectPointer = X::ObjectPointer>,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, M>>,
{
    /// Returns the inner of the tree.
    pub fn inner(&self) -> &I {
        &self.inner
    }

    /// Returns a new tree with the given inner.
    pub fn from_inner(
        inner: I,
        dml: X,
        evict: bool,
        storage_preference: StoragePreference,
    ) -> Self {
        Tree {
            inner,
            storage_map: StorageMap {
                map: dml.spl().storage_kind_map(),
                default: dml.spl().storage_kind_map()[dml.spl().default_storage_class() as usize],
            },
            dml,
            evict,
            marker: PhantomData,
            storage_preference,
        }
    }

    /// Returns the DML.
    pub fn dmu(&self) -> &X {
        &self.dml
    }

    /// Locks the root node.
    /// Returns `None` if the root node is modified.
    pub fn try_lock_root(&self) -> Option<OwningRef<RwLockWriteGuard<R>, X::ObjectPointer>> {
        let guard = self.inner.borrow().root_node.write();
        OwningRef::new(guard)
            .try_map(|guard| guard.get_unmodified().ok_or(()))
            .ok()
    }
}

impl<X, R, M, I> Tree<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectReference<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, M>>,
{
    fn tree_id(&self) -> DatasetId {
        self.inner
            .borrow()
            .tree_id
            .expect("Mutating operations called on read only tree")
    }

    fn msg_action(&self) -> &M {
        &self.inner.borrow().msg_action
    }

    fn get_mut_root_node(&self) -> Result<X::CacheValueRefMut, Error> {
        if let Some(node) = self.dml.try_get_mut(&self.inner.borrow().root_node.read()) {
            return Ok(node);
        }
        Ok(self
            .dml
            .get_mut(&mut self.inner.borrow().root_node.write(), self.tree_id())?)
    }

    fn get_root_node(&self) -> Result<X::CacheValueRef, Error> {
        self.get_node(&self.inner.borrow().root_node)
    }

    fn get_node(&self, np_ref: &RwLock<X::ObjectRef>) -> Result<X::CacheValueRef, Error> {
        if let Some(node) = self.dml.try_get(&np_ref.read()) {
            return Ok(node);
        }
        Ok(self.dml.get(&mut np_ref.write())?)
    }

    pub(crate) fn get_node_pivot(
        &self,
        pivot: &PivotKey,
    ) -> Result<Option<X::CacheValueRef>, Error> {
        let mut node = self.get_root_node()?;
        Ok(loop {
            let next_node = match node.pivot_get(pivot) {
                Some(PivotGetResult::Target(Some(np))) => break Some(self.get_node(np)?),
                Some(PivotGetResult::Target(None)) => break Some(node),
                Some(PivotGetResult::NextNode(np)) => self.get_node(np)?,
                None => break None,
            };
            node = next_node;
        })
    }

    pub(crate) fn get_mut_node_pivot(
        &self,
        pivot: &PivotKey,
    ) -> Result<Option<X::CacheValueRefMut>, Error> {
        let mut node = self.get_mut_root_node()?;
        Ok(loop {
            let next_node = match node.pivot_get_mut(pivot) {
                Some(PivotGetMutResult::Target(Some(np))) => {
                    break Some(self.get_mut_node_mut(np)?)
                }
                Some(PivotGetMutResult::Target(None)) => break Some(node),
                Some(PivotGetMutResult::NextNode(np)) => self.get_mut_node_mut(np)?,
                None => break None,
            };
            node = next_node;
        })
    }

    fn try_get_mut_node(&self, np_ref: &mut RwLock<X::ObjectRef>) -> Option<X::CacheValueRefMut> {
        self.dml.try_get_mut(np_ref.get_mut())
    }

    fn get_mut_node_mut(&self, np_ref: &mut X::ObjectRef) -> Result<X::CacheValueRefMut, Error> {
        if let Some(node) = self.dml.try_get_mut(np_ref) {
            return Ok(node);
        }
        Ok(self.dml.get_mut(np_ref, self.tree_id())?)
    }

    fn get_mut_node(
        &self,
        np_ref: &mut RwLock<X::ObjectRef>,
    ) -> Result<X::CacheValueRefMut, Error> {
        self.get_mut_node_mut(np_ref.get_mut())
    }

    /*fn walk_tree(
        &self,
        mut node: X::CacheValueRefMut,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<(), Error> {
        // T0D0 rebalance/merge nodes
        loop {
            let (size_delta, next_node) = {
                let level = node.level();
                let (size_delta, children) = node.range_delete(start, end);
                let (l_np, r_np, dead) = match children {
                    None => return Ok(()),
                    Some((l_np, r_np, dead)) => (l_np, r_np, dead),
                };
                if level == 1 {
                    // is leaf, has no references
                    for np in dead {
                        self.dml.remove(np);
                    }
                } else {
                    // is internal, has children
                    for np in dead {
                        self.remove_subtree(np)?;
                    }
                }
                if let Some(np) = r_np {
                    self.walk_tree(self.get_mut_node_mut(np)?, start, end)?;
                }
                (size_delta, self.get_mut_node_mut(l_np)?)
            };
            node.add_size(size_delta);
            node = next_node;
        }
    }

    fn remove_subtree(&self, np: X::ObjectRef) -> Result<(), Error> {
        let mut nps = vec![np];
        while let Some(np) = nps.pop() {
            {
                let mut node = self.dml.get_and_remove(np)?;
                let level = node.level();
                let result = node.drain_children();
                if let Some(child_nps) = result {
                    if level == 1 {
                        // No need to fetch the leaves
                        for np in child_nps {
                            self.dml.remove(np);
                        }
                    } else {
                        nps.extend(child_nps);
                    }
                }
            }
        }
        Ok(())
    }*/

    #[allow(missing_docs)]
    #[cfg(feature = "internal-api")]
    pub fn tree_dump(&self) -> Result<NodeInfo, Error>
    where
        X::ObjectRef: HasStoragePreference,
    {
        let root = self.get_root_node()?;

        Ok(root.node_info(&self.dml))
    }

    //    pub fn is_modified(&mut self) -> bool {
    //        self.inner.borrow_mut().root_node.is_modified()
    //    }

    pub(crate) fn get_with_info<K: Borrow<[u8]>>(
        &self,
        key: K,
    ) -> Result<Option<(KeyInfo, SlicedCowBytes)>, Error> {
        let key = key.borrow();
        let mut msgs = Vec::new();
        let mut node = self.get_root_node()?;
        let data = loop {
            let next_node = match node.get(key, &mut msgs) {
                GetResult::NextNode(np) => self.get_node(np)?,
                GetResult::Data(data) => break data,
            };
            node = next_node;
        };
        match data {
            None => {
                let mut tmp = None;
                let mut info = None;
                for (keyinfo, msg) in msgs.into_iter().rev() {
                    info = Some(keyinfo);
                    self.msg_action().apply(key, &msg, &mut tmp);
                }
                drop(node);
                if self.evict {
                    self.dml.evict()?;
                }
                Ok(tmp.map(|data| (info.unwrap(), data)))
            }
            Some((info, data)) => {
                let mut tmp = Some(data);
                for (_keyinfo, msg) in msgs.into_iter().rev() {
                    self.msg_action().apply(key, &msg, &mut tmp);
                }
                drop(node);
                if self.evict {
                    self.dml.evict()?;
                }
                Ok(tmp.map(|data| (info, data)))
            }
        }
    }

    /// "Piercing" update, with insertion logic of a B-Tree.
    /// To keep data sanity only modification of the key information is allowed
    /// and all key infos on the paths will be updated to reflect this change.
    pub(crate) fn apply_with_info<K: Borrow<[u8]>>(
        &self,
        key: K,
        pref: StoragePreference,
    ) -> Result<Option<KeyInfo>, Error> {
        let key = key.borrow();
        let mut node = self.get_mut_root_node()?;
        // Iterate to leaf
        let res = Ok(loop {
            let next_node = match node.apply_with_info(key, pref) {
                ApplyResult::NextNode(np) => self.get_mut_node_mut(np)?,
                ApplyResult::Leaf(info) => break info,
                // ApplyResult::NextNode { child, buffer } => {
                //     let mut buffer = self.get_mut_node_mut(buffer)?;
                //     buffer.apply_with_info(key, pref);
                //     self.get_mut_node_mut(child)?
                // }
            };
            node = next_node;
        });
        res
    }
}

// impl<X, R, M> Tree<X, M, Arc<Inner<X::ObjectRef, X::Info, M>>>
// where
//     X: Dml<Object = Node<Message<M>, R>, ObjectRef = R>,
//     R: ObjectRef,
//     M: MessageAction,
// {
//     pub fn is_modified(&mut self) -> Option<bool> {
// Arc::get_mut(&mut self.inner).map(|inner|
// inner.root_node.is_modified())
//     }
// }

impl<X, R, M, I> TreeLayer<M> for Tree<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectReference<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, M>>,
{
    fn get<K: Borrow<[u8]>>(&self, key: K) -> Result<Option<SlicedCowBytes>, Error> {
        self.get_with_info(key)
            .map(|res| res.map(|(_info, data)| data))
    }

    // NOTE: Our specific type actually implements a somewhat optimized variant
    // of the usual b-epsilon tree insertion, we iterate as far down as we can
    // on "Modified" nodes which do not contain the modified key already. This way we ensure that:
    // 1. Recombination of messages are minimized.
    // 2. Expensive flush operations are delayed. (Structure changes)
    fn insert<K>(
        &self,
        key: K,
        msg: SlicedCowBytes,
        storage_preference: StoragePreference,
    ) -> Result<(), Error>
    where
        K: Borrow<[u8]> + Into<CowBytes>,
    {
        if key.borrow().is_empty() {
            return Err(Error::EmptyKey);
        }
        let mut parent = None;
        let mut node = {
            let mut node = self.get_mut_root_node()?;
            loop {
                if self.storage_map.node_is_too_large(&mut node) {
                    break node;
                }
                match DerivateRefNVM::try_new(node, |node| node.try_walk(key.borrow())) {
                    Ok(mut child_buffer) => {
                        let maybe_child = self.try_get_mut_node(child_buffer.child_pointer_mut());
                        if let Some(child) = maybe_child {
                            node = child;
                            parent = Some(child_buffer);
                        } else {
                            break child_buffer.into_owner();
                        }
                    }
                    Err(node) => {
                        break node;
                    }
                };
            }
        };

        let op_preference = storage_preference.or(self.storage_preference);
        let added_size = node.insert(key, msg, self.msg_action(), op_preference);
        node.add_size(added_size);

        if parent.is_none() && node.root_needs_merge() {
            // TODO Merge, this is not implemented with the 'rebalance_tree'
            // method. Since the root has a fanout of 1 at this point, merge all
            // messages downwards and set leaf as root?
            unimplemented!();
        }

        self.rebalance_tree(node, parent)?;

        // All non-root trees will start the eviction process.
        if self.evict {
            self.dml.evict()?;
        }

        Ok(())
    }

    fn depth(&self) -> Result<u32, Error> {
        Ok(self.get_root_node()?.level() + 1)
    }

    type Pointer = X::ObjectPointer;

    type Range = RangeIterator<X, M, I>;

    fn range<K, T>(&self, range: T) -> Result<Self::Range, Error>
    where
        T: RangeBounds<K>,
        K: Borrow<[u8]> + Into<CowBytes>,
        Self: Clone,
    {
        if !is_inclusive_non_empty(&range) {
            return Err(Error::InvalidRange);
        }
        Ok(RangeIterator::new(range, self.clone()))
    }

    fn sync(&self) -> Result<Self::Pointer, Error> {
        trace!("sync: Enter");
        let obj_ptr = self
            .dml
            .write_back(|| self.inner.borrow().root_node.write())?;
        trace!("sync: Finished write_back");
        Ok(obj_ptr)
    }
}

impl<X, R, M, I> ErasedTreeSync for Tree<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectReference<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<R, M>>,
{
    type Pointer = X::ObjectPointer;
    type ObjectRef = R;
    fn erased_sync(&self) -> Result<Self::Pointer, Error> {
        TreeLayer::sync(self)
    }
    fn erased_try_lock_root(
        &self,
    ) -> Option<OwningRef<RwLockWriteGuard<Self::ObjectRef>, Self::Pointer>> {
        self.try_lock_root()
    }
}

mod derivate_ref;
mod flush;
mod internal;
mod leaf;
mod node;
mod range;
mod split;

#[cfg(feature = "internal-api")]
pub use self::node::NodeInfo;

pub use self::{node::Node, range::RangeIterator};
