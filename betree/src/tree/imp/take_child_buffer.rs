use parking_lot::RwLock;

use crate::{
    cow_bytes::CowBytes,
    data_management::{HasStoragePreference, ObjectReference},
    size::{Size, StaticSize},
};

use super::{internal::TakeChildBuffer, copyless_internal::NVMTakeChildBuffer};

pub(super) enum TakeChildBufferWrapper<'a, N: 'a + 'static> {
    TakeChildBuffer(TakeChildBuffer<'a, N>),
    NVMTakeChildBuffer(NVMTakeChildBuffer<'a, N>),
}

impl<'a, N: Size + HasStoragePreference + ObjectReference + 'a + 'static>
    TakeChildBufferWrapper<'a, N>
{
    pub fn child_pointer_mut(&mut self) -> &mut RwLock<N> {
        match self {
            TakeChildBufferWrapper::TakeChildBuffer(obj) => obj.node_pointer_mut(),
            TakeChildBufferWrapper::NVMTakeChildBuffer(obj) => obj.child_pointer_mut(),
        }
    }
}

impl<'a, N> TakeChildBufferWrapper<'a, N>
where
    N: StaticSize,
{
    pub(super) fn size(&self) -> usize {
        match self {
            TakeChildBufferWrapper::TakeChildBuffer(obj) => obj.size(),
            TakeChildBufferWrapper::NVMTakeChildBuffer(obj) => obj.size(),
        }
    }

    pub(super) fn prepare_merge(
        &mut self,
    ) -> PrepareChildBufferMerge<N>
    where
        N: ObjectReference,
    {
        match self {
            TakeChildBufferWrapper::TakeChildBuffer(obj) => {
                PrepareChildBufferMerge::Block(obj.prepare_merge())
            }
            TakeChildBufferWrapper::NVMTakeChildBuffer(obj) => {
                PrepareChildBufferMerge::Memory(obj.prepare_merge())
            }
        }
    }
}

pub(super) struct MergeChildResult<NP> {
    pub(super) pivot_key: CowBytes,
    pub(super) old_np: NP,
    pub(super) size_delta: isize,
}

use super::internal::PrepareMergeChild as Block_PMC;
use super::copyless_internal::PrepareMergeChild as Mem_PMC;

pub(super) enum PrepareChildBufferMerge<'a, N: 'static> {
    Block(Block_PMC<'a, N>),
    Memory(Mem_PMC<'a, N>),
}

impl<'a, N> PrepareChildBufferMerge<'a, N>
where
    N: ObjectReference + HasStoragePreference,
{
    pub(super) fn sibling_node_pointer(&mut self) -> &mut RwLock<N>
    where
        N: ObjectReference,
    {
        match self {
            PrepareChildBufferMerge::Block(pmc) => pmc.sibling_node_pointer(),
            PrepareChildBufferMerge::Memory(pmc) => pmc.sibling_node_pointer(),
        }
    }

    /// Wether the *sibling* of *child* is the right to child or not.
    pub(super) fn is_right_sibling(&self) -> bool {
        match self {
            PrepareChildBufferMerge::Block(pmc) => pmc.is_right_sibling(),
            PrepareChildBufferMerge::Memory(pmc) => pmc.is_right_sibling(),
        }
    }

    pub(super) fn merge_children(self) -> MergeChildResult<Box<dyn Iterator<Item = N>>>
    where
        N: ObjectReference + HasStoragePreference,
    {
        match self {
            PrepareChildBufferMerge::Block(pmc) => pmc.merge_children(),
            PrepareChildBufferMerge::Memory(pmc) => pmc.merge_children(),
        }
    }

    pub(super) fn rebalanced(&mut self, new_pivot_key: CowBytes) -> isize
    where
        N: ObjectReference + HasStoragePreference,
    {
        match self {
            PrepareChildBufferMerge::Block(pmc) => pmc.rebalanced(new_pivot_key),
            PrepareChildBufferMerge::Memory(pmc) => pmc.rebalanced(new_pivot_key),
        }
    }
}
