use parking_lot::RwLock;

use crate::{
    cow_bytes::CowBytes,
    data_management::{Dml, HasStoragePreference, ObjectReference},
    database::DatasetId,
    size::{Size, StaticSize},
};

use super::{internal::TakeChildBuffer, disjoint_internal::NVMTakeChildBuffer, Node};

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

    pub(super) fn prepare_merge<X>(
        &mut self,
        dml: &X,
        d_id: DatasetId,
    ) -> PrepareChildBufferMerge<N, X>
    where
        N: ObjectReference,
        X: Dml<Object = Node<N>, ObjectRef = N>,
    {
        match self {
            TakeChildBufferWrapper::TakeChildBuffer(obj) => {
                PrepareChildBufferMerge::Block(obj.prepare_merge())
            }
            TakeChildBufferWrapper::NVMTakeChildBuffer(obj) => {
                PrepareChildBufferMerge::Memory(obj.load_and_prepare_merge(dml, d_id))
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
use super::disjoint_internal::PrepareMergeChild as Mem_PMC;

pub(super) enum PrepareChildBufferMerge<'a, N: 'static, X: Dml> {
    Block(Block_PMC<'a, N>),
    Memory(Mem_PMC<'a, N, X>),
}

impl<'a, N, X> PrepareChildBufferMerge<'a, N, X>
where
    X: Dml<Object = Node<N>, ObjectRef = N>,
    N: ObjectReference<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
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
    pub(super) fn is_right_sibling(&self) -> bool {
        match self {
            PrepareChildBufferMerge::Block(pmc) => pmc.is_right_sibling(),
            PrepareChildBufferMerge::Memory(pmc) => pmc.is_right_sibling(),
        }
    }

    pub(super) fn merge_children(self, dml: &X) -> MergeChildResult<N>
    where
        N: ObjectReference + HasStoragePreference,
    {
        match self {
            PrepareChildBufferMerge::Block(pmc) => pmc.merge_children(),
            PrepareChildBufferMerge::Memory(pmc) => pmc.merge_children(dml),
        }
    }

    pub(super) fn rebalanced(&mut self, new_pivot_key: CowBytes, dml: &X) -> isize
    where
        N: ObjectReference + HasStoragePreference,
    {
        match self {
            PrepareChildBufferMerge::Block(pmc) => pmc.rebalanced(new_pivot_key),
            PrepareChildBufferMerge::Memory(pmc) => pmc.rebalanced(new_pivot_key, |np, d_id| {
                dml.get_mut(np.get_mut(), d_id)
                    .expect("Node fetch in prepare merge rebalanced untreated")
            }),
        }
    }
}
