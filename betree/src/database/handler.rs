use super::{
    dead_list_key, errors::*, DatasetId, DeadListData, Generation, Object, ObjectPointer,
    ObjectRef, TreeInner,
};
use crate::{
    allocator::{Action, SegmentAllocator, SegmentId, SEGMENT_SIZE_BYTES},
    atomic_option::AtomicOption,
    cow_bytes::SlicedCowBytes,
    data_management::{self, HandlerDml},
    storage_pool::DiskOffset,
    tree::{DefaultMessageAction, Tree, TreeBaseLayer},
    vdev::Block,
    StoragePreference,
};
use byteorder::{BigEndian, ByteOrder};
use owning_ref::OwningRef;
use parking_lot::{Mutex, RwLock};
use seqlock::SeqLock;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering, AtomicBool},
        Arc,
    },
};

/// Returns a message for updating the allocation bitmap.
pub fn update_allocation_bitmap_msg(
    offset: DiskOffset,
    size: Block<u32>,
    action: Action,
) -> SlicedCowBytes {
    let segment_offset = SegmentId::get_block_offset(offset);
    DefaultMessageAction::upsert_bits_msg(segment_offset, size.0, action.as_bool())
}

/// The database handler, holding management data for interactions
/// between the database and data management layers.
pub struct Handler {
    pub(crate) root_tree_inner:
        AtomicOption<Arc<TreeInner<ObjectRef, DatasetId, DefaultMessageAction>>>,
    pub(crate) root_tree_snapshot:
        RwLock<Option<TreeInner<ObjectRef, DatasetId, DefaultMessageAction>>>,
    pub(crate) current_generation: SeqLock<Generation>,
    // Free Space counted as blocks
    pub(crate) free_space: HashMap<(u8, u16), AtomicU64>,
    pub(crate) free_space_tier: Vec<AtomicU64>,
    pub(crate) delayed_messages: Mutex<Vec<(Box<[u8]>, SlicedCowBytes)>>,
    pub(crate) last_snapshot_generation: RwLock<HashMap<DatasetId, Generation>>,
    pub(crate) allocations: AtomicU64,
    pub(crate) old_root_allocation: SeqLock<Option<(DiskOffset, Block<u32>)>>,
}

// NOTE: Maybe use somehting like this in the future, for now we update another list on the side here
// pub struct FreeSpace {
//     pub(crate) disk: HashMap<(u8, u16), AtomicU64>,
//     pub(crate) class: Vec<AtomicU64>,
//     pub(crate) invalidated: AtomicBool,
// }

impl Handler {
    fn current_root_tree<'a, X>(
        &'a self,
        dmu: &'a X,
    ) -> impl TreeBaseLayer<DefaultMessageAction> + 'a
    where
        X: HandlerDml<
            Object = Object,
            ObjectRef = ObjectRef,
            ObjectPointer = ObjectPointer,
            Info = DatasetId,
        >,
    {
        Tree::from_inner(
            self.root_tree_inner.get().unwrap().as_ref(),
            dmu,
            false,
            super::ROOT_TREE_STORAGE_PREFERENCE,
        )
    }

    fn last_root_tree<'a, X>(
        &'a self,
        dmu: &'a X,
    ) -> Option<impl TreeBaseLayer<DefaultMessageAction> + 'a>
    where
        X: HandlerDml<
            Object = Object,
            ObjectRef = ObjectRef,
            ObjectPointer = ObjectPointer,
            Info = DatasetId,
        >,
    {
        OwningRef::new(self.root_tree_snapshot.read())
            .try_map(|lock| lock.as_ref().ok_or(()))
            .ok()
            .map(|inner| Tree::from_inner(inner, dmu, false, super::ROOT_TREE_STORAGE_PREFERENCE))
    }

    pub(super) fn bump_generation(&self) {
        self.current_generation.lock_write().0 += 1;
    }
}

impl data_management::HandlerTypes for Handler {
    type Generation = Generation;
    type Info = DatasetId;
}

pub(super) fn segment_id_to_key(segment_id: SegmentId) -> [u8; 9] {
    let mut key = [0; 9];
    BigEndian::write_u64(&mut key[1..], segment_id.0);
    key
}

impl data_management::Handler<ObjectRef> for Handler {
    type Object = Object;
    type Error = Error;

    fn current_generation(&self) -> Self::Generation {
        self.current_generation.read()
    }

    fn update_allocation_bitmap<X>(
        &self,
        offset: DiskOffset,
        size: Block<u32>,
        action: Action,
        dmu: &X,
    ) -> Result<()>
    where
        X: HandlerDml<
            Object = Object,
            ObjectRef = ObjectRef,
            ObjectPointer = ObjectPointer,
            Info = DatasetId,
        >,
    {
        self.allocations.fetch_add(1, Ordering::Release);
        let key = segment_id_to_key(SegmentId::get(offset));
        let msg = update_allocation_bitmap_msg(offset, size, action);
        // NOTE: We perform double the amount of atomics here than necessary, but we do this for now to avoid reiteration
        match action.clone() {
            Action::Deallocate => {
                self.free_space.get(&(offset.storage_class(), offset.disk_id())).expect("Could not find disk id in storage class").fetch_add(size.as_u64(), Ordering::Relaxed);
                self.free_space_tier[offset.storage_class() as usize].fetch_add(size.as_u64(), Ordering::Relaxed);
            },
            Action::Allocate => {
                self.free_space.get(&(offset.storage_class(), offset.disk_id())).expect("Could not find disk id in storage class").fetch_sub(size.as_u64(), Ordering::Relaxed);
                self.free_space_tier[offset.storage_class() as usize].fetch_sub(size.as_u64(), Ordering::Relaxed);
            },
        };
        self.current_root_tree(dmu)
            .insert(&key[..], msg, StoragePreference::NONE)?;
        Ok(())
    }

    fn get_allocation_bitmap<X>(&self, id: SegmentId, dmu: &X) -> Result<SegmentAllocator>
    where
        X: HandlerDml<
            Object = Object,
            ObjectRef = ObjectRef,
            ObjectPointer = ObjectPointer,
            Info = DatasetId,
        >,
    {
        let now = std::time::Instant::now();
        let mut bitmap = [0u8; SEGMENT_SIZE_BYTES];

        let key = segment_id_to_key(id);
        let segment = self.current_root_tree(dmu).get(&key[..])?;
        log::info!(
            "fetched {:?} bitmap elements",
            segment.as_ref().map(|s| s.len())
        );

        if let Some(segment) = segment {
            assert!(segment.len() <= SEGMENT_SIZE_BYTES);
            bitmap[..segment.len()].copy_from_slice(&segment[..]);
        }

        if let Some(tree) = self.last_root_tree(dmu) {
            if let Some(old_segment) = tree.get(&key[..])? {
                for (w, old) in bitmap.iter_mut().zip(old_segment.iter()) {
                    *w |= *old;
                }
            }
        }

        let mut allocator = SegmentAllocator::new(bitmap);

        if let Some((offset, size)) = self.old_root_allocation.read() {
            if SegmentId::get(offset) == id {
                allocator.allocate_at(size.as_u32(), SegmentId::get_block_offset(offset));
            }
        }

        log::info!("requested allocation bitmap, took {:?}", now.elapsed());

        Ok(allocator)
    }

    fn get_free_space(&self, class: u8, disk_id: u16) -> Option<Block<u64>> {
        self.free_space.get(&(class, disk_id)).map(|elem| Block(elem.load(Ordering::Relaxed)))
    }

    fn get_free_space_tier(&self, class: u8) -> Option<Block<u64>> {
        self.free_space_tier.get(class as usize).map(|elem| Block(elem.load(Ordering::Relaxed)))
    }

    /// Marks blocks from removed objects to be removed if they are no longer needed.
    /// Checks for the existence of snapshots which included this data, if snapshots are found continue to hold this key as "dead" key.
    // copy on write is a bit of an unlucky name
    fn copy_on_write(
        &self,
        offset: DiskOffset,
        size: Block<u32>,
        generation: Generation,
        dataset_id: DatasetId,
    ) {
        if self
            .last_snapshot_generation
            .read()
            .get(&dataset_id)
            .cloned()
            < Some(generation)
        {
            // Deallocate
            let key = &segment_id_to_key(SegmentId::get(offset)) as &[_];
            log::debug!("Marked a block range {{ offset: {:?}, size: {:?} }} for deallocation", offset, size);
            let msg = update_allocation_bitmap_msg(offset, size, Action::Deallocate);
            // NOTE: Update free size on both positions
            self.free_space.get(&(offset.storage_class(), offset.disk_id())).expect("Could not fetch disk id from storage class").fetch_add(size.as_u64(), Ordering::Relaxed);
            self.free_space_tier[offset.storage_class() as usize].fetch_add(size.as_u64(), Ordering::Relaxed);
            self.delayed_messages.lock().push((key.into(), msg));
        } else {
            // Add to dead list
            let key = &dead_list_key(dataset_id, self.current_generation.read(), offset) as &[_];

            let data = DeadListData {
                birth: generation,
                size,
            }
            .pack()
            .unwrap();

            let msg = DefaultMessageAction::insert_msg(&data);
            self.delayed_messages.lock().push((key.into(), msg));
        }
    }
}
