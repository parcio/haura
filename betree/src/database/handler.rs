use super::{
    dead_list_key, errors::*, AtomicStorageInfo, DatasetId, DeadListData, Generation, StorageInfo,
    TreeInner,
};
use crate::{
    allocator::{Action, SegmentAllocator, SegmentId, SEGMENT_SIZE_BYTES},
    atomic_option::AtomicOption,
    cow_bytes::SlicedCowBytes,
    data_management::{CopyOnWriteEvent, Dml, HasStoragePreference, ObjectReference},
    storage_pool::DiskOffset,
    tree::{DefaultMessageAction, Node, Tree, TreeLayer},
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
        atomic::{AtomicU64, Ordering},
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
pub struct Handler<OR: ObjectReference> {
    pub(crate) root_tree_inner: AtomicOption<Arc<TreeInner<OR, DatasetId, DefaultMessageAction>>>,
    pub(crate) root_tree_snapshot: RwLock<Option<TreeInner<OR, DatasetId, DefaultMessageAction>>>,
    pub(crate) current_generation: SeqLock<Generation>,
    // Free Space counted as blocks
    pub(crate) free_space: HashMap<(u8, u16), AtomicStorageInfo>,
    pub(crate) free_space_tier: Vec<AtomicStorageInfo>,
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

impl<OR: ObjectReference + HasStoragePreference> Handler<OR> {
    fn current_root_tree<'a, X>(&'a self, dmu: &'a X) -> impl TreeLayer<DefaultMessageAction> + 'a
    where
        X: Dml<
            Object = Node<OR>,
            ObjectRef = OR,
            ObjectPointer = OR::ObjectPointer,
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
    ) -> Option<impl TreeLayer<DefaultMessageAction> + 'a>
    where
        X: Dml<
            Object = Node<OR>,
            ObjectRef = OR,
            ObjectPointer = OR::ObjectPointer,
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

pub(super) fn segment_id_to_key(segment_id: SegmentId) -> [u8; 9] {
    let mut key = [0; 9];
    BigEndian::write_u64(&mut key[1..], segment_id.0);
    key
}

impl<OR: ObjectReference + HasStoragePreference> Handler<OR> {
    pub fn current_generation(&self) -> Generation {
        self.current_generation.read()
    }

    pub fn update_allocation_bitmap<X>(
        &self,
        offset: DiskOffset,
        size: Block<u32>,
        action: Action,
        dmu: &X,
    ) -> Result<()>
    where
        X: Dml<
            Object = Node<OR>,
            ObjectRef = OR,
            ObjectPointer = OR::ObjectPointer,
            Info = DatasetId,
        >,
    {
        self.allocations.fetch_add(1, Ordering::Release);
        let key = segment_id_to_key(SegmentId::get(offset));
        let msg = update_allocation_bitmap_msg(offset, size, action);
        // NOTE: We perform double the amount of atomics here than necessary, but we do this for now to avoid reiteration
        match action {
            Action::Deallocate => {
                self.free_space
                    .get(&(offset.storage_class(), offset.disk_id()))
                    .expect("Could not find disk id in storage class")
                    .free
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
                self.free_space_tier[offset.storage_class() as usize]
                    .free
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
            }
            Action::Allocate => {
                self.free_space
                    .get(&(offset.storage_class(), offset.disk_id()))
                    .expect("Could not find disk id in storage class")
                    .free
                    .fetch_sub(size.as_u64(), Ordering::Relaxed);
                self.free_space_tier[offset.storage_class() as usize]
                    .free
                    .fetch_sub(size.as_u64(), Ordering::Relaxed);
            }
        };
        self.current_root_tree(dmu)
            .insert(&key[..], msg, StoragePreference::NONE)?;
        Ok(())
    }

    pub fn get_allocation_bitmap<X>(&self, id: SegmentId, dmu: &X) -> Result<SegmentAllocator>
    where
        X: Dml<
            Object = Node<OR>,
            ObjectRef = OR,
            ObjectPointer = OR::ObjectPointer,
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

    pub fn get_free_space(&self, class: u8, disk_id: u16) -> Option<StorageInfo> {
        self.free_space
            .get(&(class, disk_id))
            .map(|elem| elem.into())
    }

    pub fn get_free_space_tier(&self, class: u8) -> Option<StorageInfo> {
        self.free_space_tier
            .get(class as usize)
            .map(|elem| elem.into())
    }

    /// Marks blocks from removed objects to be removed if they are no longer needed.
    /// Checks for the existence of snapshots which included this data, if snapshots are found continue to hold this key as "dead" key.
    // copy on write is a bit of an unlucky name
    pub fn copy_on_write(
        &self,
        offset: DiskOffset,
        size: Block<u32>,
        generation: Generation,
        dataset_id: DatasetId,
    ) -> CopyOnWriteEvent {
        if self
            .last_snapshot_generation
            .read()
            .get(&dataset_id)
            .cloned()
            < Some(generation)
        {
            // Deallocate
            let key = &segment_id_to_key(SegmentId::get(offset)) as &[_];
            log::debug!(
                "Marked a block range {{ offset: {:?}, size: {:?} }} for deallocation",
                offset,
                size
            );
            let msg = update_allocation_bitmap_msg(offset, size, Action::Deallocate);
            // NOTE: Update free size on both positions
            self.free_space
                .get(&(offset.storage_class(), offset.disk_id()))
                .expect("Could not fetch disk id from storage class")
                .free
                .fetch_add(size.as_u64(), Ordering::Relaxed);
            self.free_space_tier[offset.storage_class() as usize]
                .free
                .fetch_add(size.as_u64(), Ordering::Relaxed);
            self.delayed_messages.lock().push((key.into(), msg));
            CopyOnWriteEvent::Removed
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
            CopyOnWriteEvent::Preserved
        }
    }
}
