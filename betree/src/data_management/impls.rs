use super::{errors::*, DmlBase, Handler, HandlerTypes, HasStoragePreference, Object, PodType};
use crate::{
    allocator::{Action, SegmentAllocator, SegmentId},
    buffer::Buf,
    cache::{AddSize, Cache, ChangeKeyError, RemoveError},
    checksum::{Builder, Checksum, State},
    compression::{CompressionBuilder, DecompressionTag},
    size::{Size, SizeMut, StaticSize},
    storage_pool::{DiskOffset, StoragePoolLayer, NUM_STORAGE_CLASSES},
    vdev::{Block, BLOCK_SIZE},
    StoragePreference,
};
use futures::{executor::block_on, future::ok, prelude::*};
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde::{
    de::DeserializeOwned, ser::Error as SerError, Deserialize, Deserializer, Serialize, Serializer,
};
use stable_deref_trait::StableDeref;
use std::{
    collections::HashMap,
    mem::{replace, transmute, ManuallyDrop},
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    thread::yield_now,
};

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct ModifiedObjectId(u64, StoragePreference);

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum ObjectKey<G> {
    Unmodified { offset: DiskOffset, generation: G },
    Modified(ModifiedObjectId),
    InWriteback(ModifiedObjectId),
}

pub enum ObjectRef<P> {
    Unmodified(P),
    Modified(ModifiedObjectId),
    InWriteback(ModifiedObjectId),
}

impl<D, I, G> super::ObjectRef for ObjectRef<ObjectPointer<D, I, G>>
where
    D: 'static,
    I: 'static,
    G: Copy + 'static,
    ObjectPointer<D, I, G>: Serialize + DeserializeOwned + StaticSize,
{
    type ObjectPointer = ObjectPointer<D, I, G>;
    fn get_unmodified(&self) -> Option<&ObjectPointer<D, I, G>> {
        if let ObjectRef::Unmodified(ref p) = *self {
            Some(p)
        } else {
            None
        }
    }
}

impl<D, I, G: Copy> ObjectRef<ObjectPointer<D, I, G>> {
    fn as_key(&self) -> ObjectKey<G> {
        match *self {
            ObjectRef::Unmodified(ref ptr) => ObjectKey::Unmodified {
                offset: ptr.offset,
                generation: ptr.generation,
            },
            ObjectRef::Modified(mid) => ObjectKey::Modified(mid),
            ObjectRef::InWriteback(mid) => ObjectKey::InWriteback(mid),
        }
    }
}

impl<P: HasStoragePreference> HasStoragePreference for ObjectRef<P> {
    fn current_preference(&self) -> Option<StoragePreference> {
        Some(self.correct_preference())
    }

    fn recalculate(&self) -> StoragePreference {
        self.correct_preference()
    }

    fn correct_preference(&self) -> StoragePreference {
        match self {
            ObjectRef::Unmodified(p) => p.correct_preference(),
            ObjectRef::Modified(mid) | ObjectRef::InWriteback(mid) => mid.1,
        }
    }
}

impl<P: StaticSize> StaticSize for ObjectRef<P> {
    fn static_size() -> usize {
        P::static_size()
    }
}

impl<P: Serialize> Serialize for ObjectRef<P> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            ObjectRef::Modified(_) => Err(S::Error::custom(
                "ObjectRef: Tried to serialize a modified ObjectRef",
            )),
            ObjectRef::InWriteback(_) => Err(S::Error::custom(
                "ObjectRef: Tried to serialize a modified ObjectRef which is currently written back",
            )),
            ObjectRef::Unmodified(ref ptr) => ptr.serialize(serializer),
        }
    }
}

impl<'de, D, I, G: Copy> Deserialize<'de> for ObjectRef<ObjectPointer<D, I, G>>
where
    ObjectPointer<D, I, G>: Deserialize<'de>,
{
    fn deserialize<E>(deserializer: E) -> Result<Self, E::Error>
    where
        E: Deserializer<'de>,
    {
        ObjectPointer::<D, I, G>::deserialize(deserializer).map(ObjectRef::Unmodified)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectPointer<D, I, G> {
    decompression_tag: DecompressionTag,
    checksum: D,
    offset: DiskOffset,
    size: Block<u32>,
    info: I,
    generation: G,
}

impl<D, I, G> HasStoragePreference for ObjectPointer<D, I, G> {
    fn current_preference(&self) -> Option<StoragePreference> {
        Some(self.correct_preference())
    }

    fn recalculate(&self) -> StoragePreference {
        self.correct_preference()
    }

    fn correct_preference(&self) -> StoragePreference {
        StoragePreference::new(self.offset.storage_class())
    }
}

impl<D: StaticSize, I: StaticSize, G: StaticSize> StaticSize for ObjectPointer<D, I, G> {
    fn static_size() -> usize {
        <DecompressionTag as StaticSize>::static_size()
            + D::static_size()
            + I::static_size()
            + G::static_size()
            + <DiskOffset as StaticSize>::static_size()
            + 4
    }
}

impl<D, I, G: Copy> From<ObjectPointer<D, I, G>> for ObjectRef<ObjectPointer<D, I, G>> {
    fn from(ptr: ObjectPointer<D, I, G>) -> Self {
        ObjectRef::Unmodified(ptr)
    }
}

impl<D, I, G: Copy> ObjectPointer<D, I, G> {
    pub fn offset(&self) -> DiskOffset {
        self.offset
    }
    pub fn size(&self) -> Block<u32> {
        self.size
    }
    pub fn generation(&self) -> G {
        self.generation
    }
}

/// The Data Management Unit.
pub struct Dmu<E: 'static, SPL: StoragePoolLayer, H: 'static, I: 'static, G: 'static> {
    default_compression: Box<dyn CompressionBuilder>,
    // NOTE: Why was this included in the first place? Delayed Compression? Streaming Compression?
    // default_compression_state: C::CompressionState,
    default_storage_class: u8,
    default_checksum_builder: <SPL::Checksum as Checksum>::Builder,
    alloc_strategy: [[Option<u8>; NUM_STORAGE_CLASSES]; NUM_STORAGE_CLASSES],
    pool: SPL,
    cache: RwLock<E>,
    written_back: Mutex<HashMap<ModifiedObjectId, ObjectPointer<SPL::Checksum, I, G>>>,
    modified_info: Mutex<HashMap<ModifiedObjectId, I>>,
    handler: H,
    // NOTE: The semantic structure of this looks as this
    // Storage Pool Layers:
    //      Layer Disks:
    //          Tuple of SegmentIDs and their according Allocators
    allocation_data: Box<[Box<[Mutex<Option<(SegmentId, SegmentAllocator)>>]>]>,
    next_modified_node_id: AtomicU64,
    next_disk_id: AtomicU64,
}

impl<E, SPL, H> Dmu<E, SPL, H, H::Info, H::Generation>
where
    SPL: StoragePoolLayer,
    H: HandlerTypes,
{
    /// Returns a new `Dmu`.
    pub fn new(
        default_compression: Box<dyn CompressionBuilder>,
        default_checksum_builder: <SPL::Checksum as Checksum>::Builder,
        default_storage_class: u8,
        pool: SPL,
        alloc_strategy: [[Option<u8>; NUM_STORAGE_CLASSES]; NUM_STORAGE_CLASSES],
        cache: E,
        handler: H,
    ) -> Self {
        let allocation_data = (0..pool.storage_class_count())
            .map(|class| {
                (0..pool.disk_count(class))
                    .map(|_| Mutex::new(None))
                    .collect::<Vec<_>>()
                    .into_boxed_slice()
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Dmu {
            // default_compression_state: default_compression.new_compression().expect("Can't create compression state"),
            default_compression,
            default_storage_class,
            default_checksum_builder,
            alloc_strategy,
            pool,
            cache: RwLock::new(cache),
            written_back: Mutex::new(HashMap::new()),
            modified_info: Mutex::new(HashMap::new()),
            handler,
            allocation_data,
            next_modified_node_id: AtomicU64::new(1),
            next_disk_id: AtomicU64::new(0),
        }
    }

    /// Returns the underlying handler.
    pub fn handler(&self) -> &H {
        &self.handler
    }

    /// Returns the underlying cache.
    pub fn cache(&self) -> &RwLock<E> {
        &self.cache
    }

    /// Returns the underlying storage pool.
    pub fn pool(&self) -> &SPL {
        &self.pool
    }
}

impl<E, SPL, H> DmlBase for Dmu<E, SPL, H, H::Info, H::Generation>
where
    E: Cache<Key = ObjectKey<H::Generation>>,
    <E as Cache>::Value: SizeMut,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
    H: HandlerTypes,
{
    type ObjectRef = ObjectRef<Self::ObjectPointer>;
    type ObjectPointer = ObjectPointer<SPL::Checksum, H::Info, H::Generation>;
    type Info = H::Info;
}

impl<E, SPL, H, I, G> Dmu<E, SPL, H, I, G>
where
    E: Cache<Key = ObjectKey<G>, Value = RwLock<H::Object>>,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
    H: Handler<ObjectRef<ObjectPointer<SPL::Checksum, I, G>>, Info = I, Generation = G>,
    H::Object: Object<ObjectRef<ObjectPointer<SPL::Checksum, I, G>>>,
    I: PodType,
    G: PodType,
{
    fn steal(
        &self,
        or: &mut <Self as DmlBase>::ObjectRef,
        info: H::Info,
    ) -> Result<Option<<Self as super::HandlerDml>::CacheValueRefMut>, Error> {
        let mid = self.next_modified_node_id.fetch_add(1, Ordering::Relaxed);
        let mid = ModifiedObjectId(mid, or.correct_preference());
        let entry = {
            let mut cache = self.cache.write();
            let was_present = cache.force_change_key(&or.as_key(), ObjectKey::Modified(mid));
            if !was_present {
                return Ok(None);
            }
            self.modified_info.lock().insert(mid, info);
            cache.get(&ObjectKey::Modified(mid), false).unwrap()
        };
        let obj = CacheValueRef::write(entry);
        if let ObjectRef::Unmodified(ptr) = replace(or, ObjectRef::Modified(mid)) {
            let actual_size =
                self.pool
                    .actual_size(ptr.offset.storage_class(), ptr.offset.disk_id(), ptr.size);
            self.handler
                .copy_on_write(ptr.offset, actual_size, ptr.generation, ptr.info);
        }
        Ok(Some(obj))
    }

    /// Will be called when `or` is not in cache but was modified.
    /// Resolves two cases:
    ///     - Previous write back (`Modified(_)`) Will change `or` to
    ///       `InWriteback(_)`.
    ///     - Previous eviction after write back (`InWriteback(_)`) Will change
    ///       `or` to `Unmodified(_)`.
    fn fix_or(&self, or: &mut <Self as DmlBase>::ObjectRef) {
        match *or {
            ObjectRef::Unmodified(_) => unreachable!(),
            ObjectRef::Modified(mid) => {
                *or = ObjectRef::InWriteback(mid);
            }
            ObjectRef::InWriteback(mid) => {
                // The object must have been written back recently.
                let ptr = self.written_back.lock().remove(&mid).unwrap();
                *or = ObjectRef::Unmodified(ptr);
            }
        }
    }

    /// Fetches asynchronously an object from disk and inserts it into the
    /// cache.
    fn fetch(&self, op: &<Self as DmlBase>::ObjectPointer) -> Result<(), Error> {
        // FIXME: reuse decompression_state
        let mut decompression_state = op.decompression_tag.new_decompression()?;
        let offset = op.offset;
        let generation = op.generation;

        let compressed_data = self.pool.read(op.size, op.offset, op.checksum.clone())?;

        let object: H::Object = {
            let data = decompression_state.decompress(&compressed_data)?;
            Object::unpack_at(op.offset, data).chain_err(|| ErrorKind::DeserializationError)?
        };
        let key = ObjectKey::Unmodified { offset, generation };
        self.insert_object_into_cache(key, RwLock::new(object));
        Ok(())
    }

    /// Fetches asynchronously an object from disk and inserts it into the
    /// cache.
    fn try_fetch_async(
        &self,
        op: &<Self as DmlBase>::ObjectPointer,
    ) -> Result<
        impl TryFuture<
                Ok = (
                    ObjectPointer<<SPL as StoragePoolLayer>::Checksum, I, G>,
                    Buf,
                ),
                Error = Error,
            > + Send,
        Error,
    > {
        let ptr = op.clone();

        Ok(self
            .pool
            .read_async(op.size, op.offset, op.checksum.clone())?
            .map_err(Error::from)
            .and_then(move |data| ok((ptr, data))))
    }

    fn insert_object_into_cache(&self, key: ObjectKey<H::Generation>, mut object: E::Value) {
        let size = object.get_mut().size();
        let mut cache = self.cache.write();
        if !cache.contains_key(&key) {
            cache.insert(key, object, size);
        }
    }

    fn evict(&self, mut cache: RwLockWriteGuard<E>) -> Result<(), Error> {
        // TODO we may need to evict multiple objects
        // Algorithm overview:
        // Find some evictable object
        // - unpinned
        // - not in writeback state
        // - can_be_evicted
        // If it's `Unmodified` -> done
        // Change its key (`Modified`) to `InWriteback`
        // Pin object
        // Unlock cache
        // Serialize, compress, checksum
        // Fetch generation
        // Allocate
        // Write out
        // Try to remove from cache
        // If ok -> done
        // If this fails, call copy_on_write as object has been modified again

        let evict_result = cache.evict(|&key, entry, cache_contains_key| {
            let object = entry.get_mut();
            let can_be_evicted = match key {
                ObjectKey::InWriteback(_) => false,
                ObjectKey::Unmodified { .. } => true,
                ObjectKey::Modified(_) => object
                    .for_each_child(|or| {
                        let is_unmodified = loop {
                            if let ObjectRef::Unmodified(_) = *or {
                                break true;
                            }
                            if cache_contains_key(&or.as_key()) {
                                break false;
                            }
                            self.fix_or(or);
                        };
                        if is_unmodified {
                            Ok(())
                        } else {
                            Err(())
                        }
                    })
                    .is_ok(),
            };
            if can_be_evicted {
                Some(object.size())
            } else {
                None
            }
        });
        let (key, mut object) = match evict_result {
            None => return Ok(()),
            Some((key, object)) => (key, object),
        };

        let mid = match key {
            ObjectKey::InWriteback(_) => unreachable!(),
            ObjectKey::Unmodified { .. } => return Ok(()),
            ObjectKey::Modified(mid) => mid,
        };

        let size = object.get_mut().size();
        cache.insert(ObjectKey::InWriteback(mid), object, size);
        let entry = cache.get(&ObjectKey::InWriteback(mid), false).unwrap();

        drop(cache);
        let object = CacheValueRef::read(entry);

        self.handle_write_back(object, mid, true)?;
        Ok(())
    }

    fn handle_write_back(
        &self,
        object: <Self as super::HandlerDml>::CacheValueRef,
        mid: ModifiedObjectId,
        evict: bool,
    ) -> Result<<Self as DmlBase>::ObjectPointer, Error> {
        let object_size = {
            #[cfg(debug_assertions)]
            {
                super::Size::checked_size(&*object).expect("Size calculation mismatch")
            }
            #[cfg(not(debug_assertions))]
            {
                super::Size::size(&*object)
            }
        };
        log::trace!("Entering write back of {:?}", &mid);

        if object_size > 4 * 1024 * 1024 {
            warn!("Writing back large object: {}", object.debug_info());
        }

        let generation = self.handler.current_generation();
        let storage_preference = object.correct_preference();
        let storage_class = storage_preference
            .preferred_class()
            .unwrap_or(self.default_storage_class);

        let compression = &self.default_compression;
        let compressed_data = {
            // FIXME: cache this
            let mut state = compression.new_compression()?;
            {
                object
                    .pack(&mut state)
                    .chain_err(|| ErrorKind::SerializationError)?;
                drop(object);
            }
            state.finish()
        };

        let info = self.modified_info.lock().remove(&mid).unwrap();

        assert!(compressed_data.len() <= u32::max_value() as usize);
        let size = compressed_data.len();
        let size = Block(((size as usize + BLOCK_SIZE - 1) / BLOCK_SIZE) as u32);
        assert!(size.to_bytes() as usize >= compressed_data.len());
        let offset = self.allocate(storage_class, size)?;
        trace!("Returned from allocate");
        assert_eq!(size.to_bytes() as usize, compressed_data.len());
        /*if size.to_bytes() as usize != compressed_data.len() {
            let mut v = compressed_data.into_vec();
            v.resize(size.to_bytes() as usize, 0);
            compressed_data = v.into_boxed_slice();
        }*/

        let checksum = {
            let mut state = self.default_checksum_builder.build();
            state.ingest(&compressed_data);
            state.finish()
        };

        self.pool.begin_write(compressed_data, offset)?;
        trace!("Written to pool");

        let obj_ptr = ObjectPointer {
            offset,
            size,
            checksum,
            decompression_tag: compression.decompression_tag(),
            generation,
            info,
        };

        let was_present;
        {
            let mut cache = self.cache.write();
            // We can safely ignore pins.
            // If it's pinned, it must be a readonly request.
            was_present = if evict {
                cache.force_remove(&ObjectKey::InWriteback(mid), object_size)
            } else {
                cache.force_change_key(
                    &ObjectKey::InWriteback(mid),
                    ObjectKey::Unmodified {
                        offset: obj_ptr.offset,
                        generation: obj_ptr.generation,
                    },
                )
            };
            if was_present {
                self.written_back.lock().insert(mid, obj_ptr.clone());
            }
        }
        if !was_present {
            // The object has been `stolen`.  Notify the handler.
            let actual_size = self.pool.actual_size(
                obj_ptr.offset.storage_class(),
                obj_ptr.offset.disk_id(),
                obj_ptr.size,
            );
            self.handler.copy_on_write(
                obj_ptr.offset,
                actual_size,
                obj_ptr.generation,
                obj_ptr.info,
            );
        }

        trace!("handle_write_back: Leaving");
        Ok(obj_ptr)
    }

    fn allocate(&self, storage_preference: u8, size: Block<u32>) -> Result<DiskOffset, Error> {
        assert!(storage_preference < NUM_STORAGE_CLASSES as u8);
        if size >= Block(2048) {
            warn!("Very large allocation requested: {:?}", size);
        }

        log::trace!("Enter allocate: {{ storage_preference: {}, size: {:?} }}", storage_preference, size);

        let strategy = self.alloc_strategy[storage_preference as usize];

        'class: for &class in strategy.iter().flatten() {
            let disks_in_class = self.pool.disk_count(class);
            if disks_in_class == 0 {
                continue;
            }

            // TODO: Consider the known free size and continue on success

            let start_disk_id = (self.next_disk_id.fetch_add(1, Ordering::Relaxed)
                % u64::from(disks_in_class)) as u16;
            let disk_id = (start_disk_id..disks_in_class)
                .chain(0..start_disk_id)
                .max_by_key(|&disk_id| {
                    self.pool.effective_free_size(
                        class,
                        disk_id,
                        self.handler.get_free_space(class, disk_id).expect("We can be sure that this disk id exists."),
                    )
                })
                .unwrap();
            let size = self.pool.actual_size(class, disk_id, size);
            let disk_size = self.pool.size_in_blocks(class, disk_id);

            let disk_offset = {
                let mut x = self.allocation_data[class as usize][disk_id as usize].lock();

                if x.is_none() {
                    let segment_id = SegmentId::get(DiskOffset::new(class, disk_id, Block(0)));
                    let allocator = self
                        .handler
                        .get_allocation_bitmap(segment_id, self)
                        .chain_err(|| ErrorKind::HandlerError)?;
                    *x = Some((segment_id, allocator));
                }
                let &mut (ref mut segment_id, ref mut allocator) = x.as_mut().unwrap();

                let first_seen_segment_id = *segment_id;
                loop {
                    if let Some(segment_offset) = allocator.allocate(size.as_u32()) {
                        break segment_id.disk_offset(segment_offset);
                    }
                    let next_segment_id = segment_id.next(disk_size);
                    trace!(
                        "Next allocator segment: {:?} -> {:?} ({:?})",
                        segment_id,
                        next_segment_id,
                        disk_size,
                    );
                    if next_segment_id == first_seen_segment_id {
                        // Can't allocate in this class, try next
                        continue 'class;
                    }
                    *allocator = self
                        .handler
                        .get_allocation_bitmap(next_segment_id, self)
                        .chain_err(|| ErrorKind::HandlerError)?;
                    *segment_id = next_segment_id;
                }
            };

            info!("Allocated {:?} at {:?}", size, disk_offset);
            self.handler
                .update_allocation_bitmap(disk_offset, size, Action::Allocate, self)
                .chain_err(|| ErrorKind::HandlerError)?;

            return Ok(disk_offset);
        }

        bail!(ErrorKind::OutOfSpaceError)
    }

    /// Tries to allocate `size` blocks at `disk_offset`.  Might fail if
    /// already in use.
    pub fn allocate_raw_at(&self, disk_offset: DiskOffset, size: Block<u32>) -> Result<(), Error> {
        let disk_id = disk_offset.disk_id();
        let num_disks = self.pool.num_disks(disk_offset.storage_class(), disk_id);
        let size = size * num_disks as u32;
        let segment_id = SegmentId::get(disk_offset);
        let mut x =
            self.allocation_data[disk_offset.storage_class() as usize][disk_id as usize].lock();
        let mut allocator = self
            .handler
            .get_allocation_bitmap(segment_id, self)
            .chain_err(|| ErrorKind::HandlerError)?;
        if allocator.allocate_at(size.as_u32(), SegmentId::get_block_offset(disk_offset)) {
            *x = Some((segment_id, allocator));
            self.handler
                .update_allocation_bitmap(disk_offset, size, Action::Allocate, self)
                .chain_err(|| ErrorKind::HandlerError)?;
            Ok(())
        } else {
            bail!("Cannot allocate raw at {:?} / {:?}", disk_offset, size)
        }
    }

    fn prepare_write_back(
        &self,
        mid: ModifiedObjectId,
        dep_mids: &mut Vec<ModifiedObjectId>,
    ) -> Result<Option<<Self as super::HandlerDml>::CacheValueRef>, ()> {
        trace!("prepare_write_back: Enter");
        loop {
            trace!("prepare_write_back: Trying to acquire cache write lock");
            let mut cache = self.cache.write();
            trace!("prepare_write_back: Acquired");
            if cache.contains_key(&ObjectKey::InWriteback(mid)) {
                // TODO wait
                drop(cache);
                yield_now();
                trace!("prepare_wait_back: Cache contained key, waiting..");
                continue;
            }
            let result =
                cache.change_key(&ObjectKey::Modified(mid), |_, entry, cache_contains_key| {
                    let object = entry.get_mut();
                    let mut modified_children = false;
                    object
                        .for_each_child::<(), _>(|or| loop {
                            let mid = match *or {
                                ObjectRef::Unmodified(_) => break Ok(()),
                                ObjectRef::InWriteback(mid) | ObjectRef::Modified(mid) => mid,
                            };
                            if cache_contains_key(&or.as_key()) {
                                modified_children = true;
                                dep_mids.push(mid);
                                break Ok(());
                            }
                            self.fix_or(or);
                        })
                        .unwrap();
                    if modified_children {
                        Err(())
                    } else {
                        Ok(ObjectKey::InWriteback(mid))
                    }
                });
            return match result {
                Ok(()) => Ok(Some(
                    cache
                        .get(&ObjectKey::InWriteback(mid), false)
                        .map(CacheValueRef::read)
                        .unwrap(),
                )),
                Err(ChangeKeyError::NotPresent) => Ok(None),
                Err(ChangeKeyError::Pinned) => {
                    // TODO wait
                    warn!("Pinned node");
                    drop(cache);
                    yield_now();
                    continue;
                }
                Err(ChangeKeyError::CallbackError(())) => Err(()),
            };
        }
    }
}

impl<E, SPL, H, I, G> super::HandlerDml for Dmu<E, SPL, H, I, G>
where
    E: Cache<Key = ObjectKey<G>, Value = RwLock<H::Object>>,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
    H: Handler<ObjectRef<ObjectPointer<SPL::Checksum, I, G>>, Info = I, Generation = G>,
    H::Object: Object<<Self as DmlBase>::ObjectRef>,
    I: PodType,
    G: PodType,
{
    type Object = H::Object;
    type CacheValueRef = CacheValueRef<E::ValueRef, RwLockReadGuard<'static, H::Object>>;
    type CacheValueRefMut = CacheValueRef<E::ValueRef, RwLockWriteGuard<'static, H::Object>>;

    fn try_get(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRef> {
        let result = {
            // Drop order important
            let cache = self.cache.read();
            cache.get(&or.as_key(), false)
        };
        result.map(CacheValueRef::read)
    }

    fn try_get_mut(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRefMut> {
        if let ObjectRef::Modified(_) = *or {
            let result = {
                let cache = self.cache.read();
                cache.get(&or.as_key(), true)
            };
            result.map(CacheValueRef::write)
        } else {
            None
        }
    }

    fn get(&self, or: &mut Self::ObjectRef) -> Result<Self::CacheValueRef, Error> {
        let mut cache = self.cache.read();
        loop {
            if let Some(entry) = cache.get(&or.as_key(), true) {
                drop(cache);
                return Ok(CacheValueRef::read(entry));
            }
            if let ObjectRef::Unmodified(ref ptr) = *or {
                drop(cache);
                self.fetch(ptr)?;
                cache = self.cache.read();
            } else {
                self.fix_or(or);
            }
        }
    }

    fn get_mut(
        &self,
        or: &mut Self::ObjectRef,
        info: Self::Info,
    ) -> Result<Self::CacheValueRefMut, Error> {
        // Fast path
        if let Some(obj) = self.try_get_mut(or) {
            return Ok(obj);
        }
        // Object either not mutable or not present.
        loop {
            // Try to steal it if present.
            if let Some(obj) = self.steal(or, info)? {
                return Ok(obj);
            }
            // Fetch it.
            self.get(or)?;
        }
    }

    fn insert(&self, object: Self::Object, info: H::Info) -> Self::ObjectRef {
        let mid = ModifiedObjectId(
            self.next_modified_node_id.fetch_add(1, Ordering::Relaxed),
            object.correct_preference(),
        );
        self.modified_info.lock().insert(mid, info);
        let key = ObjectKey::Modified(mid);
        let size = object.size();
        self.cache.write().insert(key, RwLock::new(object), size);
        ObjectRef::Modified(mid)
    }

    fn insert_and_get_mut(
        &self,
        object: Self::Object,
        info: Self::Info,
    ) -> (Self::CacheValueRefMut, Self::ObjectRef) {
        let mid = ModifiedObjectId(
            self.next_modified_node_id.fetch_add(1, Ordering::Relaxed),
            object.correct_preference(),
        );
        self.modified_info.lock().insert(mid, info);
        let key = ObjectKey::Modified(mid);
        let size = object.size();
        let entry = {
            let mut cache = self.cache.write();
            cache.insert(key, RwLock::new(object), size);
            cache.get(&key, false).unwrap()
        };
        (CacheValueRef::write(entry), ObjectRef::Modified(mid))
    }

    fn remove(&self, or: Self::ObjectRef) {
        match self.cache.write().remove(&or.as_key(), |obj| obj.size()) {
            Ok(_) | Err(RemoveError::NotPresent) => {}
            // TODO
            Err(RemoveError::Pinned) => unimplemented!(),
        };
        if let ObjectRef::Unmodified(ref ptr) = or {
            let actual_size =
                self.pool
                    .actual_size(ptr.offset.storage_class(), ptr.offset.disk_id(), ptr.size);
            self.handler
                .copy_on_write(ptr.offset, actual_size, ptr.generation, ptr.info);
        }
    }

    fn get_and_remove(&self, mut or: Self::ObjectRef) -> Result<H::Object, Error> {
        let obj = loop {
            self.get(&mut or)?;
            match self.cache.write().remove(&or.as_key(), |obj| obj.size()) {
                Ok(obj) => break obj,
                Err(RemoveError::NotPresent) => {}
                // TODO
                Err(RemoveError::Pinned) => unimplemented!(),
            };
        };
        if let ObjectRef::Unmodified(ref ptr) = or {
            let actual_size =
                self.pool
                    .actual_size(ptr.offset.storage_class(), ptr.offset.disk_id(), ptr.size);
            self.handler
                .copy_on_write(ptr.offset, actual_size, ptr.generation, ptr.info);
        }
        Ok(obj.into_inner())
    }

    fn ref_from_ptr(r: Self::ObjectPointer) -> Self::ObjectRef {
        r.into()
    }

    fn evict(&self) -> Result<(), Error> {
        // TODO shortcut without locking cache
        let cache = self.cache.write();
        if cache.size() > cache.capacity() {
            self.evict(cache)?;
        }
        Ok(())
    }

    fn verify_cache(&self) {
        self.cache.write().verify();
    }
}

impl<E, SPL, H, I, G> super::Dml for Dmu<E, SPL, H, I, G>
where
    E: Cache<Key = ObjectKey<G>, Value = RwLock<H::Object>>,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
    H: Handler<ObjectRef<ObjectPointer<SPL::Checksum, I, G>>, Info = I, Generation = G>,
    H::Object: Object<<Self as DmlBase>::ObjectRef>,
    I: PodType,
    G: PodType,
{
    fn write_back<F, FO>(&self, mut acquire_or_lock: F) -> Result<Self::ObjectPointer, Error>
    where
        F: FnMut() -> FO,
        FO: DerefMut<Target = Self::ObjectRef>,
    {
        trace!("write_back: Enter");
        let (object, mid) = loop {
            trace!("write_back: Trying to acquire lock");
            let mut or = acquire_or_lock();
            trace!("write_back: Acquired lock");
            let mid = match *or {
                ObjectRef::Unmodified(ref p) => return Ok(p.clone()),
                ObjectRef::InWriteback(mid) | ObjectRef::Modified(mid) => mid,
            };
            let mut mids = Vec::new();

            trace!("write_back: Preparing write back");
            match self.prepare_write_back(mid, &mut mids) {
                Ok(None) => {
                    trace!("write_back: Was Ok(None)");
                    self.fix_or(&mut or)
                },
                Ok(Some(object)) => break (object, mid),
                Err(()) => {
                    trace!("write_back: Was Err");
                    drop(or);
                    while let Some(&mid) = mids.last() {
                        trace!("write_back: Trying to prepare write back");
                        match self.prepare_write_back(mid, &mut mids) {
                            Ok(None) => {}
                            Ok(Some(object)) => {
                                trace!("write_back: Was Ok Some");
                                self.handle_write_back(object, mid, false)?;
                            }
                            Err(()) => continue,
                        };
                        mids.pop();
                    }
                }
            }
        };
        trace!("write_back: Leave");
        self.handle_write_back(object, mid, false)
    }

    type Prefetch = Pin<
        Box<
            dyn Future<Output = Result<(<Self as DmlBase>::ObjectPointer, Buf), Error>>
                + Send
                + 'static,
        >,
    >;
    fn prefetch(&self, or: &Self::ObjectRef) -> Result<Option<Self::Prefetch>, Error> {
        if self.cache.read().contains_key(&or.as_key()) {
            return Ok(None);
        }
        Ok(match *or {
            ObjectRef::Modified(_) | ObjectRef::InWriteback(_) => None,
            ObjectRef::Unmodified(ref p) => Some(Box::pin(self.try_fetch_async(p)?.into_future())),
        })
    }

    fn finish_prefetch(&self, p: Self::Prefetch) -> Result<(), Error> {
        let (ptr, compressed_data) = block_on(p)?;
        let object: H::Object = {
            let data = ptr
                .decompression_tag
                .new_decompression()?
                .decompress(&compressed_data)?;
            Object::unpack_at(ptr.offset, data).chain_err(|| ErrorKind::DeserializationError)?
        };
        let key = ObjectKey::Unmodified {
            offset: ptr.offset,
            generation: ptr.generation,
        };
        self.insert_object_into_cache(key, RwLock::new(object));
        Ok(())
    }

    fn drop_cache(&self) {
        let mut cache = self.cache.write();
        let keys: Vec<_> = cache
            .iter()
            .cloned()
            .filter(|&key| {
                if let ObjectKey::Unmodified { .. } = key {
                    true
                } else {
                    false
                }
            })
            .collect();
        for key in keys {
            let _ = cache.remove(&key, |obj| obj.size());
        }
    }
}

impl<E, SPL, H, I, G> super::DmlWithHandler for Dmu<E, SPL, H, I, G>
where
    E: Cache<Key = ObjectKey<G>, Value = RwLock<H::Object>>,
    H::Object: Size,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
    H: Handler<ObjectRef<ObjectPointer<SPL::Checksum, I, G>>, Info = I, Generation = G>,
    H::Object: Object<<Self as DmlBase>::ObjectRef>,
    I: PodType,
    G: PodType,
{
    type Handler = H;

    fn handler(&self) -> &Self::Handler {
        &self.handler
    }
}

impl<E, SPL, H, I, G> super::DmlWithSpl for Dmu<E, SPL, H, I, G>
where
    E: Cache<Key = ObjectKey<G>, Value = RwLock<H::Object>>,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
    H: Handler<ObjectRef<ObjectPointer<SPL::Checksum, I, G>>, Info = I, Generation = G>,
    H::Object: Object<<Self as DmlBase>::ObjectRef>,
    I: PodType,
    G: PodType,
{
    type Spl = SPL;

    fn spl(&self) -> &Self::Spl {
        &self.pool
    }
}

impl<E, SPL, H, I, G> super::DmlWithCache for Dmu<E, SPL, H, I, G>
where
    E: Cache<Key = ObjectKey<G>, Value = RwLock<H::Object>>,
    H::Object: SizeMut,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
    H: Handler<ObjectRef<ObjectPointer<SPL::Checksum, I, G>>, Info = I, Generation = G>,
    H::Object: Object<<Self as DmlBase>::ObjectRef>,
    I: PodType,
    G: PodType,
{
    type CacheStats = E::Stats;

    fn cache_stats(&self) -> Self::CacheStats {
        self.cache.read().stats()
    }
}

pub struct CacheValueRef<T, U> {
    head: T,
    guard: ManuallyDrop<U>,
}

impl<T, U> Drop for CacheValueRef<T, U> {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.guard);
        }
    }
}

impl<T: AddSize, U> AddSize for CacheValueRef<T, U> {
    fn add_size(&self, size_delta: isize) {
        self.head.add_size(size_delta)
    }
}

impl<T, U> CacheValueRef<T, RwLockReadGuard<'static, U>>
where
    T: StableDeref<Target = RwLock<U>>,
{
    fn read(head: T) -> Self {
        let guard = unsafe { transmute(RwLock::read(&head)) };
        CacheValueRef {
            head,
            guard: ManuallyDrop::new(guard),
        }
    }
}

impl<T, U> CacheValueRef<T, RwLockWriteGuard<'static, U>>
where
    T: StableDeref<Target = RwLock<U>>,
{
    fn write(head: T) -> Self {
        let guard = unsafe { transmute(RwLock::write(&head)) };
        CacheValueRef {
            head,
            guard: ManuallyDrop::new(guard),
        }
    }
}

unsafe impl<T, U> StableDeref for CacheValueRef<T, RwLockReadGuard<'static, U>> {}

impl<T, U> Deref for CacheValueRef<T, RwLockReadGuard<'static, U>> {
    type Target = U;
    fn deref(&self) -> &U {
        &*self.guard
    }
}

unsafe impl<T, U> StableDeref for CacheValueRef<T, RwLockWriteGuard<'static, U>> {}

impl<T, U> Deref for CacheValueRef<T, RwLockWriteGuard<'static, U>> {
    type Target = U;
    fn deref(&self) -> &U {
        &*self.guard
    }
}

impl<T, U> DerefMut for CacheValueRef<T, RwLockWriteGuard<'static, U>> {
    fn deref_mut(&mut self) -> &mut U {
        &mut *self.guard
    }
}
