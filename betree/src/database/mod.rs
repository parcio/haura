//! This module provides the Database Layer.
use crate::{
    atomic_option::AtomicOption,
    cache::ClockCache,
    checksum::{XxHash, XxHashBuilder},
    compression::CompressionConfiguration,
    cow_bytes::SlicedCowBytes,
    data_management::{
        self, Dml, DmlBase, DmlWithCache, DmlWithHandler, DmlWithSpl, Dmu, HandlerDml, Handler as DmuHandler,
    },
    metrics::{metrics_init, MetricsConfiguration},
    size::StaticSize,
    storage_pool::{
        DiskOffset, StoragePoolConfiguration, StoragePoolLayer, StoragePoolUnit,
        NUM_STORAGE_CLASSES,
    },
    tree::{
        DefaultMessageAction, ErasedTreeSync, Inner as TreeInner, Node, Tree, TreeBaseLayer,
        TreeLayer,
    },
    vdev::Block,
    StoragePreference,
};
use bincode::{deserialize, serialize_into};
use byteorder::{BigEndian, ByteOrder, LittleEndian};
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use seqlock::SeqLock;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::HashMap,
    iter::FromIterator,
    mem::replace,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread, path::Path,
};

mod dataset;
mod errors;
mod handler;
mod snapshot;
mod superblock;
mod sync_timer;

#[cfg(feature = "figment_config")]
mod figment;

pub use self::{
    dataset::Dataset,
    errors::*,
    handler::{update_allocation_bitmap_msg, Handler},
    snapshot::Snapshot,
    superblock::Superblock,
};

const ROOT_DATASET_ID: DatasetId = DatasetId(0);
const ROOT_TREE_STORAGE_PREFERENCE: StoragePreference = StoragePreference::FASTEST;
const DEFAULT_CACHE_SIZE: usize = 256 * 1024 * 1024;
const DEFAULT_SYNC_INTERVAL_MS: u64 = 1000;

type Checksum = XxHash;

type ObjectPointer = data_management::impls::ObjectPointer<Checksum, DatasetId, Generation>;
type ObjectRef = data_management::impls::ObjectRef<ObjectPointer>;
type Object = Node<ObjectRef>;

pub(crate) type RootDmu = Dmu<
    ClockCache<data_management::impls::ObjectKey<Generation>, RwLock<Object>>,
    StoragePoolUnit<XxHash>,
    Handler,
    DatasetId,
    Generation,
>;

pub(crate) type MessageTree<Dmu, Message> =
    Tree<Arc<Dmu>, Message, Arc<TreeInner<ObjectRef, DatasetId, Message>>>;

pub(crate) type RootTree<Dmu> = MessageTree<Dmu, DefaultMessageAction>;
pub(crate) type DatasetTree<Dmu> = RootTree<Dmu>;

/// This trait describes a multi-step database initialisation procedure.
/// Each function is called sequentially to configure the individual components
/// of the database.
///
/// Components of lower layers (Spu, Dmu) can be replaced, but the [handler::Handler] structure
/// belongs to the database layer, and is necessary for the database to function.
// TODO: Is this multi-step process unnecessarily rigid? Would fewer functions defeat the purpose?
pub trait DatabaseBuilder: Send + Sync + 'static
where
    Self::Spu: StoragePoolLayer,
    Self::Dmu: DmlBase<ObjectRef = ObjectRef, ObjectPointer = ObjectPointer, Info = DatasetId>
        + Dml<Object = Object, ObjectRef = ObjectRef>
        + HandlerDml<Object = Object>
        + DmlWithHandler<Handler = handler::Handler>
        + DmlWithSpl<Spl = Self::Spu>
        + DmlWithCache
        + Send
        + Sync
        + 'static,
{
    /// A storage pool unit, implementing the storage pool layer trait
    type Spu;
    /// A data management unit
    type Dmu;

    /// Separate actions to perform before building the database, e.g. setting up metrics
    fn pre_build(&self) {}
    /// Assemble a new storage layer unit from `self`
    fn new_spu(&self) -> Result<Self::Spu>;
    /// Assemble a new [handler::Handler] for `spu`, optionally configured from `self`
    fn new_handler(&self, spu: &Self::Spu) -> handler::Handler;
    /// Assemble a new data management unit for `spu` and `handler`, optionally configured from
    /// `self`
    fn new_dmu(&self, spu: Self::Spu, handler: handler::Handler) -> Self::Dmu;
    /// Find and return the location of the root tree root node to use.
    /// This may create a new root tree, then return the location of its new root node,
    /// or retrieve a previously written root tree.
    fn select_root_tree(&self, dmu: Arc<Self::Dmu>)
        -> Result<(RootTree<Self::Dmu>, ObjectPointer)>;

    fn sync_mode(&self) -> SyncMode;
}

/// This enum controls whether to search for an existing database to reuse,
/// and whether to create a new one if none could be found or.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum AccessMode {
    /// Read and use an existing superblock, abort if none is found.
    OpenIfExists,
    /// Don't check for an existing superblock, create a new root tree and write it to disk.
    AlwaysCreateNew,
    /// Use an existing database if found, create a new one otherwise.
    OpenOrCreate,
}

/// Determines when sync is called
pub enum SyncMode {
    /// No automatic sync, only on user call
    Explicit,
    /// Every `interval_ms` milliseconds, sync is called
    Periodic { interval_ms: u64 },
}

/// A bundle type of component configuration types, used during [Database::build]
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DatabaseConfiguration {
    /// Backing storage to be used
    pub storage: StoragePoolConfiguration,

    /// During allocation of a request with priority p, the allocator will
    /// try every class in alloc_strategy\[p\] in order.
    ///
    /// A few example strategies:
    /// - `[[0], [1], [2], [3]]` will allocate each request in the request class,
    ///     but doesn't allow falling back to other classes if an allocation fails
    /// - `[[0], [1], [1], [1]]` will allocate requests for 0 and 1 in the requested classes,
    ///     but maps 2 and 3 to 1 as well. Again, without fallback
    /// - `[[0, 1, 2, 3], [1, 2, 3], [2, 3], [3]]` will try to allocate as requested,
    ///     but allows falling back to higher classes if a class is full
    pub alloc_strategy: [Vec<u8>; NUM_STORAGE_CLASSES],
    /// Default storage class, used when attempting to allocate a tree object without
    /// a storage preference
    pub default_storage_class: u8,
    /// Which compression type to use, and the type-specific compression parameters
    pub compression: CompressionConfiguration,
    /// Size of cache in TODO
    pub cache_size: usize,
    /// Whether to check for and open an existing database, or overwrite it
    pub access_mode: AccessMode,

    /// When set, try to sync all datasets every `sync_interval_ms` milliseconds
    pub sync_interval_ms: Option<u64>,

    /// If and how to log database metrics
    pub metrics: Option<MetricsConfiguration>,
}

impl Default for DatabaseConfiguration {
    fn default() -> Self {
        Self {
            storage: StoragePoolConfiguration::default(),
            // identity mapping
            alloc_strategy: [vec![0], vec![1], vec![2], vec![3]],
            default_storage_class: 0,
            compression: CompressionConfiguration::None,
            cache_size: DEFAULT_CACHE_SIZE,
            access_mode: AccessMode::OpenIfExists,
            sync_interval_ms: Some(DEFAULT_SYNC_INTERVAL_MS),
            metrics: None,
        }
    }
}

impl DatabaseConfiguration {
    /// Serialize the configuration to a given path in the json format.
    pub fn write_to_json<P: AsRef<Path>>(&self,path: P) -> Result<()> {
        let file = std::fs::OpenOptions::new().write(true).truncate(true).create(true).open(path)?;
        serde_json::to_writer_pretty(file, &self).map_err(|e| crate::database::errors::Error::with_chain(e, ErrorKind::SerializeFailed))?;
        Ok(())
    }
}

impl DatabaseBuilder for DatabaseConfiguration {
    type Spu = StoragePoolUnit<XxHash>;
    type Dmu = RootDmu;

    fn new_spu(&self) -> Result<Self::Spu> {
        Ok(StoragePoolUnit::<XxHash>::new(&self.storage)?)
    }

    fn new_handler(&self, spu: &Self::Spu) -> handler::Handler {
        // TODO: Update the free sizes of each used vdev here.
        // How do we recover this from the storage?
        // FIXME: Ensure this is recovered properly from storage
        Handler {
            root_tree_inner: AtomicOption::new(),
            root_tree_snapshot: RwLock::new(None),
            current_generation: SeqLock::new(Generation(1)),
            free_space: HashMap::from_iter((0..spu.storage_class_count()).flat_map(|class| {
                (0..spu.disk_count(class)).map(move |disk_id| ((class, disk_id), AtomicU64::new(
                    spu.effective_free_size(
                        class,
                        disk_id,
                        spu.size_in_blocks(class, disk_id)
                    ).as_u64()
                )))
            })),
            // FIXME: This can be done much nicer
            free_space_tier: (0..spu.storage_class_count()).map(|class| {
                AtomicU64::new((0..spu.disk_count(class)).fold(0, |acc, disk_id| {
                    acc + spu.effective_free_size(
                        class,
                        disk_id,
                        spu.size_in_blocks(class, disk_id)
                    ).as_u64()
                }))
            }).collect_vec(),
            delayed_messages: Mutex::new(Vec::new()),
            last_snapshot_generation: RwLock::new(HashMap::new()),
            allocations: AtomicU64::new(0),
            old_root_allocation: SeqLock::new(None),
        }
    }

    fn new_dmu(&self, spu: Self::Spu, handler: handler::Handler) -> Self::Dmu {
        let mut strategy: [[Option<u8>; NUM_STORAGE_CLASSES]; NUM_STORAGE_CLASSES] =
            [[None; NUM_STORAGE_CLASSES]; NUM_STORAGE_CLASSES];

        for (dst, src) in strategy.iter_mut().zip(self.alloc_strategy.iter()) {
            assert!(
                src.len() < NUM_STORAGE_CLASSES,
                "Invalid allocation strategy, can't try more than once per class"
            );

            for (dst, src) in dst.iter_mut().zip(src) {
                *dst = Some(*src);
            }
        }

        Dmu::new(
            self.compression.to_builder(),
            XxHashBuilder,
            self.default_storage_class,
            spu,
            strategy,
            ClockCache::new(self.cache_size),
            handler,
        )
    }

    fn select_root_tree(
        &self,
        dmu: Arc<Self::Dmu>,
    ) -> Result<(RootTree<Self::Dmu>, ObjectPointer)> {
        if let Some(cfg) = &self.metrics {
            metrics_init::<Self>(&cfg, dmu.clone())?;
        }

        let root_ptr = if let AccessMode::OpenIfExists | AccessMode::OpenOrCreate = self.access_mode
        {
            match Superblock::<ObjectPointer>::fetch_superblocks(dmu.pool()) {
                Ok(None) if self.access_mode == AccessMode::OpenIfExists => {
                    bail!(ErrorKind::InvalidSuperblock)
                }
                Ok(ptr) => ptr,
                Err(e) => return Err(e),
            }
        } else {
            None
        };

        if let Some(root_ptr) = root_ptr {
            let tree = RootTree::open(
                ROOT_DATASET_ID,
                root_ptr.clone(),
                DefaultMessageAction,
                dmu,
                ROOT_TREE_STORAGE_PREFERENCE,
            );
            *tree.dmu().handler().old_root_allocation.lock_write() =
                Some((root_ptr.offset(), root_ptr.size()));
            tree.dmu()
                .handler()
                .root_tree_inner
                .set(Arc::clone(tree.inner()));
            Ok((tree, root_ptr))
        } else {
            Superblock::<ObjectPointer>::clear_superblock(dmu.pool())?;
            let tree = RootTree::empty_tree(
                ROOT_DATASET_ID,
                DefaultMessageAction,
                dmu.clone(),
                ROOT_TREE_STORAGE_PREFERENCE,
            );
            tree.dmu()
                .handler()
                .root_tree_inner
                .set(Arc::clone(tree.inner()));
            {
                let dmu = tree.dmu();
                for class in 0..dmu.pool().storage_class_count() {
                    for disk_id in 0..dmu.pool().disk_count(class) {
                        dmu.allocate_raw_at(DiskOffset::new(class, disk_id, Block(0)), Block(2))
                            .chain_err(|| "Superblock allocation failed")?;
                    }
                }
            }
            let root_ptr = tree.sync()?;
            Ok((tree, root_ptr))
        }
    }

    fn sync_mode(&self) -> SyncMode {
        if let Some(interval_ms) = self.sync_interval_ms {
            SyncMode::Periodic { interval_ms }
        } else {
            SyncMode::Explicit
        }
    }
}

type ErasedTree = dyn ErasedTreeSync<Pointer = ObjectPointer, ObjectRef = ObjectRef> + Send + Sync;

/// The database type.
pub struct Database<Config: DatabaseBuilder> {
    root_tree: RootTree<Config::Dmu>,
    builder: Config,
    open_datasets: HashMap<DatasetId, Box<ErasedTree>>,
}

impl Database<DatabaseConfiguration> {
    /// Opens a database given by the storage pool configuration.
    pub fn open(cfg: StoragePoolConfiguration) -> Result<Self> {
        Self::build(DatabaseConfiguration {
            storage: cfg,
            access_mode: AccessMode::OpenIfExists,
            ..Default::default()
        })
    }

    /// Creates a database given by the storage pool configuration.
    ///
    /// Note that any existing database will be overwritten!
    pub fn create(cfg: StoragePoolConfiguration) -> Result<Self> {
        Self::build(DatabaseConfiguration {
            storage: cfg,
            access_mode: AccessMode::AlwaysCreateNew,
            ..Default::default()
        })
    }

    /// Opens or creates a database given by the storage pool configuration.
    pub fn open_or_create(cfg: StoragePoolConfiguration) -> Result<Self> {
        Self::build(DatabaseConfiguration {
            storage: cfg,
            access_mode: AccessMode::OpenOrCreate,
            ..Default::default()
        })
    }

    /// Write the current configuration to the specified path in the json format.
    pub fn write_config_json<P: AsRef<Path>>(&self, p: P) -> Result<()> {
        self.builder.write_to_json(p)
    }
}

impl<Config: DatabaseBuilder> Database<Config> {
    /// Opens or creates a database given by the storage pool configuration and
    /// sets the given cache size.
    pub fn build(builder: Config) -> Result<Self> {
        builder.pre_build();
        let spl = builder.new_spu()?;
        let handler = builder.new_handler(&spl);
        let dmu = Arc::new(builder.new_dmu(spl, handler));
        let (tree, root_ptr) = builder.select_root_tree(dmu)?;

        *tree.dmu().handler().current_generation.lock_write() = root_ptr.generation().next();
        *tree.dmu().handler().root_tree_snapshot.write() = Some(TreeInner::new_ro(
            RootDmu::ref_from_ptr(root_ptr),
            DefaultMessageAction,
        ));

        let db = Database {
            root_tree: tree,
            builder,
            open_datasets: Default::default(),
        };

        Ok(db)
    }

    /// If this [Database] was created with a [SyncMode::Periodic], this function
    /// will wrap self in an `Arc<RwLock<_>>` and start a thread to periodically
    /// call `self.sync()`.
    ///
    /// This is a separate step from [Database::build] because some usecases don't require
    /// periodic syncing.
    pub fn with_sync(self) -> Arc<RwLock<Self>> {
        if let SyncMode::Periodic { interval_ms } = self.builder.sync_mode() {
            let db = Arc::new(RwLock::new(self));
            thread::spawn({
                let db = db.clone();
                move || sync_timer::sync_timer(interval_ms, db)
            });
            db
        } else {
            Arc::new(RwLock::new(self))
        }
    }

    fn sync_ds(&self, ds_id: DatasetId, ds_tree: &ErasedTree) -> Result<()> {
        trace!("sync_ds: Enter");
        let ptr = ds_tree.erased_sync()?;
        trace!("sync_ds: erased_sync");
        let msg = DatasetData::update_ptr(ptr)?;
        let key = &ds_data_key(ds_id) as &[_];
        self.root_tree.insert(key, msg, StoragePreference::NONE)?;
        Ok(())
    }

    fn flush_delayed_messages(&self) -> Result<()> {
        loop {
            let v = replace(
                &mut *self.root_tree.dmu().handler().delayed_messages.lock(),
                Vec::new(),
            );
            if v.is_empty() {
                break;
            }
            for (key, msg) in v {
                self.root_tree.insert(key, msg, StoragePreference::NONE)?;
            }
        }
        Ok(())
    }

    /// Synchronizes the database.
    pub fn sync(&mut self) -> Result<()> {
        let mut ds_locks = Vec::with_capacity(self.open_datasets.len());
        for (&ds_id, ds_tree) in &self.open_datasets {
            loop {
                if let Some(lock) = ds_tree.erased_try_lock_root() {
                    ds_locks.push(lock);
                    break;
                }
                info!("Sync: syncing tree of {:?}", ds_id);
                self.sync_ds(ds_id, ds_tree.as_ref())?;
            }
        }
        let root_ptr = loop {
            self.flush_delayed_messages()?;
            let allocations_before = self
                .root_tree
                .dmu()
                .handler()
                .allocations
                .load(Ordering::Acquire);
            info!("Sync: syncing root tree");

            let root_ptr = self.root_tree.sync()?;
            let allocations_after = self
                .root_tree
                .dmu()
                .handler()
                .allocations
                .load(Ordering::Acquire);
            let allocations = allocations_after - allocations_before;
            if allocations <= 1 {
                break root_ptr;
            } else {
                info!("Sync: resyncing -- seen {} allocations", allocations);
            }
        };
        let pool = self.root_tree.dmu().spl();
        pool.flush()?;
        Superblock::<ObjectPointer>::write_superblock(pool, &root_ptr)?;
        pool.flush()?;
        let handler = self.root_tree.dmu().handler();
        *handler.old_root_allocation.lock_write() = None;
        handler.bump_generation();
        handler
            .root_tree_snapshot
            .write()
            .as_mut()
            .unwrap()
            .update_root_node(RootDmu::ref_from_ptr(root_ptr));
        Ok(())
    }

    pub fn free_space_tier(&self) -> Vec<StorageInfo> {
        (0..self.root_tree.dmu().spl().storage_class_count()).map(|class| {
            StorageInfo {
                free: self.root_tree.dmu().handler().get_free_space_tier(class).expect("Could not find expected class"),
                total: Block(0u64),
            }
        }).collect()
    }

    #[allow(missing_docs)]
    #[cfg(feature = "internal-api")]
    pub fn root_tree(&self) -> &RootTree<Config::Dmu> {
        &self.root_tree
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageInfo {
    pub free: Block<u64>,
    pub total: Block<u64>,
}

fn ss_key(ds_id: DatasetId, name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 8 + name.len());
    key.push(3);
    key.extend_from_slice(&ds_id.pack());
    key.extend_from_slice(name);
    key
}

fn ss_data_key(ds_id: DatasetId, ss_id: Generation) -> [u8; 17] {
    let mut key = [0; 17];
    key[0] = 4;
    key[1..9].copy_from_slice(&ds_id.pack());
    key[9..].copy_from_slice(&ss_id.pack());
    key
}

fn ss_data_key_max(mut ds_id: DatasetId) -> [u8; 9] {
    ds_id.0 += 1;
    let mut key = [0; 9];
    key[0] = 4;
    key[1..9].copy_from_slice(&ds_id.pack());
    key
}

fn dead_list_min_key(ds_id: DatasetId, ss_id: Generation) -> [u8; 17] {
    let mut key = [0; 17];
    key[0] = 5;
    key[1..9].copy_from_slice(&ds_id.pack());
    key[9..].copy_from_slice(&ss_id.pack());
    key
}

fn dead_list_max_key(ds_id: DatasetId, ss_id: Generation) -> [u8; 17] {
    dead_list_min_key(ds_id, ss_id.next())
}

fn dead_list_max_key_ds(mut ds_id: DatasetId) -> [u8; 9] {
    ds_id.0 += 1;
    let mut key = [0; 9];
    key[0] = 5;
    key[1..9].copy_from_slice(&ds_id.pack());
    key
}

fn dead_list_key(ds_id: DatasetId, cur_gen: Generation, offset: DiskOffset) -> [u8; 25] {
    let mut key = [0; 25];
    key[0] = 5;
    key[1..9].copy_from_slice(&ds_id.pack());
    key[9..17].copy_from_slice(&cur_gen.pack());
    BigEndian::write_u64(&mut key[17..], offset.as_u64());
    key
}

fn offset_from_dead_list_key(key: &[u8]) -> DiskOffset {
    DiskOffset::from_u64(BigEndian::read_u64(&key[17..]))
}

fn ds_data_key(id: DatasetId) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = 2;
    key[1..].copy_from_slice(&id.pack());
    key
}

fn fetch_ds_data<T>(root_tree: &T, id: DatasetId) -> Result<DatasetData<ObjectPointer>>
where
    T: TreeBaseLayer<DefaultMessageAction>,
{
    let key = &ds_data_key(id) as &[_];
    let data = root_tree.get(key)?.ok_or(ErrorKind::DoesNotExist)?;
    DatasetData::unpack(&data)
}

fn fetch_ss_data<T>(
    root_tree: &T,
    ds_id: DatasetId,
    ss_id: Generation,
) -> Result<DatasetData<ObjectPointer>>
where
    T: TreeBaseLayer<DefaultMessageAction>,
{
    let key = ss_data_key(ds_id, ss_id);
    let data = root_tree.get(key)?.ok_or(ErrorKind::DoesNotExist)?;
    DatasetData::unpack(&data)
}

#[derive(Debug, Serialize, Deserialize)]
struct DeadListData {
    size: Block<u32>,
    birth: Generation,
}

impl DeadListData {
    fn pack(&self) -> Result<[u8; 12]> {
        let mut buf = [0; 12];
        serialize_into(&mut buf[..], self)?;
        Ok(buf)
    }
    fn unpack(b: &[u8]) -> Result<Self> {
        Ok(deserialize(b)?)
    }
}

/// Internal identifier for a dataset
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct DatasetId(u64);

impl DatasetId {
    fn pack(self) -> [u8; 8] {
        let mut b = [0; 8];
        BigEndian::write_u64(&mut b, self.0);
        b
    }

    fn unpack(b: &[u8]) -> Self {
        DatasetId(BigEndian::read_u64(b))
    }

    fn next(self) -> Self {
        DatasetId(self.0 + 1)
    }
}

impl StaticSize for DatasetId {
    fn static_size() -> usize {
        8
    }
}

#[derive(Debug)]
struct DatasetData<P> {
    previous_snapshot: Option<Generation>,
    ptr: P,
}

impl<P> DatasetData<P> {
    fn update_previous_snapshot(x: Option<Generation>) -> SlicedCowBytes {
        let mut b = [0; 8];
        let x = if let Some(generation) = x {
            assert_ne!(generation.0, 0);
            generation.0
        } else {
            0
        };
        LittleEndian::write_u64(&mut b[..], x);

        DefaultMessageAction::upsert_msg(0, &b)
    }
}

impl<P: Serialize> DatasetData<P> {
    fn update_ptr(ptr: P) -> Result<SlicedCowBytes> {
        let mut v = Vec::new();
        serialize_into(&mut v, &ptr)?;
        let msg = DefaultMessageAction::upsert_msg(8, &v);
        Ok(msg)
    }

    fn pack(&self) -> Result<Vec<u8>> {
        let mut v = vec![0; 8];
        let x = if let Some(generation) = self.previous_snapshot {
            assert_ne!(generation.0, 0);
            generation.0
        } else {
            0
        };
        LittleEndian::write_u64(&mut v, x);
        serialize_into(&mut v, &self.ptr)?;
        Ok(v)
    }
}
impl<P: DeserializeOwned> DatasetData<P> {
    fn unpack(b: &[u8]) -> Result<Self> {
        let x = LittleEndian::read_u64(b.get(..8).ok_or("invalid data")?);
        let ptr = deserialize(&b[8..])?;
        Ok(DatasetData {
            previous_snapshot: if x > 0 { Some(Generation(x)) } else { None },
            ptr,
        })
    }
}

/// Internal identifier of a generation
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Generation(u64);

impl StaticSize for Generation {
    fn static_size() -> usize {
        8
    }
}

impl Generation {
    fn pack(self) -> [u8; 8] {
        let mut b = [0; 8];
        BigEndian::write_u64(&mut b, self.0);
        b
    }

    fn unpack(b: &[u8]) -> Self {
        Generation(BigEndian::read_u64(b))
    }

    fn next(self) -> Self {
        Generation(self.0 + 1)
    }
}
