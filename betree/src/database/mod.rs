//! This module provides the Database Layer.
use crate::{
    atomic_option::AtomicOption,
    cache::ClockCache,
    checksum::{XxHash, XxHashBuilder},
    compression,
    cow_bytes::SlicedCowBytes,
    data_management::{self, Dml, DmlBase, DmlWithHandler, DmlWithSpl, Dmu, HandlerDml},
    size::StaticSize,
    storage_pool::{DiskOffset, StorageConfiguration, StoragePoolLayer, StoragePoolUnit},
    tree::{
        MessageAction, DefaultMessageAction, ErasedTreeSync, Inner as TreeInner, Node, Tree, TreeBaseLayer,
        TreeLayer,
    },
    vdev::Block,
};
use bincode::{deserialize, serialize_into};
use byteorder::{BigEndian, ByteOrder, LittleEndian};
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
};

mod dataset;
mod errors;
mod handler;
mod snapshot;
mod superblock;
pub use self::{handler::Handler, superblock::Superblock};

pub use self::{
    dataset::Dataset, errors::*, handler::update_allocation_bitmap_msg,
    in_memory::InMemoryConfiguration, snapshot::Snapshot,
};

const ROOT_DATASET_ID: DatasetId = DatasetId(0);
const DEFAULT_CACHE_SIZE: usize = 256 * 1024 * 1024;

type Compression = compression::None;
type Checksum = XxHash;

type ObjectPointer =
    data_management::impls::ObjectPointer<Compression, Checksum, DatasetId, Generation>;
type ObjectRef = data_management::impls::ObjectRef<ObjectPointer>;
type Object = Node<ObjectRef>;

pub(crate) type RootDmu = Dmu<
    compression::None,
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

// TODO: Is this multi-step process unnecessarily rigid? Would fewer functions defeat the purpose?
pub trait DatabaseBuilder
where
    Self::Spu: StoragePoolLayer,
    Self::Dmu: DmlBase<ObjectRef = ObjectRef, ObjectPointer = ObjectPointer, Info = DatasetId>
        + Dml<Object = Object, ObjectRef = ObjectRef>
        + HandlerDml<Object = Object>
        + DmlWithHandler<Handler = handler::Handler>
        + DmlWithSpl<Spl = Self::Spu>
        + 'static,
{
    type Spu: StoragePoolLayer;
    type Dmu: DmlBase<ObjectRef = ObjectRef, ObjectPointer = ObjectPointer, Info = DatasetId> + Send + Sync;

    fn new_spu(&self) -> Result<Self::Spu>;
    fn new_handler(&self, spu: &Self::Spu) -> handler::Handler;
    fn new_dmu(&self, spu: Self::Spu, handler: handler::Handler) -> Self::Dmu;
    fn select_root_tree(&self, dmu: Arc<Self::Dmu>)
        -> Result<(RootTree<Self::Dmu>, ObjectPointer)>;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AccessMode {
    /// Read and use an existing superblock, abort if none is found.
    OpenIfExists,
    /// Don't check for an existing superblock, create a new root tree and write it to disk.
    AlwaysCreateNew,
    /// Use an existing database if found, create a new one otherwise.
    OpenOrCreate,
}

pub struct DatabaseConfiguration {
    /// Backing storage to be used
    pub storage: StorageConfiguration,
    /// Size of cache in TODO
    pub cache_size: usize,
    /// Whether to check for and open an existing database, or overwrite it
    pub access_mode: AccessMode,
}

impl Default for DatabaseConfiguration {
    fn default() -> Self {
        Self {
            storage: StorageConfiguration::new(Vec::new()),
            cache_size: DEFAULT_CACHE_SIZE,
            access_mode: AccessMode::OpenOrCreate,
        }
    }
}

impl DatabaseBuilder for DatabaseConfiguration {
    type Spu = StoragePoolUnit<XxHash>;
    type Dmu = RootDmu;

    fn new_spu(&self) -> Result<Self::Spu> {
        StoragePoolUnit::<XxHash>::new(&self.storage).chain_err(|| ErrorKind::SplConfiguration)
    }

    fn new_handler(&self, spu: &Self::Spu) -> handler::Handler {
        Handler {
            root_tree_inner: AtomicOption::new(),
            root_tree_snapshot: RwLock::new(None),
            current_generation: SeqLock::new(Generation(1)),
            free_space: HashMap::from_iter(
                (0..spu.disk_count()).map(|disk_id| (disk_id, AtomicU64::new(0))),
            ),
            delayed_messages: Mutex::new(Vec::new()),
            last_snapshot_generation: RwLock::new(HashMap::new()),
            allocations: AtomicU64::new(0),
            old_root_allocation: SeqLock::new(None),
        }
    }

    fn new_dmu(&self, spu: Self::Spu, handler: handler::Handler) -> RootDmu {
        Dmu::new(
            compression::None,
            XxHashBuilder,
            spu,
            ClockCache::new(self.cache_size),
            handler,
        )
    }

    fn select_root_tree(&self, dmu: Arc<RootDmu>) -> Result<(RootTree<Self::Dmu>, ObjectPointer)> {
        let root_ptr = if let AccessMode::OpenIfExists | AccessMode::OpenOrCreate = self.access_mode
        {
            match Superblock::<ObjectPointer>::fetch_superblocks(dmu.pool()) {
                None if self.access_mode == AccessMode::OpenIfExists => {
                    bail!(ErrorKind::InvalidSuperblock)
                }
                ptr => ptr,
            }
        } else {
            None
        };

        if let Some(root_ptr) = root_ptr {
            let tree = RootTree::open(ROOT_DATASET_ID, root_ptr.clone(), DefaultMessageAction, dmu);
            *tree.dmu().handler().old_root_allocation.lock_write() =
                Some((root_ptr.offset(), root_ptr.size()));
            tree.dmu()
                .handler()
                .root_tree_inner
                .set(Arc::clone(tree.inner()));
            Ok((tree, root_ptr))
        } else {
            Superblock::<ObjectPointer>::clear_superblock(dmu.pool())?;
            let tree = RootTree::empty_tree(ROOT_DATASET_ID, DefaultMessageAction, dmu.clone());
            tree.dmu()
                .handler()
                .root_tree_inner
                .set(Arc::clone(tree.inner()));
            {
                let dmu = tree.dmu();
                for disk_id in 0..dmu.pool().disk_count() {
                    dmu.allocate_raw_at(DiskOffset::new(disk_id as usize, Block(0)), Block(2))
                        .chain_err(|| "Superblock allocation failed")?;
                }
            }
            let root_ptr = tree.sync()?;
            Ok((tree, root_ptr))
        }
    }
}

type ErasedTree = dyn ErasedTreeSync<
    Pointer = <RootDmu as DmlBase>::ObjectPointer,
    ObjectRef = <RootDmu as DmlBase>::ObjectRef,
> + Send + Sync;

/// The database type.
pub struct Database<Config: DatabaseBuilder> {
    root_tree: RootTree<Config::Dmu>,
    open_datasets: HashMap<DatasetId, Box<ErasedTree>>,
}

impl Database<DatabaseConfiguration> {
    /// Opens a database given by the storage pool configuration.
    pub fn open(cfg: StorageConfiguration) -> Result<Self> {
        Self::build(DatabaseConfiguration {
            storage: cfg,
            access_mode: AccessMode::OpenIfExists,
            ..Default::default()
        })
    }

    /// Creates a database given by the storage pool configuration.
    ///
    /// Note that any existing database will be overwritten!
    pub fn create(cfg: StorageConfiguration) -> Result<Self> {
        Self::build(DatabaseConfiguration {
            storage: cfg,
            access_mode: AccessMode::AlwaysCreateNew,
            ..Default::default()
        })
    }

    /// Opens or creates a database given by the storage pool configuration.
    pub fn open_or_create(cfg: StorageConfiguration) -> Result<Self> {
        Self::build(DatabaseConfiguration {
            storage: cfg,
            access_mode: AccessMode::OpenOrCreate,
            ..Default::default()
        })
    }
}

impl<Config: DatabaseBuilder> Database<Config> {
    /// Opens or creates a database given by the storage pool configuration and
    /// sets the given cache size.
    pub fn build(builder: Config) -> Result<Self> {
        let spl = builder.new_spu()?;
        let handler = builder.new_handler(&spl);
        let dmu = Arc::new(builder.new_dmu(spl, handler));
        let (tree, root_ptr) = builder.select_root_tree(dmu)?;

        *tree.dmu().handler().current_generation.lock_write() = root_ptr.generation().next();
        *tree.dmu().handler().root_tree_snapshot.write() = Some(TreeInner::new_ro(
            RootDmu::ref_from_ptr(root_ptr),
            DefaultMessageAction,
        ));

        Ok(Database {
            root_tree: tree,
            open_datasets: Default::default(),
        })
    }

    fn sync_ds(&self, ds_id: DatasetId, ds_tree: &ErasedTree) -> Result<()> {
        let ptr = ds_tree.erased_sync()?;
        let msg = DatasetData::update_ptr(ptr)?;
        let key = &ds_data_key(ds_id) as &[_];
        self.root_tree.insert(key, msg)?;
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
                self.root_tree.insert(key, msg)?;
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

    #[cfg(feature = "internal-api")]
    pub fn root_tree(&self) -> &RootTree<Config::Dmu> {
        &self.root_tree
    }
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
    fn size() -> usize {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Generation(u64);

impl StaticSize for Generation {
    fn size() -> usize {
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
