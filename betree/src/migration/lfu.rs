use crossbeam_channel::Receiver;
use lfu_cache::LfuCache;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    io::Write,
    sync::Arc,
};

use crate::{
    cow_bytes::CowBytes,
    data_management::DmlWithStorageHints,
    database::DatabaseBuilder,
    object::{ObjectStore, ObjectStoreId},
    storage_pool::{DiskOffset, NUM_STORAGE_CLASSES},
    vdev::Block,
    Database, StoragePreference,
};

use super::{
    errors::Result, reinforcment_learning::open_file_buf_write, DatabaseMsg, DmlMsg,
    MigrationConfig, ObjectKey,
};

#[derive(Clone, Debug, Hash, PartialEq)]
/// Internal Object Representation to locate objects on changing buckets and tiers
struct ObjectLocation(StoragePreference, u64);

/// Implementation of Least Frequently Used
pub struct Lfu<C: DatabaseBuilder + Clone> {
    leafs: [LfuCache<DiskOffset, LeafInfo>; NUM_STORAGE_CLASSES],
    dml_rx: Receiver<DmlMsg>,
    db_rx: Receiver<DatabaseMsg<C>>,
    db: Arc<RwLock<Database<C>>>,
    dmu: Arc<<C as DatabaseBuilder>::Dmu>,
    config: MigrationConfig<LfuConfig>,
    // Store open object stores to move inactive objects within.
    object_stores: HashMap<ObjectStoreId, Option<ObjectStore<C>>>,
    /// Object Buckets dividing them into multiple file size ranges,
    /// taken from <https://doi.org/10.1145/3489143>
    objects: HashMap<ObjectKey, ObjectLocation>,
    object_buckets: (
        [[LfuCache<ObjectKey, CowBytes>; NUM_SIZE_BUCKETS]; NUM_STORAGE_CLASSES],
        StoragePreference,
    ),
    /// HashMap accessible by the DML, resolution is not guaranteed but always
    /// used when a object is written.
    storage_hint_dml: Arc<Mutex<HashMap<DiskOffset, StoragePreference>>>,
}

// Bucket boundaries, see comment below:
const SMALLEST: u64 = 256 * 1024;
const SMALLEST_: u64 = 256 * 1024 + 1;
const SMALL: u64 = 1024 * 1024;
const SMALL_: u64 = 1024 * 1024 + 1;
const MEDIUM: u64 = 10 * 1024 * 1024;
const MEDIUM_: u64 = 10 * 1024 * 1024 + 1;
const LARGE: u64 = 100 * 1024 * 1024;
const LARGE_: u64 = 100 * 1024 * 1024 + 1;
const LARGEST: u64 = 1024 * 1024 * 1024;
const LARGEST_: u64 = 1024 * 1024 * 1024 + 1;
const NUM_SIZE_BUCKETS: usize = 6;

impl ObjectLocation {
    // Exlusive Range Patterns are not yet stable, since 6 years...
    // https://github.com/rust-lang/rust/issues/37854
    fn bucket_id(&self) -> usize {
        match self.1 {
            0..=SMALLEST => 0,
            SMALLEST_..=SMALL => 1,
            SMALL_..=MEDIUM => 2,
            MEDIUM_..=LARGE => 3,
            LARGE_..=LARGEST => 4,
            LARGEST_.. => 5,
        }
    }

    fn pref_id(&self, default_class: StoragePreference) -> usize {
        self.0.or(default_class).as_u8() as usize
    }
}

/// Lfu specific configuration details.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct LfuConfig {
    /// Maximum number of nodes to be promoted at once. An object size is
    /// maximum 4 MiB.
    promote_num: u32,
    /// Maximum amount of blocks to promote at once. An object size is maximum 4
    /// MiB.
    promote_size: Block<u32>,
    // Print post-timestep "known" timestep
    path_state: Option<std::path::PathBuf>,
    // Migration decisions
    path_delta: Option<std::path::PathBuf>,
    // Migrate Objects, Nodes or Both
    mode: LfuMode,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum LfuMode {
    Object,
    Node,
    Both,
}

impl Default for LfuConfig {
    fn default() -> Self {
        Self {
            promote_num: u32::MAX,
            promote_size: Block(128),
            path_state: None,
            path_delta: None,
            mode: LfuMode::Object,
        }
    }
}

struct LeafInfo {
    offset: DiskOffset,
    size: Block<u32>,
}

impl Display for LeafInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "LeafInfo {{ mid: {:?}, size: {:?}",
            self.offset, self.size
        ))
    }
}

impl<'lfu, C: DatabaseBuilder + Clone> Lfu<C> {
    fn get_object(
        store: &'lfu mut (
            [[LfuCache<ObjectKey, CowBytes>; NUM_SIZE_BUCKETS]; NUM_STORAGE_CLASSES],
            StoragePreference,
        ),
        loc: &ObjectLocation,
        key: &ObjectKey,
    ) -> Option<&'lfu CowBytes> {
        store.0[loc.pref_id(store.1)][loc.bucket_id()].get(key)
    }

    fn insert_object(
        store: &'lfu mut (
            [[LfuCache<ObjectKey, CowBytes>; NUM_SIZE_BUCKETS]; NUM_STORAGE_CLASSES],
            StoragePreference,
        ),
        loc: ObjectLocation,
        key: ObjectKey,
        name: CowBytes,
    ) -> Option<CowBytes> {
        store.0[loc.pref_id(store.1)][loc.bucket_id()].insert(key, name)
    }

    fn remove_object(
        store: &'lfu mut (
            [[LfuCache<ObjectKey, CowBytes>; NUM_SIZE_BUCKETS]; NUM_STORAGE_CLASSES],
            StoragePreference,
        ),
        loc: &ObjectLocation,
        key: &ObjectKey,
    ) -> Option<(CowBytes, usize)> {
        store.0[loc.pref_id(store.1)][loc.bucket_id()].remove(key)
    }

    fn update_dml(&mut self) -> Result<()> {
        // Consume available messages
        for msg in self.dml_rx.try_iter() {
            match msg {
                DmlMsg::Fetch(info) | DmlMsg::Write(info) => {
                    if let Some(entry) =
                        self.leafs[info.offset.storage_class() as usize].get_mut(&info.offset)
                    {
                        // Known Offset
                        entry.offset = info.offset;
                        entry.size = info.size;
                    } else {
                        // Unknonwn Offset
                        match info.previous_offset {
                            Some(offset) => {
                                debug!("Message: Old Offset {offset:?}");
                                // Moved Entry
                                let new_offset = info.offset;
                                let new_tier = new_offset.storage_class() as usize;
                                let old_tier = offset.storage_class() as usize;
                                if let Some((previous_value, freq)) =
                                    self.leafs[old_tier].remove(&offset)
                                {
                                    debug!("Message: Moving entry..");
                                    self.leafs[new_tier].insert(new_offset, previous_value);

                                    // FIXME: This is hacky way to transfer the
                                    // frequency to the new location. It would
                                    // generally be better _and_ more efficient
                                    // to do this directly in the lfu cache
                                    // crate.
                                    for _ in 0..(freq.saturating_sub(1)) {
                                        self.leafs[new_tier].get(&new_offset);
                                    }
                                } else {
                                    // For some reason the previous entry could not be found.
                                    self.leafs[new_tier].insert(
                                        new_offset,
                                        LeafInfo {
                                            offset: info.offset,
                                            size: info.size,
                                        },
                                    );
                                }
                                let entry = self.leafs[new_tier].get_mut(&new_offset).unwrap();
                                entry.offset = info.offset;
                                entry.size = info.size;
                            }
                            None => {
                                // New Entry
                                self.leafs[info.offset.storage_class() as usize].insert(
                                    info.offset,
                                    LeafInfo {
                                        offset: info.offset,
                                        size: info.size,
                                    },
                                );
                            }
                        }
                    }
                }
                DmlMsg::Remove(opinfo) => {
                    // Delete Offset
                    self.leafs[opinfo.offset.storage_class() as usize].remove(&opinfo.offset);
                }
                // This policy ignores all other messages.
                _ => {}
            }
        }
        Ok(())
    }

    fn update_db(&mut self) -> Result<()> {
        for msg in self.db_rx.try_iter() {
            match msg {
                // cool but not as useful right now
                // FIXME: Noop for now, this should optimally be maybe matching
                // which datasets are active to prevent migrations from them or
                // some other kind of preference treatment
                DatabaseMsg::DatasetOpen(_) => {}
                DatabaseMsg::DatasetClose(_) => {}
                DatabaseMsg::ObjectstoreOpen(key, store) => {
                    self.object_stores.insert(key, Some(store));
                }
                DatabaseMsg::ObjectstoreClose(key) => {
                    self.object_stores.insert(key, None);
                }
                DatabaseMsg::ObjectOpen(key, info, name)
                | DatabaseMsg::ObjectDiscover(key, info, name) => {
                    if let Entry::Vacant(e) = self.object_stores.entry(*key.store_key()) {
                        e.insert(None);
                    }
                    let new_location = ObjectLocation(info.pref, info.size);
                    let old = self.objects.insert(key.clone(), new_location.clone());
                    match old {
                        Some(old_value) if old_value == new_location => {
                            // Update
                            Lfu::<C>::get_object(&mut self.object_buckets, &old_value, &key);
                        }
                        Some(old_value) => {
                            // For some reason move
                            if let Some((name, _)) =
                                Lfu::<C>::remove_object(&mut self.object_buckets, &old_value, &key)
                            {
                                Lfu::<C>::insert_object(
                                    &mut self.object_buckets,
                                    new_location,
                                    key,
                                    name,
                                );
                            }
                        }
                        None => {
                            // Insert new
                            Lfu::<C>::insert_object(
                                &mut self.object_buckets,
                                new_location,
                                key,
                                name,
                            );
                        }
                    }
                }
                DatabaseMsg::ObjectClose(key, info) => {
                    // Insert and get old value
                    let new_location = ObjectLocation(info.pref, info.size);
                    let old = self
                        .objects
                        .insert(key.clone(), new_location.clone())
                        .clone();
                    match old {
                        Some(old_value) if old_value == new_location => {
                            // Update
                            Lfu::<C>::get_object(&mut self.object_buckets, &old_value, &key);
                        }
                        Some(old_value) => {
                            // For some reason move
                            if let Some((name, _)) =
                                Lfu::<C>::remove_object(&mut self.object_buckets, &old_value, &key)
                            {
                                Lfu::<C>::insert_object(
                                    &mut self.object_buckets,
                                    new_location,
                                    key,
                                    name,
                                );
                            }
                        }
                        None => unreachable!(),
                    }
                }
                DatabaseMsg::ObjectRead(key, _) => {
                    if let Some(location) = self.objects.get(&key) {
                        Lfu::<C>::get_object(&mut self.object_buckets, location, &key);
                    }
                }
                DatabaseMsg::ObjectWrite(key, size, pref, _) => {
                    let new_location = ObjectLocation(pref, size);
                    let old = self
                        .objects
                        .insert(key.clone(), new_location.clone())
                        .clone();
                    match old {
                        Some(old_value) if old_value == new_location => {
                            // Update
                            Lfu::<C>::get_object(&mut self.object_buckets, &old_value, &key);
                        }
                        Some(old_value) => {
                            if let Some((name, _)) =
                                Lfu::<C>::remove_object(&mut self.object_buckets, &old_value, &key)
                            {
                                Lfu::<C>::insert_object(
                                    &mut self.object_buckets,
                                    new_location,
                                    key,
                                    name,
                                );
                            }
                        }
                        None => unreachable!(),
                    }
                }
                DatabaseMsg::ObjectMigrate(key, pref) => {
                    if let Some(location) = self.objects.get(&key) {
                        if location.0 != pref {
                            // Move in representation
                            let new_location = ObjectLocation(pref, location.1);
                            if let Some((name, _)) =
                                Lfu::<C>::remove_object(&mut self.object_buckets, location, &key)
                            {
                                Lfu::<C>::insert_object(
                                    &mut self.object_buckets,
                                    new_location,
                                    key,
                                    name,
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub(super) fn build(
        dml_rx: Receiver<DmlMsg>,
        db_rx: Receiver<DatabaseMsg<C>>,
        db: Arc<RwLock<Database<C>>>,
        config: MigrationConfig<LfuConfig>,
        storage_hint_dml: Arc<Mutex<HashMap<DiskOffset, StoragePreference>>>,
    ) -> Self {
        let dmu = Arc::clone(db.read().root_tree.dmu());
        let default_storage_class = dmu.default_storage_class();
        Self {
            leafs: [(); NUM_STORAGE_CLASSES].map(|_| LfuCache::unbounded()),
            dml_rx,
            db_rx,
            dmu,
            db,
            config,
            storage_hint_dml,
            objects: Default::default(),
            object_stores: Default::default(),
            object_buckets: (
                [(); NUM_STORAGE_CLASSES]
                    .map(|_| [(); NUM_SIZE_BUCKETS].map(|_| LfuCache::unbounded())),
                default_storage_class,
            ),
        }
    }
}

impl<C: DatabaseBuilder + Clone> super::MigrationPolicy<C> for Lfu<C> {
    fn promote(&mut self, storage_tier: u8) -> super::errors::Result<Block<u64>> {
        // PROMOTE OF NODES
        // let mut moved = Block(0_u32);

        let desired = Block(self.config.policy_config.promote_size.as_u64());
        let mut moved = Block(0_u64);

        match self.config.policy_config.mode {
            LfuMode::Object => {
                for bucket in 0..NUM_SIZE_BUCKETS {
                    let mut foo = vec![];
                    while let Some((objectkey, name, freq)) = self.object_buckets.0
                        [storage_tier as usize][bucket]
                        .pop_mfu_key_value_frequency()
                    {
                        if let (Some(store_result), Some(lifted)) = (
                            self.object_stores.get(objectkey.store_key()),
                            StoragePreference::from_u8(storage_tier).lift(),
                        ) {
                            // NOTE: Object store can either be unused or active.
                            if let Some(active_store) = store_result.as_ref() {
                                let mut obj = active_store.open_object(&name).unwrap().unwrap();
                                obj.migrate(lifted).expect("Could not migrate");
                                let entry = self.objects.get_mut(&objectkey).unwrap();
                                let size = entry.1;
                                entry.0 = lifted;
                                let new_location = ObjectLocation(lifted, size);
                                Self::insert_object(
                                    &mut self.object_buckets,
                                    new_location.clone(),
                                    objectkey.clone(),
                                    name,
                                );
                                for _ in 0..freq {
                                    let _ = Self::get_object(
                                        &mut self.object_buckets,
                                        &new_location,
                                        &objectkey,
                                    );
                                }
                                moved += Block::from_bytes(size);
                            } else {
                                // NOTE: If not active we may try to open the store.
                                // This carries some implications to the actual user
                                // using the storage stack and should be fixed.

                                // Best effort to try to open an object store.
                                if let Some(mut db) = self.db.try_write() {
                                    let os = db
                                        .open_object_store_with_id(*objectkey.store_key())?;
                                    if let Some(mut obj) = os.open_object(&*name)? {
                                        obj.migrate(lifted)?;
                                        let entry = self.objects.get_mut(&objectkey).unwrap();
                                        let size = entry.1;
                                        entry.0 = lifted;
                                        let new_location = ObjectLocation(lifted, size);
                                        Self::insert_object(
                                            &mut self.object_buckets,
                                            new_location.clone(),
                                            objectkey.clone(),
                                            name,
                                        );
                                        for _ in 0..freq {
                                            let _ = Self::get_object(
                                                &mut self.object_buckets,
                                                &new_location,
                                                &objectkey,
                                            );
                                        }
                                        moved += Block::from_bytes(size);
                                    }
                                    db.close_object_store(os);
                                }
                            }

                            if moved >= desired {
                                break;
                            }
                        } else {
                            foo.push((objectkey, name))
                        }
                    }
                    // This more of shitty fix bc we only have active stores rn, pls FIXME
                    for (key, name) in foo.into_iter() {
                        self.object_buckets.0[storage_tier as usize][bucket].insert(key, name);
                    }
                    if moved >= desired {
                        break;
                    }
                }
            }
            LfuMode::Node => {
                let mut num_moved = 0;
                while moved < Block(self.config.policy_config.promote_size.as_u64())
                    && num_moved < self.config.policy_config.promote_num
                    && !self.leafs[storage_tier as usize].is_empty()
                {
                    if let Some(entry) = self.leafs[storage_tier as usize].pop_mfu() {
                        if let Some(lifted) = StoragePreference::from_u8(storage_tier).lift() {
                            debug!("Moving {:?}", entry.offset);
                            debug!("Was on storage tier: {:?}", storage_tier);
                            self.storage_hint_dml.lock().insert(entry.offset, lifted);
                            debug!("New storage preference: {:?}", lifted);
                            moved += Block(entry.size.as_u64());
                            num_moved += 1;
                        }
                    } else {
                        // If this message occured you'll have most likely encountered a bug in the lfu implemenation.
                        // See https://github.com/jwuensche/lfu-cache
                        warn!(
                            "Cache indicated that it is not empty but no value could be fetched."
                        );
                    }
                }
            }
            LfuMode::Both => unimplemented!(),
        }

        Ok(moved)
    }

    fn demote(&mut self, storage_tier: u8, desired: Block<u64>) -> Result<Block<u64>> {
        let mut moved = Block(0_u64);
        match self.config.policy_config.mode {
            LfuMode::Object => {
                // DEMOTION OF OBJECTS - Use active object stores?
                // Try to demote smallest objects first migrating as many as possible to
                // the next lower storage class.
                for bucket in (0..NUM_SIZE_BUCKETS).skip(3).chain(0..2) {
                    // NOTE: This function turned out to have a quite high code width
                    // with a maximum of 8 indentation.. this is too high. It would be
                    // good to to refactor this code as we continue to work on haura
                    // also to reduce it's complexity. If you are reading this "we"
                    // might be even you :D
                    let mut foo = vec![];
                    while let Some((objectkey, name, freq)) = self.object_buckets.0
                        [storage_tier as usize][bucket]
                        .pop_lfu_key_value_frequency()
                    {
                        if let (Some(store_result), Some(lowered)) = (
                            self.object_stores.get(objectkey.store_key()),
                            StoragePreference::from_u8(storage_tier).lower(),
                        ) {
                            // NOTE: Object store can either be unused or active.
                            if let Some(active_store) = store_result.as_ref() {
                                let mut obj = active_store.open_object(&name).unwrap().unwrap();
                                obj.migrate(lowered).expect("Could not migrate");
                                let entry = self.objects.get_mut(&objectkey).unwrap();
                                let size = entry.1;
                                entry.0 = lowered;
                                let new_location = ObjectLocation(lowered, size);
                                Self::insert_object(
                                    &mut self.object_buckets,
                                    new_location.clone(),
                                    objectkey.clone(),
                                    name,
                                );
                                for _ in 0..freq {
                                    let _ = Self::get_object(
                                        &mut self.object_buckets,
                                        &new_location,
                                        &objectkey,
                                    );
                                }
                                moved += Block::from_bytes(size);
                            } else {
                                // NOTE: If not active we may try to open the store.
                                // This carries some implications to the actual user
                                // using the storage stack and should be fixed.

                                // Best effort to try to open an object store.
                                if let Some(mut db) = self.db.try_write() {
                                    let os = db
                                        .open_object_store_with_id(*objectkey.store_key())?;
                                    if let Some(mut obj) = os.open_object(&*name)? {
                                        obj.migrate(lowered)?;
                                        let entry = self.objects.get_mut(&objectkey).unwrap();
                                        let size = entry.1;
                                        entry.0 = lowered;
                                        let new_location = ObjectLocation(lowered, size);
                                        Self::insert_object(
                                            &mut self.object_buckets,
                                            new_location.clone(),
                                            objectkey.clone(),
                                            name,
                                        );
                                        for _ in 0..freq {
                                            let _ = Self::get_object(
                                                &mut self.object_buckets,
                                                &new_location,
                                                &objectkey,
                                            );
                                        }
                                        moved += Block::from_bytes(size);
                                    }
                                    db.close_object_store(os);
                                }
                            }

                            if moved >= desired {
                                break;
                            }
                        } else {
                            foo.push((objectkey, name))
                        }
                    }
                    // This more of shitty fix bc we only have active stores rn, pls FIXME
                    for (key, name) in foo.into_iter() {
                        self.object_buckets.0[storage_tier as usize][bucket].insert(key, name);
                    }
                    if moved >= desired {
                        break;
                    }
                }
            }
            LfuMode::Node => {
                while moved < desired && !self.leafs[storage_tier as usize].is_empty() {
                    if let Some(entry) = self.leafs[storage_tier as usize].pop_lfu() {
                        if let Some(lowered) = StoragePreference::from_u8(storage_tier).lower() {
                            debug!("Moving {:?}", entry.offset);
                            debug!("Was on storage tier: {:?}", storage_tier);
                            self.storage_hint_dml.lock().insert(entry.offset, lowered);
                            moved += Block(entry.size.as_u64());
                            debug!("New storage preference: {:?}", lowered);
                        }
                    } else {
                        // If this message occured you'll have most likely encountered a bug in the lfu implemenation.
                        // See https://github.com/jwuensche/lfu-cache
                        warn!(
                            "Cache indicated that it is not empty but no value could be fetched."
                        );
                    }
                }
            }
            LfuMode::Both => todo!(),
        }

        Ok(moved)
    }

    fn db(&self) -> &Arc<RwLock<Database<C>>> {
        &self.db
    }

    fn config(&self) -> MigrationConfig<()> {
        self.config.clone().erased()
    }

    fn update(&mut self) -> Result<()> {
        self.update_dml()?;
        self.update_db()
    }

    fn dmu(&self) -> &Arc<<C as DatabaseBuilder>::Dmu> {
        &self.dmu
    }

    /// Write metrics about current timestep
    ///
    /// Implemented for objects atm due to road blocks.
    fn metrics(&self) -> Result<()> {
        #[derive(Serialize)]
        struct Dummy {
            files: HashMap<ObjectKey, (f32, u64, u32)>,
        }

        impl Dummy {
            fn new() -> Self {
                Dummy {
                    files: HashMap::new(),
                }
            }
        }

        // Hashmap key to
        // (Hotness, Size, pausesteps)
        if let Some(Some(mut file)) = self
            .config
            .policy_config
            .path_state
            .as_ref()
            .map(|path| open_file_buf_write(path).ok())
        {
            // Map to be compatible to the metric output from RL policy
            let mut maps: [Dummy; NUM_STORAGE_CLASSES] =
                [0; NUM_STORAGE_CLASSES].map(|_| Dummy::new());

            let d_class = self.object_buckets.1;
            for (obj, loc) in self.objects.iter() {
                let pref = loc.pref_id(d_class);
                maps[pref].files.insert(obj.clone(), (0.0, loc.1, 0));
            }
            serde_json::to_writer(&mut file, &maps)?;
            file.write_all(b"\n")?;
            file.flush()?;
        }

        Ok(())
    }
}
