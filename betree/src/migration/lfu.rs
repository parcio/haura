use crossbeam_channel::Receiver;
use lfu_cache::LfuCache;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap},
    io::Write,
    sync::Arc,
};

use crate::{
    cow_bytes::CowBytes,
    data_management::{DmlWithStorageHints, HasStoragePreference},
    database::RootDmu,
    object::{ObjectStore, ObjectStoreId},
    storage_pool::NUM_STORAGE_CLASSES,
    tree::PivotKey,
    vdev::Block,
    Database, StoragePreference,
};

use super::{
    errors::{Error, Result},
    reinforcment_learning::open_file_buf_write,
    DatabaseMsg, DmlMsg, GlobalObjectId, MigrationConfig,
};

/// Implementation of Least Frequently Used
pub struct Lfu {
    nodes: [LfuCache<PivotKey, Block<u32>>; NUM_STORAGE_CLASSES],
    dml_rx: Receiver<DmlMsg>,
    db_rx: Receiver<DatabaseMsg>,
    db: Arc<RwLock<Database>>,
    dmu: Arc<RootDmu>,
    config: MigrationConfig<LfuConfig>,
    // Store open object stores to move inactive objects within.
    object_stores: HashMap<ObjectStoreId, Option<ObjectStore>>,
    objects: [LfuCache<GlobalObjectId, CowBytes>; NUM_STORAGE_CLASSES],
    /// HashMap accessible by the DML, resolution is not guaranteed but always
    /// used when a object is written.
    storage_hint_dml: Arc<Mutex<HashMap<PivotKey, StoragePreference>>>,
}

/// Least frequently used (LFU) specific configuration details.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct LfuConfig {
    /// Maximum number of elements to be promoted at once. This can be added as
    /// an additional restriction on the size limitation of promote procedures.
    pub promote_num: u32,
    /// Maximum amount of blocks to promote at once. The default value for this
    /// field is 1024 which represents about 4 MiB of data. Together with the
    /// update interval of the policy we can determine the expected base load
    /// applied by the policy. So for example, for an update interval of 1
    /// second we have 4 MiB per second.  Please note that you'll likely
    /// experience better performance with LFU by choosing a greater amount of
    /// blocks (>128 MiB) and longer update intervals due to a reduced
    /// interference with other operations, especially on seek latency sensitive
    /// mediums such as HDDs.
    pub promote_size: Block<u32>,
    /// Path to file which stores the complete recorded state of the storage
    /// stack after each timestep in a newline-delimited json format.
    pub path_state: Option<std::path::PathBuf>,
    /// Path to file which stores all migration decisions made by the policy in
    /// a timestep.  Stored as CSV.
    pub path_delta: Option<std::path::PathBuf>,
    /// Migrate Objects, Nodes or Both.
    pub mode: LfuMode,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
/// Descriptor of the kind of operation the LFU policy should perform.
pub enum LfuMode {
    /// Only migrates complete objects. This mode will allow migration only for
    /// [crate::object::ObjectStore]s. Any openend [crate::database::Dataset]s
    /// are unaffected.
    Object,
    /// Only migrate nodes. This mode will migrate any node which has been seen
    /// in the process of the session. Therefore this mode is well fitted to
    /// cases in which data is likely needed to be promoted often.
    Node,
    /// Apply both object and node operations. This is of advantage when bulk
    /// data needs to be migrated downwards for longer term storage and ranges
    /// of objects should be upgraded to optimize for latency.
    Both,
}

impl Default for LfuConfig {
    fn default() -> Self {
        Self {
            promote_num: u32::MAX,
            promote_size: Block(1024),
            path_state: None,
            path_delta: None,
            mode: LfuMode::Object,
        }
    }
}

impl<'lfu> Lfu {
    // Okay for new logic.
    fn update_dml(&mut self) -> Result<()> {
        // Consume available messages
        for msg in self.dml_rx.try_iter() {
            match msg {
                DmlMsg::Write(info) => {
                    // We may possibly encounter the node on other layers if it
                    // has been migrated. So scan previous locations (still
                    // constant but suboptimal).
                    for tier in 0..NUM_STORAGE_CLASSES {
                        if tier == info.offset.storage_class() as usize {
                            continue;
                        }
                        // Move to new storage tier
                        if let Some((_, freq)) =
                            self.nodes[tier].remove_with_frequency(&info.pivot_key)
                        {
                            self.nodes[info.offset.storage_class() as usize].insert_with_frequency(
                                info.pivot_key.clone(),
                                info.size,
                                freq,
                            );
                        }
                    }

                    *self.nodes[info.offset.storage_class() as usize]
                        .entry(info.pivot_key)
                        .or_insert(info.size) = info.size;
                }
                DmlMsg::Fetch(info) => {
                    *self.nodes[info.offset.storage_class() as usize]
                        .entry(info.pivot_key)
                        .or_insert(info.size) = info.size;
                }
                DmlMsg::Remove(info) => {
                    self.nodes[info.offset.storage_class() as usize].remove(&info.pivot_key);
                }
            }
        }
        Ok(())
    }

    fn update_db(&mut self) -> Result<()> {
        for msg in self.db_rx.try_iter() {
            match msg {
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
                    match self.objects[info.pref.as_u8() as usize].get(&key) {
                        Some(_) => {}
                        None => {
                            // Insert new
                            self.objects[info.pref.as_u8() as usize].insert(key, name);
                        }
                    }
                }
                DatabaseMsg::ObjectClose(key, info) => {
                    // NO-OP
                }
                DatabaseMsg::ObjectRead(key, _) | DatabaseMsg::ObjectWrite(key, ..) => {
                    // This has the potential to be of cost 4*hash_lookup but overall it is still constant..
                    for id in 0..NUM_STORAGE_CLASSES {
                        self.objects[id].get(&key);
                    }
                }
                DatabaseMsg::ObjectMigrate(key, pref) => {
                    for id in 0..NUM_STORAGE_CLASSES {
                        if let Some(name) = self.objects[id].remove(&key) {
                            self.objects[pref.as_u8() as usize].insert(key.clone(), name);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub(super) fn build(
        dml_rx: Receiver<DmlMsg>,
        db_rx: Receiver<DatabaseMsg>,
        db: Arc<RwLock<Database>>,
        config: MigrationConfig<LfuConfig>,
        storage_hint_dml: Arc<Mutex<HashMap<PivotKey, StoragePreference>>>,
    ) -> Self {
        let dmu = Arc::clone(db.read().root_tree.dmu());
        let default_storage_class = dmu.default_storage_class();
        Self {
            nodes: [(); NUM_STORAGE_CLASSES].map(|_| LfuCache::unbounded()),
            dml_rx,
            db_rx,
            dmu,
            db,
            config,
            storage_hint_dml,
            object_stores: Default::default(),
            objects: [(); NUM_STORAGE_CLASSES].map(|_| LfuCache::unbounded()),
        }
    }

    fn migrate_object_from_to(
        &self,
        object_id: GlobalObjectId,
        object_name: &CowBytes,
        from: StoragePreference,
        to: StoragePreference,
    ) -> Result<Block<u64>> {
        // NOTE: Object store can either be unused or active.
        let store = if let Some(Some(active_store)) = self.object_stores.get(object_id.store_key())
        {
            active_store.clone()
        } else {
            // NOTE: If not active we may try to open the store.
            // This carries some implications to the actual user
            // using the storage stack and should be fixed.

            // Best effort to try to open an object store.
            if let Some(mut db) = self.db.try_write() {
                let os = db.open_object_store_with_id(*object_id.store_key())?;
                os
            } else {
                return Err(Error::from_kind(super::errors::ErrorKind::MigrationFailed));
            }
        };
        if let Some(mut obj) = store.open_object(object_name)? {
            let size = obj
                .info()?
                .expect("Object does not have any metadata.")
                .size;
            obj.migrate(to)?;
            return Ok(Block::from_bytes(size));
        }
        Err(Error::from_kind(super::errors::ErrorKind::MigrationFailed))
    }
}

impl super::MigrationPolicy for Lfu {
    fn promote(
        &mut self,
        storage_tier: u8,
        tight_space: bool,
    ) -> super::errors::Result<Block<u64>> {
        let desired = Block(self.config.policy_config.promote_size.as_u64());
        let mut moved = Block(0_u64);
        let target = StoragePreference::from_u8(storage_tier - 1);
        match self.config.policy_config.mode {
            LfuMode::Object => {
                while let Some((object_id, name, freq)) =
                    self.objects[storage_tier as usize].peek_mfu_key_value_frequency()
                {
                    let up_freq = if let Some((_upper_size, other_freq)) =
                        self.objects[target.as_u8() as usize].peek_lfu_frequency()
                    {
                        other_freq
                    } else {
                        0
                    };

                    if up_freq < freq || !tight_space {
                        moved += self.migrate_object_from_to(
                            object_id.clone(),
                            &name,
                            StoragePreference::from_u8(storage_tier),
                            target,
                        )?;
                        let (key, val, freq) = self.objects[storage_tier as usize]
                            .pop_mfu_key_value_frequency()
                            .expect("Invalid Pop");
                        self.objects[target.as_u8() as usize].insert_with_frequency(key, val, freq);

                        if moved >= desired {
                            break;
                        }
                    }
                }
            }
            LfuMode::Node => {
                loop {
                    if let Some((key, lower_size, freq)) =
                        self.nodes[storage_tier as usize].peek_mfu_key_value_frequency()
                    {
                        let up_freq = if let Some((_upper_size, other_freq)) =
                            self.nodes[target.as_u8() as usize].peek_lfu_frequency()
                        {
                            other_freq
                        } else {
                            0
                        };
                        if up_freq < freq || !tight_space {
                            // MOVE DATA UPWARDS
                            self.storage_hint_dml.lock().insert(key.clone(), target);
                            moved += lower_size.as_u64();

                            // In case enough data has been moved; rate limited.
                            if moved >= desired {
                                break;
                            }
                            // NOTE:
                            // Any node which could have been displaced by
                            // this operation is migrated later downwards
                            // when clearing the space requirements
                            continue;
                        }
                    }
                    break;
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
                let target = StoragePreference::from_u8(storage_tier + 1);
                while let Some((object_id, name, freq)) =
                    self.objects[storage_tier as usize].peek_lfu_key_value_frequency()
                {
                    moved += self.migrate_object_from_to(
                        object_id.clone(),
                        &name,
                        StoragePreference::from_u8(storage_tier),
                        target,
                    )?;

                    let (key, val, freq) = self.objects[storage_tier as usize]
                        .pop_lfu_key_value_frequency()
                        .expect("Invalid Pop");
                    self.objects[target.as_u8() as usize].insert_with_frequency(key, val, freq);

                    if moved >= desired {
                        break;
                    }
                }
            }
            LfuMode::Node => {
                let target = StoragePreference::from_u8(storage_tier + 1);

                while moved < desired && !self.nodes[storage_tier as usize].is_empty() {
                    if let Some((key, entry, freq)) =
                        self.nodes[storage_tier as usize].pop_lfu_key_value_frequency()
                    {
                        let ds = self
                            .db
                            .write()
                            .open_dataset_with_id(key.d_id())
                            .expect("Dataset Id incorrect");
                        let mut cache_entry = ds.get_node_pivot_mut(&key).unwrap().unwrap();
                        cache_entry.set_system_storage_preference(target);
                        // This does not adhere to constant costs, but rather is of O(number of unique frequencies)
                        debug!("Moving {:?}", key);
                        self.nodes[target.as_u8() as usize].insert_with_frequency(key, entry, freq);
                        debug!("Was on storage tier: {:?}", storage_tier);
                        moved += Block(entry.as_u64());
                        debug!("New storage preference: {:?}", target);
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

    fn db(&self) -> &Arc<RwLock<Database>> {
        &self.db
    }

    fn config(&self) -> MigrationConfig<()> {
        self.config.clone().erased()
    }

    fn update(&mut self) -> Result<()> {
        self.update_dml()?;
        self.update_db()
    }

    fn dmu(&self) -> &Arc<RootDmu> {
        &self.dmu
    }

    /// Write metrics about current timestep
    ///
    /// Implemented for objects atm due to road blocks.
    fn metrics(&self) -> Result<()> {
        #[derive(Serialize)]
        struct Dummy {
            files: HashMap<GlobalObjectId, (f32, u64, u32)>,
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

            // let d_class = self.object_buckets.1;
            // for (obj, loc) in self.objects.iter() {
            //     let pref = loc.pref_id(d_class);
            //     maps[pref].files.insert(obj.clone(), (0.0, loc.1, 0));
            // }
            serde_json::to_writer(&mut file, &maps)?;
            file.write_all(b"\n")?;
            file.flush()?;
        }

        Ok(())
    }
}
