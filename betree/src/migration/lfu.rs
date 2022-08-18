use crossbeam_channel::Receiver;
use lfu_cache::LfuCache;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display, sync::Arc};

use crate::{
    database::{DatabaseBuilder, DatasetId, ObjectRef},
    storage_pool::{DiskOffset, NUM_STORAGE_CLASSES},
    vdev::Block,
    Database, StoragePreference,
};

use super::{errors::Result, DatabaseMsg, DmlMsg, MigrationConfig};

/// Implementation of Least Frequently Used
pub struct Lfu<C: DatabaseBuilder + Clone> {
    leafs: [LfuCache<DiskOffset, LeafInfo>; NUM_STORAGE_CLASSES],
    dml_rx: Receiver<DmlMsg>,
    db_rx: Receiver<DatabaseMsg<C>>,
    db: Arc<RwLock<Database<C>>>,
    dmu: Arc<<C as DatabaseBuilder>::Dmu>,
    config: MigrationConfig<LfuConfig>,
    /// HashMap accessible by the DML, resolution is not guaranteed but always
    /// used when a object is written.
    storage_hint_dml: Arc<Mutex<HashMap<DiskOffset, StoragePreference>>>,
}

/// Lfu specific configuration details.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
pub struct LfuConfig {
    /// Maximum number of nodes to be promoted at once. An object size is
    /// maximum 4 MiB.
    promote_num: u32,
    /// Maximum amount of blocks to promote at once. An object size is maximum 4
    /// MiB.
    promote_size: Block<u32>,
}

impl Default for LfuConfig {
    fn default() -> Self {
        Self {
            promote_num: u32::MAX,
            promote_size: Block(128),
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

impl<C: DatabaseBuilder + Clone> Lfu<C> {
    fn update_dml(&mut self) -> Result<()> {
        // Consume available messages
        for msg in self.dml_rx.try_iter() {
            match msg.clone() {
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
        while let Ok(_) = self.db_rx.try_recv() {}
        Ok(())
    }
}

impl<C: DatabaseBuilder + Clone> super::MigrationPolicy<C> for Lfu<C> {
    type Config = LfuConfig;

    fn build(
        dml_rx: Receiver<DmlMsg>,
        db_rx: Receiver<DatabaseMsg<C>>,
        db: Arc<RwLock<Database<C>>>,
        config: MigrationConfig<LfuConfig>,
        storage_hint_dml: Arc<Mutex<HashMap<DiskOffset, StoragePreference>>>,
    ) -> Self {
        let dmu = Arc::clone(db.read().root_tree.dmu());
        Self {
            leafs: [(); NUM_STORAGE_CLASSES].map(|_| LfuCache::unbounded()),
            dml_rx,
            db_rx,
            dmu,
            db,
            config,
            storage_hint_dml,
        }
    }

    fn promote(&mut self, storage_tier: u8) -> super::errors::Result<Block<u32>> {
        // PROMOTE
        let mut moved = Block(0_u32);
        let mut num_moved = 0;
        while moved < self.config.policy_config.promote_size
            && num_moved < self.config.policy_config.promote_num
            && !self.leafs[storage_tier as usize].is_empty()
        {
            if let Some(entry) = self.leafs[storage_tier as usize].pop_mfu() {
                if let Some(lifted) = StoragePreference::from_u8(storage_tier).lift() {
                    debug!("Moving {:?}", entry.offset);
                    debug!("Was on storage tier: {:?}", storage_tier);
                    self.storage_hint_dml.lock().insert(entry.offset, lifted);
                    debug!("New storage preference: {:?}", lifted);
                    moved += entry.size;
                    num_moved += 1;
                }
            } else {
                // If this message occured you'll have most likely encountered a bug in the lfu implemenation.
                // See https://github.com/jwuensche/lfu-cache
                warn!("Cache indicated that it is not empty but no value could be fetched.");
            }
        }
        return Ok(moved);
    }

    fn demote(&mut self, storage_tier: u8, desired: Block<u32>) -> Result<Block<u32>> {
        // DEMOTE
        let mut moved = Block(0_u32);
        while moved < desired && !self.leafs[storage_tier as usize].is_empty() {
            if let Some(entry) = self.leafs[storage_tier as usize].pop_lfu() {
                if let Some(lowered) = StoragePreference::from_u8(storage_tier).lower() {
                    debug!("Moving {:?}", entry.offset);
                    debug!("Was on storage tier: {:?}", storage_tier);
                    self.storage_hint_dml.lock().insert(entry.offset, lowered);
                    moved += entry.size;
                    debug!("New storage preference: {:?}", lowered);
                }
            } else {
                // If this message occured you'll have most likely encountered a bug in the lfu implemenation.
                // See https://github.com/jwuensche/lfu-cache
                warn!("Cache indicated that it is not empty but no value could be fetched.");
            }
        }
        return Ok(moved);
    }

    fn db(&self) -> &Arc<RwLock<Database<C>>> {
        &self.db
    }

    fn config(&self) -> &MigrationConfig<LfuConfig> {
        &self.config
    }

    fn update(&mut self) -> Result<()> {
        self.update_dml()?;
        self.update_db()
    }

    fn dmu(&self) -> &Arc<<C as DatabaseBuilder>::Dmu> {
        &self.dmu
    }
}
