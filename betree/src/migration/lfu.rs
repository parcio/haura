use crossbeam_channel::Receiver;
use lfu_cache::LfuCache;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    sync::Arc,
    time::Instant,
};

use crate::{
    data_management::{HandlerDml, HasStoragePreference, ObjectRef as ObjectRefTrait},
    database::{DatabaseBuilder, DatasetId, ObjectRef},
    storage_pool::{DiskOffset, NUM_STORAGE_CLASSES},
    vdev::Block,
    Database, StoragePreference,
};

use super::{MigrationConfig, OpInfo, ProfileMsg};

const FREQ_LEN: usize = 10;
const FREQ_LOWER_BOUND: f32 = 0.0001;

/// Implementation of Least Frequently Used
pub struct Lfu<C: DatabaseBuilder> {
    leafs: [LfuCache<DiskOffset, LeafInfo>; NUM_STORAGE_CLASSES],
    rx: Receiver<ProfileMsg<ObjectRef>>,
    db: Arc<RwLock<Database<C>>>,
    dmu: Arc<<C as DatabaseBuilder>::Dmu>,
    config: MigrationConfig<LfuConfig>,
}

/// Lfu specific configuration details.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
pub struct LfuConfig {
    /// If any object falls below this threshold we might pick any of these to avoid sorting effort.
    low_threshold: Option<f32>,
    /// If any object falls above this mark we may upgrade it without further for other objects.
    high_threshold: Option<f32>,
}

impl Default for LfuConfig {
    fn default() -> Self {
        Self {
            low_threshold: Some(FREQ_LOWER_BOUND),
            high_threshold: None,
        }
    }
}

struct LeafInfo {
    mid: ObjectRef,
    size: Block<u32>,
    info: DatasetId,
}

impl Display for LeafInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "LeafInfo {{ mid: {:?}, info: {:?}",
            self.mid, self.info
        ))
    }
}

impl LeafInfo {
    fn mid_mut(&self) -> ObjectRef {
        self.mid.clone()
    }
}

impl<C: DatabaseBuilder> super::MigrationPolicy<C> for Lfu<C> {
    type ObjectReference = ObjectRef;
    type Message = ProfileMsg<Self::ObjectReference>;
    type Config = LfuConfig;

    fn build(
        rx: Receiver<Self::Message>,
        db: Arc<RwLock<Database<C>>>,
        config: MigrationConfig<LfuConfig>,
    ) -> Self {
        let dmu = Arc::clone(db.read().root_tree.dmu());
        Self {
            leafs: [(); NUM_STORAGE_CLASSES].map(|_| LfuCache::unbounded()),
            rx,
            dmu,
            db,
            config,
        }
    }

    fn promote(
        &mut self,
        storage_tier: u8,
        desired: Block<u32>,
    ) -> super::errors::Result<Block<u32>> {
        // PROMOTE
        let mut moved = Block(0_u32);
        while moved < desired && !self.leafs[storage_tier as usize].is_empty() {
            if let Some(entry) = self.leafs[storage_tier as usize].pop_mfu() {
                let mut handle = entry.mid_mut();
                let size = handle.get_unmodified().unwrap().size();
                let mut node = self.dmu.get_mut(&mut handle, entry.info)?;
                if let Some(lifted) = StoragePreference::from_u8(storage_tier).lift() {
                    node.set_system_storage_preference(lifted);
                }
                // node.evict()
                moved += size;
            }
        }
        return Ok(moved);
    }

    fn demote(
        &mut self,
        storage_tier: u8,
        desired: Block<u32>,
    ) -> super::errors::Result<Block<u32>> {
        // DEMOTE
        let mut moved = Block(0_u32);
        while moved < desired && !self.leafs[storage_tier as usize].is_empty() {
            if let Some(entry) = self.leafs[storage_tier as usize].pop_lfu() {
                let mut handle = entry.mid_mut();
                let size = handle.get_unmodified().unwrap().size();
                let mut node = self.dmu.get_mut(&mut handle, entry.info)?;
                if let Some(lowered) = StoragePreference::from_u8(storage_tier).lower() {
                    node.set_system_storage_preference(lowered);
                }
                // node.evict()
                moved += size;
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

    fn update(&mut self) -> super::errors::Result<()> {
        // Consume available messages
        for msg in self.rx.try_iter() {
            match msg.clone() {
                ProfileMsg::Fetch(info) | ProfileMsg::Write(info) => {
                    // debug!("Node added {:?}", info.mid.get_unmodified().unwrap().offset());
                    // Based on the message type we can guarantee that this will only contain Unmodified references.
                    if let Some(entry) = self.leafs[info.storage_tier as usize]
                        .get_mut(&info.mid.get_unmodified().unwrap().offset())
                    {
                        debug!("Known Diskoffset {:?}", info.mid.get_unmodified().unwrap());
                        entry.mid = info.mid;
                        entry.size = info.size;
                    } else {
                        debug!(
                            "Unknown Diskoffset {:?}",
                            info.mid.get_unmodified().unwrap()
                        );
                        match info.p_disk_offset {
                            Some(offset) => {
                                let new_offset = info.mid.get_unmodified().unwrap().offset();
                                let new_tier = new_offset.storage_class() as usize;
                                let old_tier = offset.storage_class() as usize;
                                if let Some((previous_value, freq)) =
                                    self.leafs[old_tier].remove(&offset)
                                {
                                    debug!("Node has been moved. Moving entry..");
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
                                            mid: info.mid,
                                            size: info.size,
                                            info: info.mid.get_unmodified().unwrap().info(),
                                        },
                                    );
                                }
                                let entry = self.leafs[new_tier].get_mut(&new_offset).unwrap();
                                entry.mid = info.mid;
                                entry.size = info.size;
                            }
                            None => {
                                // debug!("New Entry {:?}", info.mid.get_unmodified().unwrap());
                                self.leafs[info.storage_tier as usize].insert(
                                    info.mid.get_unmodified().unwrap().offset(),
                                    LeafInfo {
                                        info: info.mid.get_unmodified().unwrap().info(),
                                        mid: info.mid,
                                        size: info.size,
                                    },
                                );
                                // debug!("Map now has {} entries.", self.leafs[info.storage_tier as usize].len());
                            }
                        }
                    }
                }
                ProfileMsg::Remove(mid) => {
                    let offset = mid.get_unmodified().unwrap().offset();
                    // debug!("Node will be removed {:?}", offset);
                    self.leafs[offset.storage_class() as usize].remove(&offset);
                }
                ProfileMsg::Discover(mid) => {
                    let op = mid.get_unmodified().unwrap();
                    let offset = op.offset();
                    let info = op.info();
                    let size = op.size();
                    self.leafs[offset.storage_class() as usize]
                        .insert(offset, LeafInfo { mid, info, size });
                }
                // This policy ignores all other messages.
                _ => {}
            }
        }
        Ok(())
    }

    fn dmu(&self) -> &Arc<<C as DatabaseBuilder>::Dmu> {
        &self.dmu
    }
}
