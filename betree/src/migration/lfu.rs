use crossbeam_channel::Receiver;
use lfu_cache::LfuCache;
use parking_lot::{RwLock, Mutex};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Display,
    sync::Arc, collections::HashMap,
};

use crate::{
    database::{DatabaseBuilder, DatasetId, ObjectRef},
    storage_pool::{DiskOffset, NUM_STORAGE_CLASSES},
    vdev::Block,
    Database, StoragePreference,
};

use super::{MigrationConfig, ProfileMsg};

/// Implementation of Least Frequently Used
pub struct Lfu<C: DatabaseBuilder> {
    leafs: [LfuCache<DiskOffset, LeafInfo>; NUM_STORAGE_CLASSES],
    rx: Receiver<ProfileMsg>,
    db: Arc<RwLock<Database<C>>>,
    dmu: Arc<<C as DatabaseBuilder>::Dmu>,
    config: MigrationConfig<LfuConfig>,
    storage_hint_sink: Arc<Mutex<HashMap<DiskOffset, StoragePreference>>>,
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
            low_threshold: None,
            high_threshold: None,
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

impl<C: DatabaseBuilder> super::MigrationPolicy<C> for Lfu<C> {
    type ObjectReference = ObjectRef;
    type Message = ProfileMsg;
    type Config = LfuConfig;

    fn build(
        rx: Receiver<Self::Message>,
        db: Arc<RwLock<Database<C>>>,
        config: MigrationConfig<LfuConfig>,
        storage_hint_sink: Arc<Mutex<HashMap<DiskOffset, StoragePreference>>>,
    ) -> Self {
        let dmu = Arc::clone(db.read().root_tree.dmu());
        Self {
            leafs: [(); NUM_STORAGE_CLASSES].map(|_| LfuCache::unbounded()),
            rx,
            dmu,
            db,
            config,
            storage_hint_sink,
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
                if let Some(lifted) = StoragePreference::from_u8(storage_tier).lift() {
                    debug!("Moving {:?}", entry.offset);
                    debug!("Was on storage tier: {:?}", storage_tier);
                    self.storage_hint_sink.lock().insert(entry.offset, lifted);
                    moved += entry.size;
                    debug!("New storage preference: {:?}", lifted);
                }
            } else {
                warn!("Cache indicated that it is not empty but no value could be fetched.");
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
                if let Some(lifted) = StoragePreference::from_u8(storage_tier).lower() {
                    debug!("Moving {:?}", entry.offset);
                    debug!("Was on storage tier: {:?}", storage_tier);
                    self.storage_hint_sink.lock().insert(entry.offset, lifted);
                    moved += entry.size;
                    debug!("New storage preference: {:?}", lifted);
                }
            } else {
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

    fn update(&mut self) -> super::errors::Result<()> {
        // Consume available messages
        for msg in self.rx.try_iter() {
            match msg.clone() {
                ProfileMsg::Fetch(info) | ProfileMsg::Write(info) => {
                    match msg.clone() {
                        ProfileMsg::Fetch(_) => warn!("Message: Node fetched {:?}", info.offset),
                        ProfileMsg::Write(_) => warn!("Message: Node written {:?}", info.offset),
                        _ => {},
                    }
                    if let Some(entry) = self.leafs[info.offset.storage_class() as usize]
                        .get_mut(&info.offset)
                    {
                        debug!("Known Diskoffset {:?}", info.offset);
                        entry.offset = info.offset;
                        entry.size = info.size;
                    } else {
                        warn!(
                            "Message: Unknown Diskoffset {:?}",
                            info.offset
                        );
                        match info.previous_offset {
                            Some(offset) => {
                                warn!("Message: Old Offset {offset:?}");
                                let new_offset = info.offset;
                                let new_tier = new_offset.storage_class() as usize;
                                let old_tier = offset.storage_class() as usize;
                                if let Some((previous_value, freq)) =
                                    self.leafs[old_tier].remove(&offset)
                                {
                                    warn!("Message: Moving entry..");
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
                                warn!("Message: New Entry {:?}", info.offset);
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
                ProfileMsg::Remove(opinfo) => {
                    debug!("Message: Node removed {:?}", opinfo.offset);
                    self.leafs[opinfo.offset.storage_class() as usize].remove(&opinfo.offset);
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
