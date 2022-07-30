use crossbeam_channel::Receiver;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc, fmt::Display,
};

use crate::{
    data_management::{HandlerDml, ObjectRef as ObjectRefTrait},
    database::{DatabaseBuilder, DatasetId, ObjectRef},
    storage_pool::{DiskOffset, NUM_STORAGE_CLASSES},
    Database,
};

use super::{MigrationConfig, ProfileMsg, OpInfo};

const FREQ_LEN: usize = 10;
const FREQ_LOWER_BOUND: f32 = 0.0001;

/// Implementation of Least Frequently Used
pub struct Lfu<C: DatabaseBuilder> {
    leafs: [HashMap<DiskOffset, LeafInfo>; NUM_STORAGE_CLASSES],
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
        Self { low_threshold: Some(FREQ_LOWER_BOUND), high_threshold: None }
    }
}

struct LeafInfo {
    freq: f32,
    msgs: VecDeque<ProfileMsg<ObjectRef>>,
    mid: ObjectRef,
    info: DatasetId,
}

impl Display for LeafInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("LeafInfo {{ freq: {}, mid: {:?}, info: {:?}", self.freq, self.mid, self.info))
    }
}

impl LeafInfo {
    fn mid_mut(&self) -> ObjectRef {
        self.mid.clone()
    }
}

impl<C: DatabaseBuilder> Lfu<C> {
    fn update_entry(&mut self, msg: ProfileMsg<ObjectRef>, info: OpInfo<ObjectRef>) {
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
            leafs: [(); NUM_STORAGE_CLASSES].map(|_| Default::default()),
            rx,
            dmu,
            db,
            config,
        }
    }

    fn action(&self, storage_tier: u8) -> super::errors::Result<()> {
        if let Some(selected) = self.leafs[storage_tier as usize].iter().find(|elem| {
            // frequency comparison with some random constant for testing
            elem.1.freq < 0.25
        }) {
            let mut handle = selected.1.mid_mut();
            let guard = self.db.read();
            let dmu = guard.root_tree.dmu();
            let node = dmu.get_mut(&mut handle, selected.1.info).unwrap();
            // TODO: Update system preference of selected nodes
            todo!()
        }
        todo!()
    }

    fn db(&self) -> &Arc<RwLock<Database<C>>> {
        &self.db
    }

    fn config(&self) -> &MigrationConfig<LfuConfig> {
        &self.config
    }

    fn update(&mut self) -> super::errors::Result<()> {
        let update_entry = move |leafs: &mut [HashMap<DiskOffset, LeafInfo>; NUM_STORAGE_CLASSES], info: OpInfo<ObjectRef>, msg: ProfileMsg<ObjectRef>| {
            if let Some(entry) = leafs[info.storage_tier as usize]
                .get_mut(&info.mid.get_unmodified().unwrap().offset())
            {
                entry.msgs.push_front(msg);
                entry.msgs.truncate(FREQ_LEN);
                // Only classify as soon as enough entries are collected
                if let (Some(first), Some(last)) =
                    (entry.msgs.get(0), entry.msgs.get(FREQ_LEN - 1))
                {
                    match (first, last) {
                        (ProfileMsg::Fetch(f), ProfileMsg::Fetch(l))
                        | (ProfileMsg::Fetch(f), ProfileMsg::Write(l))
                        | (ProfileMsg::Write(f), ProfileMsg::Fetch(l))
                        | (ProfileMsg::Write(f), ProfileMsg::Write(l)) => {
                            entry.freq = FREQ_LEN as f32
                                / (l.time
                                    .duration_since(f.time)
                                    .expect("Events not ordered"))
                                .as_secs_f32();
                        }
                        _ => {}
                    }
                }
            }
        };

        // Consume available messages
        for msg in self.rx.try_iter() {
            match msg.clone() {
                ProfileMsg::Fetch(info) | ProfileMsg::Write(info) => {
                    // debug!("Node added {:?}", info.mid.get_unmodified().unwrap().offset());
                    // Based on the message type we can guarantee that this will only contain Unmodified references.
                    if self.leafs[info.storage_tier as usize].contains_key(&info.mid.get_unmodified().unwrap().offset())
                    {
                        debug!("Known Diskoffset {:?}", info.mid.get_unmodified().unwrap());
                        update_entry(&mut self.leafs, info, msg);
                    } else {
                        debug!("Unknown Diskoffset {:?}", info.mid.get_unmodified().unwrap());
                        match info.p_disk_offset {
                            Some(offset) => {
                                if let Some(previous_value) = self.leafs[info.storage_tier as usize].remove(&offset) {
                                    debug!("Node has been moved. Moving entry..");
                                    self.leafs[info.storage_tier as usize].insert(info.mid.get_unmodified().unwrap().offset(), previous_value);
                                }
                                update_entry(&mut self.leafs, info, msg);
                            },
                            None => {
                                // debug!("New Entry {:?}", info.mid.get_unmodified().unwrap());
                                self.leafs[info.storage_tier as usize].insert(
                                    info.mid.get_unmodified().unwrap().offset(),
                                    LeafInfo {
                                        freq: 0.0,
                                        msgs: vec![msg].into(),
                                        info: info.mid.get_unmodified().unwrap().info(),
                                        mid: info.mid,
                                    },
                                );
                                // debug!("Map now has {} entries.", self.leafs[info.storage_tier as usize].len());
                            },
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
                    self.leafs[offset.storage_class() as usize].insert(
                        offset,
                        LeafInfo {
                            freq: 0.0,
                            msgs: vec![].into(),
                            mid,
                            info,
                        },
                    );
                }
                // This policy ignores all other messages.
                _ => {}
            }
        }
        Ok(())
    }
}
