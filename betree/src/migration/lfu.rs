use crossbeam_channel::Receiver;
use parking_lot::RwLock;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use crate::{
    data_management::{ObjectRef as ObjectRefTrait, HandlerDml},
    database::{DatabaseBuilder, ObjectRef, DatasetId},
    storage_pool::{DiskOffset, NUM_STORAGE_CLASSES},
    Database,
};

use super::{
    ProfileMsg,
    MigrationConfig,
};

const FREQ_LEN: usize = 10;

/// Implementation of Least Frequently Used

pub struct Lfu<C: DatabaseBuilder> {
    leafs: [HashMap<DiskOffset, LeafInfo>; NUM_STORAGE_CLASSES],
    rx: Receiver<ProfileMsg<ObjectRef>>,
    db: Arc<RwLock<Database<C>>>,
    config: MigrationConfig,
}

struct LeafInfo {
    freq: f32,
    msgs: VecDeque<ProfileMsg<ObjectRef>>,
    mid: ObjectRef,
    info: DatasetId,
}

impl LeafInfo {
    fn mid_mut(&self) -> ObjectRef {
        self.mid.clone()
    }
}

impl<
        C: DatabaseBuilder,
    > super::MigrationPolicy<C> for Lfu<C>
{
    type ObjectReference = ObjectRef;
    type Message = ProfileMsg<Self::ObjectReference>;

    fn build(rx: Receiver<Self::Message>, db: Arc<RwLock<Database<C>>>, config: MigrationConfig) -> Self {
        Self {
            leafs: [(); NUM_STORAGE_CLASSES].map(|_| Default::default()),
            rx,
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
            let guard = self.db.write();
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

    fn config(&self) -> &MigrationConfig {
        &self.config
    }

    fn update(&mut self) -> super::errors::Result<()> {
        // Consume available messages
        for msg in self.rx.try_iter() {
            match msg.clone() {
                ProfileMsg::Fetch(info) | ProfileMsg::Write(info) => {
                    // Based on the message type we can guarantee that this will only contain Unmodified references.
                    if let Some(entry) = self.leafs[info.storage_tier as usize]
                        .get_mut(&info.mid.get_unmodified().unwrap().offset())
                    {
                        // TODO: Move entry on moving object pointer
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
                    } else {
                        self.leafs[info.storage_tier as usize].insert(
                            info.mid.get_unmodified().unwrap().offset(),
                            LeafInfo {
                                freq: 0.0,
                                msgs: vec![msg].into(),
                                info: info.mid.get_unmodified().unwrap().info(),
                                mid: info.mid,
                            });
                    }
                }
                ProfileMsg::Remove(mid) => {
                    let offset = mid.get_unmodified().unwrap().offset();
                    self.leafs[offset.storage_class() as usize].remove(&offset);
                }
                ProfileMsg::Discover(mid) => {
                    let op = mid.get_unmodified().unwrap();
                    let offset = op.offset();
                    let info = op.info();
                    self.leafs[offset.storage_class() as usize].insert(offset, LeafInfo { freq: 0.0, msgs: vec![].into(), mid, info });
                }
                // This policy ignores all other messages.
                _ => {}
            }
        }
        Ok(())
    }
}
