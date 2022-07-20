use crossbeam_channel::Receiver;
use parking_lot::RwLock;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use crate::{
    data_management::ObjectRef,
    database::DatabaseBuilder,
    storage_pool::{DiskOffset, NUM_STORAGE_CLASSES},
    Database,
};

use super::msg::ProfileMsg;

const FREQ_LEN: usize = 10;

/// Implementation of Least Frequently Used

pub struct Lfu<M: ObjectRef + Clone, C: DatabaseBuilder> {
    leafs: [HashMap<DiskOffset, (f32, VecDeque<ProfileMsg<M>>)>; NUM_STORAGE_CLASSES],
    rx: Receiver<ProfileMsg<M>>,
    db: Arc<RwLock<Database<C>>>,
}

impl<
        D,
        I,
        G: Copy,
        M: ObjectRef<ObjectPointer = crate::data_management::impls::ObjectPointer<D, I, G>> + Clone,
        C: DatabaseBuilder,
    > super::MigrationPolicy<C> for Lfu<M, C>
{
    type Message = ProfileMsg<M>;

    fn build(rx: Receiver<ProfileMsg<M>>, db: Arc<RwLock<Database<C>>>) -> Self {
        Self {
            leafs: [(); NUM_STORAGE_CLASSES].map(|_| Default::default()),
            rx,
            db,
        }
    }

    fn action(&self, storage_tier: u8) -> super::errors::Result<()> {
        todo!()
    }

    fn db(&self) -> &Arc<RwLock<Database<C>>> {
        &self.db
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
                        entry.1.push_front(msg);
                        entry.1.truncate(FREQ_LEN);
                        // Only classify as soon as enough entries are collected
                        if let (Some(first), Some(last)) =
                            (entry.1.get(0), entry.1.get(FREQ_LEN - 1))
                        {
                            match (first, last) {
                                (ProfileMsg::Fetch(f), ProfileMsg::Fetch(l))
                                | (ProfileMsg::Fetch(f), ProfileMsg::Write(l))
                                | (ProfileMsg::Write(f), ProfileMsg::Fetch(l))
                                | (ProfileMsg::Write(f), ProfileMsg::Write(l)) => {
                                    entry.0 = FREQ_LEN as f32
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
                            (0.0, vec![msg].into()),
                        );
                    }
                }
                ProfileMsg::Remove(mid) => {
                    let offset = mid.get_unmodified().unwrap().offset();
                    self.leafs[offset.storage_class() as usize].remove(&offset);
                }
                // This policy ignores all other messages.
                _ => {}
            }
        }
        Ok(())
    }
}
