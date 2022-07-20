mod errors;
mod lfu;
mod msg;

use crossbeam_channel::Receiver;
use errors::*;
pub(crate) use msg::*;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{data_management::ObjectRef, database::DatabaseBuilder, Database};

use self::lfu::Lfu;

/// Available policies for auto migrations.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy)]
pub enum MigrationPolicies {
    /// Least frequently used, promote and demote nodes based on their usage in the current session.
    Lfu,
}

pub(crate) trait BuildPolicy<M: Clone> {
    fn build(rx: Receiver<ProfileMsg<M>>) -> Self;
}

impl MigrationPolicies {
    pub(crate) fn construct<
        D,
        I,
        G: Copy,
        M: Clone + ObjectRef<ObjectPointer = crate::data_management::impls::ObjectPointer<D, I, G>>,
        C: DatabaseBuilder,
    >(
        self,
        rx: Receiver<ProfileMsg<M>>,
        db: Arc<RwLock<Database<C>>>,
    ) -> impl MigrationPolicy<C> {
        match self {
            MigrationPolicies::Lfu => Lfu::build(rx, db),
        }
    }
}

use std::time::Duration;

pub struct MigrationConfig {
    pub grace_period: Duration,
    pub migration_threshold: f32,
    pub update_period: Duration,
}

// FIXME: Draft, no types are final
pub(crate) trait MigrationPolicy<C: DatabaseBuilder> {
    type Message;

    fn build(rx: Receiver<Self::Message>, db: Arc<RwLock<Database<C>>>) -> Self;

    /// Perform all available operations on a preset storage tier.
    fn action(&self, storage_tier: u8) -> Result<()>;

    // Consume all present messages and update the migration selection
    // status for all afflicted objects
    fn update(&mut self) -> Result<()>;

    // fn promote(&self, id: u32);
    // fn demote(&self, id: u32);

    // Getters
    fn db(&self) -> &Arc<RwLock<Database<C>>>;

    /// The main loop of the
    fn thread_loop(&mut self, cfg: MigrationConfig) -> Result<()> {
        std::thread::sleep(cfg.grace_period);
        loop {
            std::thread::sleep(cfg.update_period);
            self.update()?;
            let space_info = self.db().read().free_space_tier();
            for (tier, _) in space_info.iter().enumerate().filter(|(_, info)| {
                (info.free.as_u64() as f32 / info.total.as_u64() as f32)
                    < (1.0 - cfg.migration_threshold.clamp(0.0, 1.0))
            }) {
                self.action(tier as u8)?;
            }
        }
    }
}
