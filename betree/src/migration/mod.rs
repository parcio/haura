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
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub enum MigrationPolicies {
    /// Least frequently used, promote and demote nodes based on their usage in the current session.
    Lfu(MigrationConfig),
}

impl MigrationPolicies {
    pub(crate) fn construct<C: DatabaseBuilder>(
        self,
        rx: Receiver<ProfileMsg<crate::database::ObjectRef>>,
        db: Arc<RwLock<Database<C>>>,
    ) -> impl MigrationPolicy<C> {
        match self {
            MigrationPolicies::Lfu(config) => Lfu::build(rx, db, config),
        }
    }
}

use std::time::Duration;

/// Configuration type for [MigrationPolicy]
/// TODO: Extend for specific policy configuration. Maybe as a trait? Or composed generic.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Time at start where operations are _only_ recorded. This may help in avoiding incorrect early migrations by depending on a larger historical data.
    pub grace_period: Duration,
    /// Threshold at which downwards migrations are considered. Or at which upwards migrations are blocked. Values are on a range of 0 to 1.
    pub migration_threshold: f32,
    /// Duration between consumption of operational messages. Enlarging this leads to greater memory usage, but reduces ongoing computational load.
    pub update_period: Duration,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        MigrationConfig {
            grace_period: Duration::from_secs(300),
            migration_threshold: 0.8,
            update_period: Duration::from_secs(30),
        }
    }
}

// FIXME: Draft, no types are final
pub(crate) trait MigrationPolicy<C: DatabaseBuilder> {
    type ObjectReference;
    type Message;

    fn build(
        rx: Receiver<Self::Message>,
        db: Arc<RwLock<Database<C>>>,
        config: MigrationConfig,
    ) -> Self;

    /// Perform all available operations on a preset storage tier.
    fn action(&self, storage_tier: u8) -> Result<()>;

    // Consume all present messages and update the migration selection
    // status for all afflicted objects
    fn update(&mut self) -> Result<()>;

    // fn promote(&self, storage_tier: u8);
    // fn demote(&self, storage_tier: u8);

    // Getters
    fn db(&self) -> &Arc<RwLock<Database<C>>>;

    fn config(&self) -> &MigrationConfig;

    /// The main loop of the
    fn thread_loop(&mut self) -> Result<()> {
        std::thread::sleep(self.config().grace_period);
        loop {
            std::thread::sleep(self.config().update_period);
            self.update()?;
            if let Some(db) = self.db().try_read() {
                let space_info = db.free_space_tier();
                for (tier, _) in space_info.iter().enumerate().filter(|(_, info)| {
                    (info.free.as_u64() as f32 / info.total.as_u64() as f32)
                        < (1.0 - self.config().migration_threshold.clamp(0.0, 1.0))
                }) {
                    self.action(tier as u8)?;
                }
            }
        }
    }
}
