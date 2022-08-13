mod errors;
mod lfu;
mod msg;

use crossbeam_channel::Receiver;
use errors::*;
use itertools::Itertools;
pub(crate) use msg::*;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    data_management::{DmlWithHandler, Handler},
    database::DatabaseBuilder,
    storage_pool::NUM_STORAGE_CLASSES,
    vdev::Block,
    Database,
};

use self::lfu::{Lfu, LfuConfig};

/// Available policies for auto migrations.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub enum MigrationPolicies {
    /// Least frequently used, promote and demote nodes based on their usage in the current session.
    Lfu(MigrationConfig<LfuConfig>),
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
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct MigrationConfig<Config> {
    /// Time at start where operations are _only_ recorded. This may help in avoiding incorrect early migrations by depending on a larger historical data.
    pub grace_period: Duration,
    /// Threshold at which downwards migrations are considered. Or at which upwards migrations are blocked. Values are on a range of 0 to 1.
    pub migration_threshold: f32,
    /// Duration between consumption of operational messages. Enlarging this leads to greater memory usage, but reduces ongoing computational load.
    pub update_period: Duration,
    /// Policy dependent configuration
    pub policy_config: Config,
}

impl<Config: Default> Default for MigrationConfig<Config> {
    fn default() -> Self {
        MigrationConfig {
            grace_period: Duration::from_secs(300),
            migration_threshold: 0.8,
            update_period: Duration::from_secs(30),
            policy_config: Default::default(),
        }
    }
}

// FIXME: Draft, no types are final
pub(crate) trait MigrationPolicy<C: DatabaseBuilder> {
    type ObjectReference;
    type Message;
    type Config;

    fn build(
        rx: Receiver<Self::Message>,
        db: Arc<RwLock<Database<C>>>,
        config: MigrationConfig<Self::Config>,
    ) -> Self;

    // /// Perform all available operations on a preset storage tier.
    // fn action(&mut self, storage_tier: u8) -> Result<Block<u32>>;

    // Consume all present messages and update the migration selection
    // status for all afflicted objects
    fn update(&mut self) -> Result<()>;

    fn promote(&mut self, storage_tier: u8, desired: Block<u32>) -> Result<Block<u32>>;
    fn demote(&mut self, storage_tier: u8, desired: Block<u32>) -> Result<Block<u32>>;

    // Getters
    fn db(&self) -> &Arc<RwLock<Database<C>>>;

    fn dmu(&self) -> &Arc<<C as DatabaseBuilder>::Dmu>;

    fn config(&self) -> &MigrationConfig<Self::Config>;

    /// The main loop of the
    fn thread_loop(&mut self) -> Result<()> {
        std::thread::sleep(self.config().grace_period);
        loop {
            // PAUSE
            std::thread::sleep(self.config().update_period);
            // Consuming all messages and updating internal state.
            self.update()?;

            use crate::database::StorageInfo;

            let threshold = self.config().migration_threshold.clamp(0.0, 1.0);
            let infos: Vec<(u8, StorageInfo)> = (0u8..NUM_STORAGE_CLASSES as u8)
                .filter_map(|class| {
                    self.dmu()
                        .handler()
                        .get_free_space_tier(class)
                        .map(|blocks| (class, blocks))
                })
                .collect();

            for ((_high_tier, _high_info), (low_tier, _low_info)) in infos
                .iter()
                .tuple_windows()
                .filter(|((_, high_info), (_, low_info))| {
                    (high_info.free.as_u64() as f32 / high_info.total.as_u64() as f32)
                        > (1.0 - threshold)
                        && low_info.total != Block(0)
                })
            {
                // TODO: Calculate moving size, until threshold barely not fulfilled?
                self.promote(*low_tier, BATCH)?;
            }
            for ((high_tier, _high_info), (_low_tier, _low_info)) in infos
                .iter()
                .tuple_windows()
                .filter(|((_, high_info), (_, low_info))| {
                    (high_info.free.as_u64() as f32 / high_info.total.as_u64() as f32)
                        < (1.0 - threshold)
                        && (low_info.free.as_u64() as f32 / low_info.total.as_u64() as f32)
                            > (1.0 - threshold)
                })
            {
                // TODO: Calculate moving size, until threshold barely not fulfilled?
                self.demote(*high_tier, BATCH)?;
            }
        }
    }
}

const BATCH: Block<u32> = Block(32768);
