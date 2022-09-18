mod errors;
mod lfu;
mod msg;
mod reinforcment_learning;

use crossbeam_channel::Receiver;
use errors::*;
use itertools::Itertools;
pub(crate) use msg::*;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

use crate::{
    data_management::{DmlWithHandler, Handler},
    database::DatabaseBuilder,
    storage_pool::{DiskOffset, NUM_STORAGE_CLASSES},
    vdev::Block,
    Database, StoragePreference,
};

use self::{
    lfu::{Lfu, LfuConfig},
    reinforcment_learning::{RlConfig, ZhangHellanderToor},
};

/// Available policies for auto migrations.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum MigrationPolicies {
    /// Least frequently used, promote and demote nodes based on their usage in the current session.
    Lfu(MigrationConfig<LfuConfig>),
    /// Reinforcment Learning based tier classfication based on Zhang et al. (2022)
    ReinforcementLearning(MigrationConfig<Option<RlConfig>>),
}

impl MigrationPolicies {
    pub(crate) fn construct<C: DatabaseBuilder + Clone>(
        self,
        dml_rx: Receiver<DmlMsg>,
        db_rx: Receiver<DatabaseMsg<C>>,
        db: Arc<RwLock<Database<C>>>,
        storage_hint_sink: Arc<Mutex<HashMap<DiskOffset, StoragePreference>>>,
    ) -> Box<dyn MigrationPolicy<C>> {
        match self {
            MigrationPolicies::Lfu(config) => {
                Box::new(Lfu::build(dml_rx, db_rx, db, config, storage_hint_sink))
            }
            MigrationPolicies::ReinforcementLearning(config) => {
                Box::new(ZhangHellanderToor::build(dml_rx, db_rx, db, config))
            }
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

impl<Config> MigrationConfig<Config> {
    /// Create an erased version of this configuration with the specific
    /// migration policy options removed.
    fn erased(self) -> MigrationConfig<()> {
        MigrationConfig {
            policy_config: (),
            grace_period: self.grace_period,
            migration_threshold: self.migration_threshold,
            update_period: self.update_period,
        }
    }
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
pub(crate) trait MigrationPolicy<C: DatabaseBuilder + Clone> {
    // /// Perform all available operations on a preset storage tier.
    // fn action(&mut self, storage_tier: u8) -> Result<Block<u32>>;

    // Consume all present messages and update the migration selection
    // status for all afflicted objects
    fn update(&mut self) -> Result<()>;

    fn metrics(&self) -> Result<()>;

    fn promote(&mut self, storage_tier: u8) -> Result<Block<u64>>;
    fn demote(&mut self, storage_tier: u8, desired: Block<u64>) -> Result<Block<u64>>;

    // Getters
    fn db(&self) -> &Arc<RwLock<Database<C>>>;

    fn dmu(&self) -> &Arc<<C as DatabaseBuilder>::Dmu>;

    fn config(&self) -> MigrationConfig<()>;

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

            for ((high_tier, _high_info)) in infos
                .iter()
                .skip(1)
                .filter(|((_, high_info))| {
                    high_info.total != Block(0)
                })
            {
                self.promote(*high_tier)?;
            }

            // Update after iteration
            let infos: Vec<(u8, StorageInfo)> = (0u8..NUM_STORAGE_CLASSES as u8)
                .filter_map(|class| {
                    self.dmu()
                        .handler()
                        .get_free_space_tier(class)
                        .map(|blocks| (class, blocks))
                })
                .collect();

            for ((high_tier, high_info), (_low_tier, _low_info)) in infos
                .iter()
                .tuple_windows()
                .filter(|((_, high_info), (_, low_info))| {
                    high_info.percent_full() > threshold && low_info.percent_full() < threshold
                })
            {
                // TODO: Calculate moving size, until threshold barely not fulfilled?
                let desired: Block<u64> =
                    Block((high_info.total.as_u64() as f32 * (1.0 - threshold)) as u64)
                        - high_info.free.as_u64();
                self.demote(*high_tier, desired)?;
            }
            self.metrics()?;
        }
    }
}
