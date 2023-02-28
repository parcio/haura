//! Automated migration policies which can migrate both nodes and objects in
//! Haura.
//!
//! There are multiple policies available, they can be found in the
//! [MigrationPolicies] enum.
//!
//! # Usage
//!
//! The migration policy has to be initiated with the initialization of the
//! database itself. See the configuration options of
//! [crate::database::DatabaseConfiguration].  Some policies may have additional
//! configuration options which can be seen in the specific documentation of the
//! policies.
//!
//! For example a simple LFU configuration which moves 1024 blocks of data at
//! once, this configuration could look like this:
//! ```
//! # use betree_storage_stack::{
//! #     database::AccessMode,
//! #     storage_pool::{LeafVdev, TierConfiguration, Vdev},
//! #     Database, DatabaseConfiguration, Dataset, StoragePoolConfiguration, StoragePreference, migration::{MigrationPolicies, MigrationConfig, LfuConfig}, vdev::Block,
//! # };
//! # fn main() {
//! let mut db = Database::build(DatabaseConfiguration {
//!     storage: StoragePoolConfiguration {
//!         tiers: vec![
//!         TierConfiguration::new(vec![Vdev::Leaf(LeafVdev::Memory {
//!             mem: 128 * 1024 * 1024,
//!         })]),
//!         TierConfiguration::new(vec![Vdev::Leaf(LeafVdev::Memory {
//!             mem: 32 * 1024 * 1024,
//!         })]),
//!         ],
//!         ..StoragePoolConfiguration::default()
//!     },
//!     access_mode: AccessMode::AlwaysCreateNew,
//!     migration_policy: Some(MigrationPolicies::Lfu(MigrationConfig {
//!         policy_config: LfuConfig {
//!             promote_size: Block(1024),
//!             ..LfuConfig::default()
//!         },
//!         ..MigrationConfig::default()
//!     })),
//!     ..DatabaseConfiguration::default()
//! }).unwrap();
//! # }
//! ```
//!
//! All policies implement a default configuration which can be used if no
//! specific knowledge is known beforehand. Although, it is good practice to
//! give some help to users for determining a fitting configuration in the
//! policy config documentation. You can find the according documentation from
//! [MigrationPolicies].
//!
//! # Types of Migrations
//!
//! We support two kinds of automated migrations, objects and nodes.
//! **Object migrations** are relatively easy to apply and allow for eager data
//! migration upwards and downwards in the stack.  **Node migration** is more
//! tricky and is currently only implemented lazily via hints given to the DML.
//! This makes downward migrations difficult as the lazy hints are resolved on
//! access.  For further information about this issue and the current state of
//! resolving look at the issue tracker.
//!
//! A policy can use a combination of these migration types and is not forced to
//! use any method over the other. Policies declare in their documentation which
//! kinds are used and how they impact the storage use.
//!
mod errors;
mod lfu;
mod msg;
mod reinforcment_learning;

use crossbeam_channel::Receiver;
use errors::*;
use itertools::Itertools;
pub use lfu::{LfuConfig, LfuMode};
pub(crate) use msg::*;
use parking_lot::{Mutex, RwLock};
pub use reinforcment_learning::RlConfig;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

use crate::{
    data_management::DmlWithHandler, database::RootDmu, storage_pool::NUM_STORAGE_CLASSES,
    tree::PivotKey, vdev::Block, Database, StoragePreference,
};

use self::{lfu::Lfu, reinforcment_learning::ZhangHellanderToor};

/// Available policies for auto migrations.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum MigrationPolicies {
    /// Least frequently used, promote and demote nodes based on their usage in
    /// the current session.  This policy can use either objects, nodes, or a
    /// combination of these. Currently only objects are advised to be used.
    ///
    /// This policy optimistically promotes data as soon as space is available.
    /// Also a size categorization scheme is used to promote objects based on
    /// rough bucket sizes. This is partially due to other research in this area
    /// as for example Ge et al. 2022, as well as performance measurements with
    /// Haura which showed some contradictions to common assumptions due to
    /// Write-optimization.
    ///
    /// # Configuration
    ///
    /// The configuration of this policy has been shown to be finnicky and has
    /// to be chosen relative well to the existing storage utilization and
    /// access patterns. For more information about this look at [LfuConfig].
    Lfu(MigrationConfig<LfuConfig>),
    /// Reinforcment Learning based tier classfication by Vengerov 2008.
    /// This policy only uses objects and allows for a dynamic fitting of
    /// objects to current and experienced access patterns. The approach is
    /// similar to a temperature scaling of objects in the storage stack and has
    /// been shown to determine access frequencies well in randomized scenarios.
    /// If you will be using Haura with both objet stores and key-value stores
    /// this policies might perform suboptimally as it cannot gain a holistic
    /// view of the state of the storage.
    ///
    /// # Configuration
    ///
    /// The configuration is purely informational but may be expanded in the
    /// future to allow for some experimentation with set learning values. They
    /// are closer described in [RlConfig].
    ReinforcementLearning(MigrationConfig<Option<RlConfig>>),
}

impl MigrationPolicies {
    pub(crate) fn construct(
        self,
        dml_rx: Receiver<DmlMsg>,
        db_rx: Receiver<DatabaseMsg>,
        db: Arc<RwLock<Database>>,
        storage_hint_sink: Arc<Mutex<HashMap<PivotKey, StoragePreference>>>,
    ) -> Box<dyn MigrationPolicy> {
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

/// Configuration type for [MigrationPolicies]
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct MigrationConfig<Config> {
    /// Time at start where operations are _only_ recorded. This may help in avoiding incorrect early migrations by depending on a larger historical data.
    pub grace_period: Duration,
    /// Threshold at which downwards migrations are considered. Or at which upwards migrations are blocked. Values are on a range of 0 to 1.
    pub migration_threshold: [f32; NUM_STORAGE_CLASSES],
    /// Duration between consumption of operational messages. Enlarging this leads to greater memory usage, but reduces ongoing computational load.
    pub update_period: Duration,
    /// Policy dependent configuration.
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
            migration_threshold: [0.95; NUM_STORAGE_CLASSES],
            update_period: Duration::from_secs(30),
            policy_config: Default::default(),
        }
    }
}

/// An automated migration policy interface definition.
///
/// If you are adding a new policy also include a new variant in
/// [MigrationPolicies] with a short-hand of your policy name to allow the user
/// to create your policy from the database definition.
///
/// When implementing a migration policy you can use two types of messages which
/// are produced. They are divided by user interface and internal tree
/// representation. These messages are defined in the two message types [DmlMsg] and [DatabaseMsg]
pub(crate) trait MigrationPolicy {
    /// Consume all present messages and update the migration selection
    /// status for all afflicted objects
    fn update(&mut self) -> Result<()>;

    /// Run any relevant metric logic such as accumulation and writing out data.
    fn metrics(&self) -> Result<()>;

    /// Promote any amount of data from the given tier to the next higher one.
    ///
    /// This functions returns how many blocks have been migrated in total. When
    /// using lazy node migration also specify the amount of blocks hinted to be
    /// migrated.
    fn promote(&mut self, storage_tier: u8, tight_space: bool) -> Result<Block<u64>>;
    /// Demote atleast `desired` many blocks from the given storage tier to any
    /// tier lower than the given tier.
    fn demote(&mut self, storage_tier: u8, desired: Block<u64>) -> Result<Block<u64>>;

    /// Return a reference to the active [Database].
    fn db(&self) -> &Arc<RwLock<Database>>;

    /// Return a reference to the underlying DML.
    fn dmu(&self) -> &Arc<RootDmu>;

    /// Return the cleaned configuration.
    fn config(&self) -> MigrationConfig<()>;

    /// The main loop of the migration policy.
    ///
    /// We provide a basic default implementation which may be used or discarded
    /// if desired.
    fn thread_loop(&mut self) -> Result<()> {
        std::thread::sleep(self.config().grace_period);
        loop {
            // PAUSE
            std::thread::sleep(self.config().update_period);
            // Consuming all messages and updating internal state.
            self.update()?;

            use crate::database::StorageInfo;

            let threshold: Vec<f32> = self
                .config()
                .migration_threshold
                .iter()
                .map(|val| val.clamp(0.0, 1.0))
                .collect();
            let infos: Vec<(u8, StorageInfo)> = (0u8..NUM_STORAGE_CLASSES as u8)
                .filter_map(|class| {
                    self.dmu()
                        .handler()
                        .get_free_space_tier(class)
                        .map(|blocks| (class, blocks))
                })
                .collect();

            for ((high_tier, high_info), (low_tier, _low_info)) in infos
                .iter()
                .tuple_windows()
                .filter(|(_, (_, low_info))| low_info.total != Block(0))
            {
                self.promote(
                    *low_tier,
                    high_info.percent_full() >= threshold[*high_tier as usize],
                )?;
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
                .filter(|((high_tier, high_info), (low_tier, low_info))| {
                    high_info.percent_full() > threshold[*high_tier as usize]
                        && low_info.percent_full() < threshold[*low_tier as usize]
                })
            {
                let desired: Block<u64> = Block(
                    (high_info.total.as_u64() as f32 * (1.0 - threshold[*high_tier as usize]))
                        as u64,
                ) - high_info.free.as_u64();
                self.demote(*high_tier, desired)?;
            }
            self.metrics()?;
        }
    }
}
