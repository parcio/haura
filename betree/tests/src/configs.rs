use betree_storage_stack::{
    database::AccessMode,
    migration::{MigrationConfig, MigrationPolicies},
    storage_pool::{configuration::Vdev, LeafVdev, TierConfiguration},
    DatabaseConfiguration, StoragePoolConfiguration,
};

use crate::TO_MEBIBYTE;

pub(crate) fn migration_config() -> DatabaseConfiguration {
    DatabaseConfiguration {
        storage: StoragePoolConfiguration {
            tiers: vec![TierConfiguration {
                top_level_vdevs: vec![Vdev::Leaf(LeafVdev::Memory {
                    mem: 2048 * TO_MEBIBYTE,
                })],
            }],
            ..Default::default()
        },
        access_mode: AccessMode::OpenOrCreate,
        migration_policy: Some(MigrationPolicies::Lfu(MigrationConfig {
            grace_period: std::time::Duration::from_millis(0),
            migration_threshold: 0.7,
            update_period: std::time::Duration::from_millis(100),
        })),
        ..Default::default()
    }
}

pub(crate) fn migration_config_file_backed() -> DatabaseConfiguration {
    DatabaseConfiguration {
        storage: StoragePoolConfiguration {
            tiers: vec![TierConfiguration {
                top_level_vdevs: vec![Vdev::Leaf(LeafVdev::File("test_disk_tier_fastest".into()))],
            }],
            ..Default::default()
        },
        migration_policy: Some(MigrationPolicies::Lfu(MigrationConfig {
            grace_period: std::time::Duration::from_millis(0),
            migration_threshold: 0.7,
            update_period: std::time::Duration::from_millis(100),
        })),
        access_mode: AccessMode::OpenOrCreate,
        ..Default::default()
    }
}
