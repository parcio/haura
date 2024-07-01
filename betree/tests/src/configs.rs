use std::sync::{Once, RwLock, RwLockWriteGuard};

use betree_storage_stack::{
    database::AccessMode,
    migration::{LfuConfig, LfuMode, MigrationConfig, MigrationPolicies},
    storage_pool::{configuration::Vdev, LeafVdev, TierConfiguration},
    tree::StorageKind,
    DatabaseConfiguration, StoragePoolConfiguration,
};

use crate::TO_MEBIBYTE;

pub fn access_specific_config() -> DatabaseConfiguration {
    DatabaseConfiguration {
        storage: StoragePoolConfiguration {
            tiers: vec![
                TierConfiguration {
                    top_level_vdevs: vec![Vdev::Leaf(LeafVdev::Memory {
                        mem: 2048 * TO_MEBIBYTE,
                    })],
                    preferred_access_type:
                        betree_storage_stack::PreferredAccessType::RandomReadWrite,
                    storage_kind: StorageKind::Ssd,
                },
                TierConfiguration {
                    top_level_vdevs: vec![Vdev::Leaf(LeafVdev::Memory {
                        mem: 2048 * TO_MEBIBYTE,
                    })],
                    preferred_access_type:
                        betree_storage_stack::PreferredAccessType::SequentialReadWrite,
                    storage_kind: StorageKind::Hdd,
                },
            ],
            ..Default::default()
        },
        access_mode: AccessMode::OpenOrCreate,
        ..Default::default()
    }
}

pub fn migration_config_lfu_node() -> DatabaseConfiguration {
    migration_config_lfu(LfuMode::Node)
}

pub fn migration_config_lfu_object() -> DatabaseConfiguration {
    migration_config_lfu(LfuMode::Object)
}

pub fn migration_config_lfu_both() -> DatabaseConfiguration {
    migration_config_lfu(LfuMode::Both)
}

fn migration_config_lfu(mode: LfuMode) -> DatabaseConfiguration {
    DatabaseConfiguration {
        storage: StoragePoolConfiguration {
            tiers: vec![
                TierConfiguration {
                    top_level_vdevs: vec![Vdev::Leaf(LeafVdev::Memory {
                        mem: 2048 * TO_MEBIBYTE,
                    })],
                    ..Default::default()
                },
                TierConfiguration {
                    top_level_vdevs: vec![Vdev::Leaf(LeafVdev::Memory {
                        mem: 2048 * TO_MEBIBYTE,
                    })],
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        access_mode: AccessMode::OpenOrCreate,
        migration_policy: Some(MigrationPolicies::Lfu(MigrationConfig {
            grace_period: std::time::Duration::from_millis(0),
            migration_threshold: [0.7; 4],
            update_period: std::time::Duration::from_secs(1),
            policy_config: LfuConfig {
                mode,
                ..LfuConfig::default()
            },
        })),
        default_storage_class: 1,
        ..Default::default()
    }
}

pub(crate) fn migration_config_rl() -> DatabaseConfiguration {
    DatabaseConfiguration {
        storage: StoragePoolConfiguration {
            tiers: vec![
                TierConfiguration {
                    top_level_vdevs: vec![Vdev::Leaf(LeafVdev::Memory {
                        mem: 2048 * TO_MEBIBYTE,
                    })],
                    ..Default::default()
                },
                TierConfiguration {
                    top_level_vdevs: vec![Vdev::Leaf(LeafVdev::Memory {
                        mem: 2048 * TO_MEBIBYTE,
                    })],
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        access_mode: AccessMode::OpenOrCreate,
        migration_policy: Some(MigrationPolicies::ReinforcementLearning(MigrationConfig {
            grace_period: std::time::Duration::from_millis(0),
            migration_threshold: [0.7; 4],
            update_period: std::time::Duration::from_millis(100),
            policy_config: None,
        })),
        default_storage_class: 1,
        ..Default::default()
    }
}

static mut FILE_BACKED_CONFIG: Option<RwLock<DatabaseConfiguration>> = None;

static FILE_BACKED_CONFIG_INIT: Once = Once::new();

// NOTE: A bit hacky solution to get somehting like a singleton working for
//       limited resources like files in this case. Arguably still error-prone
//       with the combination of the file_backed_config as a fixture the guard
//       is kept alive and guarantees.
pub(crate) fn file_backed() -> RwLockWriteGuard<'static, DatabaseConfiguration> {
    FILE_BACKED_CONFIG_INIT.call_once(|| unsafe {
        FILE_BACKED_CONFIG = Some(RwLock::new(DatabaseConfiguration {
            storage: StoragePoolConfiguration {
                tiers: vec![TierConfiguration {
                    top_level_vdevs: vec![Vdev::Leaf(LeafVdev::File(
                        "test_disk_tier_fastest".into(),
                    ))],
                    ..Default::default()
                }],
                ..Default::default()
            },
            migration_policy: None,
            access_mode: AccessMode::AlwaysCreateNew,
            ..Default::default()
        }));
    });
    unsafe {
        let guard = FILE_BACKED_CONFIG.as_ref().unwrap();
        guard.write().unwrap()
    }
}
