use std::{
    os::unix::prelude::FileExt,
    sync::{Arc, Once, RwLock, RwLockWriteGuard},
};

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
            tiers: vec![
                TierConfiguration {
                    top_level_vdevs: vec![
                        Vdev::Leaf(LeafVdev::Memory {
                            mem: 2048 * TO_MEBIBYTE,
                        }),
                    ],
                    ..Default::default()
                },
                TierConfiguration {
                    top_level_vdevs: vec![
                        Vdev::Leaf(LeafVdev::Memory {
                            mem: 2048 * TO_MEBIBYTE,
                        }),
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        access_mode: AccessMode::OpenOrCreate,
        migration_policy: Some(MigrationPolicies::Lfu(MigrationConfig {
            grace_period: std::time::Duration::from_millis(0),
            migration_threshold: 0.7,
            update_period: std::time::Duration::from_millis(100),
            policy_config: Default::default(),
        })),
        default_storage_class: 1,
        ..Default::default()
    }
}

static mut FILE_BACKED_CONFIG: Option<RwLock<DatabaseConfiguration>> = None;

static FILE_BACKED_CONFIG_INIT: Once = Once::new();

// NOTE: A bit hacky solution to get somehting like a singleton working for limited resources
//       like files in this case. Arguably still error-prone with the combination of the file_backed_config
//       as a fixture the guard is kept alive and guarantees.
pub(crate) fn migration_config_file_backed() -> RwLockWriteGuard<'static, DatabaseConfiguration> {
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
            migration_policy: Some(MigrationPolicies::Lfu(MigrationConfig {
                grace_period: std::time::Duration::from_millis(0),
                migration_threshold: 0.7,
                update_period: std::time::Duration::from_millis(100),
                policy_config: Default::default(),
            })),
            access_mode: AccessMode::AlwaysCreateNew,
            ..Default::default()
        }));
    });
    unsafe {
        let guard = FILE_BACKED_CONFIG.as_ref().unwrap();
        guard.write().unwrap()
    }
}
