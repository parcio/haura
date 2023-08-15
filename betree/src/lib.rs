//! Storage stack with key-value store interface on top of B<sup>e</sup>-Trees.
#![warn(missing_docs)]
// We have a number of functions which are only implemented and not further
// referenced right now.  To avoid numerous warnings on build, we allow dead
// code as the alternative would be allowing each single method only provided on
// a maybe needed in the future basis.
#![allow(dead_code)]
#![feature(allocator_api)]
#![feature(btreemap_alloc)]

#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
#[cfg(test)]
#[macro_use]
extern crate quickcheck_macros;

pub mod allocator;
pub mod atomic_option;
pub mod bounded_future_queue;
pub mod buffer;
pub mod c_interface;
pub mod cache;
pub mod checksum;
pub mod compression;
pub mod cow_bytes;
pub mod data_management;
pub mod database;
pub mod range_validation;
#[cfg(feature = "nvm")]
pub mod replication;
pub mod size;
pub mod storage_pool;
pub mod tree;
pub mod vdev;

pub mod metrics;
pub mod object;

pub mod migration;

#[cfg(feature = "init_env_logger")]
pub mod env_logger;

#[cfg(test)]
mod arbitrary;

pub use self::{
    database::{Database, DatabaseConfiguration, Dataset, Error, Snapshot},
    storage_pool::{
        AtomicStoragePreference, PreferredAccessType, StoragePoolConfiguration, StoragePreference,
    },
};
