//! Storage stack with key-value store interface on top of B<sup>e</sup>-Trees.
#![warn(missing_docs)]
// We have a number of functions which are only implemented and not further
// referenced right now.  To avoid numerous warnings on build, we allow dead
// code as the alternative would be allowing each single method only provided on
// a maybe needed in the future basis.
#![allow(dead_code)]

extern crate bincode;
extern crate byteorder;
extern crate core;
#[macro_use]
extern crate error_chain;
extern crate futures;
extern crate itertools;
extern crate libc;
#[macro_use]
extern crate log;
extern crate owning_ref;
extern crate parking_lot;
#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use]
extern crate quickcheck_macros;
#[cfg(test)]
extern crate rand;
extern crate seqlock;
extern crate serde;
extern crate stable_deref_trait;
extern crate twox_hash;
#[cfg(test)]
#[macro_use]
extern crate bencher;

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
    storage_pool::{AtomicStoragePreference, StoragePoolConfiguration, StoragePreference, PreferredAccessType},
};
