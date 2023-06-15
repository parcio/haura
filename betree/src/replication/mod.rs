//! A replication structure can be used by the engine to store highly requested
//! data on a "replication layer" or persistent cache. The advantage is that
//! access to slow media can be avoided if data is found in this layer.
//!
//! Multiple Layered Layers: 1. Cache
//!                          2. Replication
//!                          3. Disks
//!                             3.1 FASTEST
//!                             3.2 FAST
//!                             3.3 SLOW
//!                             3.4 SLOWEST
//!
//!
//! ```
//!                   1
//!                  / \
//!                 1   1
//!               / \   / \
//!              1  2   1  2
//!             /|\ |  /|\ |\
//!             321 2  313 22
//! ```
//!
//! Map Keys
//! ========
//!
//! - `0[hash]`  data key-value pairs
//! - `1[hash]` - Lru node keys
//! - `2` - Lru root node

const PREFIX_KV: u8 = 0;
const PREFIX_LRU: u8 = 1;
const PREFIX_LRU_ROOT: u8 = 2;

use std::{
    hash::Hash,
    ops::{Deref, DerefMut},
    path::PathBuf,
    ptr::NonNull,
};

use pmem_hashmap::{PMap, PMapError};

mod lru;
use lru::Plru;
use serde::{Deserialize, Serialize};

use self::lru::LruKey;

/// A pointer to a region in persistent memory.
pub struct Persistent<T>(NonNull<T>);
// Pointer to persistent memory can be assumed to be non-thread-local
unsafe impl<T> Send for Persistent<T> {}
impl<T> Deref for Persistent<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}
impl<T> DerefMut for Persistent<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

/// Internally used key to access data store.
pub struct DataKey(u64);

impl DataKey {
    /// Create an internal store from an external key.
    pub fn from<K: Hash>(key: K, pmap: &mut PMap) -> Self {
        DataKey(pmap.hash(key))
    }

    /// Expose as byte range for FFI use.
    pub fn key(&self) -> [u8; 9] {
        let mut key = [0; 9];
        key[0] = PREFIX_KV;
        key[1..].copy_from_slice(&self.0.to_le_bytes());
        key
    }

    /// Convert to an internally used representation for LRU entries.
    pub fn to_lru_key(&self) -> LruKey {
        LruKey::from(self.0)
    }
}

/// Persistent byte array cache. Optimized for read performance, avoid frequent
/// updates.
pub struct PersistentCache {
    pmap: PMap,
    // Persistent Pointer
    lru: Persistent<Plru>,
}

/// Configuration for a persistent cache.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PersistentCacheConfig {
    /// Path to underlying file representing cache.
    pub path: PathBuf,
    /// Cache capacity in bytes.
    pub bytes: usize,
}

impl PersistentCache {
    /// Open an existent [PersistentCache]. Fails if no cache exist or invalid.
    pub fn open<P: Into<std::path::PathBuf>>(path: P) -> Result<Self, PMapError> {
        let mut pmap = PMap::open(path.into())?;
        Ok(Self {
            lru: Plru::open(&mut pmap)?,
            pmap,
        })
    }

    /// Create a new [PersistentCache] in the specified path. Fails if underlying resources are not valid.
    pub fn create<P: Into<std::path::PathBuf>>(path: P, size: usize) -> Result<Self, PMapError> {
        let mut pmap = PMap::create(path.into(), size)?;
        Ok(Self {
            lru: Plru::create(&mut pmap, size as u64)?,
            pmap,
        })
    }

    /// Fetch an entry from the hashmap.
    pub fn get<K: Hash>(&mut self, key: K) -> Result<&[u8], PMapError> {
        let k = DataKey::from(key, &mut self.pmap);
        self.lru.touch(&mut self.pmap, k.to_lru_key())?;
        Ok(self.pmap.get(k.key())?)
    }

    /// Insert an entry and remove infrequent values.
    pub fn insert<K: Hash>(&mut self, key: K, value: &[u8]) -> Result<(), PMapError> {
        // TODO: Update old values? Convenience
        let k = DataKey::from(key, &mut self.pmap);
        self.pmap.insert(k.key(), value)?;
        self.lru
            .insert(&mut self.pmap, k.to_lru_key(), value.len() as u64)?;
        while let Some(id) = self.lru.evict() {
            self.pmap.remove(id.key())?;
            self.lru.remove(&mut self.pmap, id.to_lru_key())?;
        }
        Ok(())
    }

    /// Remove an entry.
    pub fn remove<K: Hash>(&mut self, key: K) -> Result<(), PMapError> {
        let k = DataKey::from(key, &mut self.pmap);
        self.pmap.remove(k.key())?;
        self.lru.remove(&mut self.pmap, k.to_lru_key())?;
        Ok(())
    }
}
