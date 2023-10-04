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

use crossbeam_channel::Sender;
use parking_lot::RwLock;
use pmem_hashmap::{
    allocator::{Pal, PalPtr},
    PMap, PMapError,
};
use std::{
    collections::BTreeMap,
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::size_of,
    ops::{Deref, DerefMut},
    path::PathBuf,
    ptr::NonNull,
    thread::JoinHandle,
};
use twox_hash::XxHash64;
use zstd_safe::WriteBuf;

mod lru;
mod lru_worker;
use lru::Plru;
use serde::{Deserialize, Serialize};

use crate::buffer::Buf;

use self::lru::PlruNode;

/// A pointer to a region in persistent memory.
pub struct Persistent<T>(PalPtr<T>);
// Pointer to persistent memory can be assumed to be non-thread-local
unsafe impl<T> Send for Persistent<T> {}
impl<T> Deref for Persistent<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.load() }
    }
}
impl<T> DerefMut for Persistent<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.load_mut() }
    }
}

impl<T> Drop for Persistent<T> {
    fn drop(&mut self) {
        // NO-OP
    }
}

// TODO: Is this really safe?
unsafe impl<T> Sync for Persistent<PCacheRoot<T>> {}

/// Persistent byte array cache. Optimized for read performance, avoid frequent
/// updates.
pub struct PersistentCache<K, T> {
    pal: Pal,
    root: Persistent<PCacheRoot<T>>,
    tx: Sender<lru_worker::Msg<T>>,
    placement_tx: Option<Sender<crate::migration::ReplicationMsg>>,
    hndl: Option<JoinHandle<()>>,
    // Fix key types
    key_type: PhantomData<K>,
}

impl<K, T> Drop for PersistentCache<K, T> {
    fn drop(&mut self) {
        // Spin while the queue needs to be processed
        self.tx
            .send(lru_worker::Msg::Close)
            .expect("Thread panicked.");
        self.hndl
            .take()
            .expect("Thread handle has been empty?")
            .join();
        self.pal.close();
    }
}

pub struct PCacheRoot<T> {
    map: BTreeMap<u64, PCacheMapEntry<T>, Pal>,
    lru: RwLock<Plru<T>>,
}

#[derive(Debug)]
pub struct PCacheMapEntry<T> {
    size: usize,
    lru_node: PalPtr<PlruNode<T>>,
    data: PalPtr<u8>,
}

/// Configuration for a persistent cache.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PersistentCacheConfig {
    /// Path to underlying file representing cache.
    pub path: PathBuf,
    /// Cache capacity in bytes.
    pub bytes: usize,
}

/// Ephemeral struct created in the preparation for an insertion.
pub struct PersistentCacheInsertion<'a, K, T> {
    cache: &'a mut PersistentCache<K, T>,
    key: u64,
    value: Buf,
    baggage: T,
}

impl<'a, K, T: Clone> PersistentCacheInsertion<'a, K, T> {
    /// Performs an execution and calls the given function on each evicted entry
    /// from the store. On error the entire insertion is aborted and has to be
    /// initiated anew.
    pub fn insert<F>(self, f: F) -> Result<(), PMapError>
    where
        F: Fn(&T, Buf) -> Result<(), crate::vdev::Error>,
    {
        loop {
            let key = {
                let lock = self.cache.root.lru.read();
                let res = lock.evict(self.value.len() as u64);
                match res {
                    Ok(Some((key, baggage))) => {
                        // let data = self.cache.pmap.get(key.key())?;
                        let entry = self.cache.root.map.get(&key).unwrap();
                        let data = unsafe {
                            core::slice::from_raw_parts(entry.data.load() as *const u8, entry.size)
                        };

                        let buf = Buf::from_persistent_ptr(entry.data, entry.size as u32);
                        if f(baggage, buf).is_err() {
                            return Err(PMapError::ExternalError("Writeback failed".into()));
                        }
                        key
                    }
                    _ => break,
                }
            };
            // Finally actually remove the entries
            let mut entry = self.cache.root.map.remove(&key).unwrap();
            entry.data.free();
            self.cache.placement_tx.map(|tx| tx.send(crate::migration::ReplicationMsg::Evict()))
            self.cache.tx.send(lru_worker::Msg::Remove(entry.lru_node));
        }

        let lru_ptr = self.cache.pal.allocate(lru::PLRU_NODE_SIZE).unwrap();
        let data = self.value.as_slice_with_padding();
        let data_ptr = self.cache.pal.allocate(data.len()).unwrap();
        data_ptr.copy_from(data, &self.cache.pal);
        self.cache.tx.send(lru_worker::Msg::Insert(
            lru_ptr.clone(),
            self.key,
            data.len() as u64,
            self.baggage,
        ));
        let map_entry = PCacheMapEntry {
            lru_node: lru_ptr,
            data: data_ptr,
            size: data.len(),
        };
        self.cache.root.map.insert(self.key, map_entry);
        Ok(())
    }
}

impl<K: Hash, T: Send + 'static> PersistentCache<K, T> {
    /// Open an existent [PersistentCache]. Fails if no cache exist or invalid.
    pub fn open<P: Into<std::path::PathBuf>>(path: P) -> Result<Self, PMapError> {
        let pal = Pal::open(path.into()).unwrap();
        let root = pal.root(size_of::<PCacheRoot<T>>()).unwrap();
        let (tx, rx) = crossbeam_channel::unbounded();
        let root_lru = Persistent(root.clone());
        let hndl = std::thread::spawn(move || lru_worker::main(rx, root_lru));
        let root = Persistent(root);
        Ok(Self {
            pal,
            tx,
            placement_tx: None,
            root,
            hndl: Some(hndl),
            key_type: PhantomData::default(),
        })
    }

    /// Create a new [PersistentCache] in the specified path. Fails if underlying resources are not valid.
    pub fn create<P: Into<std::path::PathBuf>>(path: P, size: usize) -> Result<Self, PMapError> {
        let mut pal = Pal::create(path.into(), size, 0o666).unwrap();
        let mut root: PalPtr<PCacheRoot<T>> = pal.root(size_of::<PCacheRoot<T>>()).unwrap();
        root.init(
            &PCacheRoot {
                lru: RwLock::new(Plru::init(size as u64)),
                map: BTreeMap::new_in(pal.clone()),
            },
            std::mem::size_of::<PCacheRoot<T>>(),
        );
        let (tx, rx) = crossbeam_channel::unbounded();
        let root_lru = Persistent(root.clone());
        let hndl = std::thread::spawn(move || lru_worker::main(rx, root_lru));
        let mut root = Persistent(root);
        Ok(Self {
            pal,
            tx,
            placement_tx: None,
            root,
            hndl: Some(hndl),
            key_type: PhantomData::default(),
        })
    }

    /// Fetch an entry from the hashmap.
    pub fn get(&self, key: K) -> Result<&[u8], PMapError> {
        let mut hasher = XxHash64::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let res = self.root.map.get(&hash);
        if let Some(entry) = res {
            self.tx.send(lru_worker::Msg::Touch(entry.lru_node));
            // self.root.lru.touch(&entry.lru_node)?;
            Ok(unsafe { core::slice::from_raw_parts(entry.data.load() as *const u8, entry.size) })
        } else {
            Err(PMapError::DoesNotExist)
        }
    }

    /// Return a [Buf].
    ///
    /// TODO: We have to pin these entries to ensure that they may not be
    /// evicted while in read-only mode.
    pub fn get_buf(&self, key: K) -> Result<Buf, PMapError> {
        let mut hasher = XxHash64::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let res = self.root.map.get(&hash);
        if let Some(entry) = res {
            self.tx.send(lru_worker::Msg::Touch(entry.lru_node));
            // self.root.lru.touch(&entry.lru_node)?;
            Ok(Buf::from_persistent_ptr(entry.data, entry.size as u32))
        } else {
            Err(PMapError::DoesNotExist)
        }
    }

    /// Start an insertion. An insertion can only be successfully completed if values are properly evicted from the cache
    pub fn prepare_insert<'a>(
        &'a mut self,
        key: K,
        value: Buf,
        baggage: T,
    ) -> PersistentCacheInsertion<'a, K, T> {
        let mut hasher = XxHash64::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        PersistentCacheInsertion {
            cache: self,
            key: hash,
            value,
            baggage,
        }
    }

    /// Remove an entry.
    pub fn remove(&mut self, key: K) -> Result<(), PMapError> {
        let mut hasher = XxHash64::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        if let Some(mut entry) = self.root.map.remove(&hash) {
            self.tx.send(lru_worker::Msg::Remove(entry.lru_node));
            // self.root.lru.remove(&mut entry.lru_node).unwrap();
            // entry.lru_node.free();
            entry.data.free();
            Ok(())
        } else {
            Err(PMapError::DoesNotExist)
        }
    }
}
