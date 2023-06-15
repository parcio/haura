//! This crate provides a persistent hashmap which may utilize underlying
//! persistent memory. The hashmap is implmented via transactions on persistent
//! memory which allows for failure-free reconstruction on errors. Furthermore,
//! only byte ranges are supported with arbitrary keys as long as they are
//! hashable.
//!
//! ## Example
//!
//! ``` rust
//! # const TO_MEBIBYTE: usize = 1024 * 1024;
//! use pmem_hashmap::PMap;
//! fn main() {
//!     let mut pmap = PMap::create("/tmp/demo", 32 * TO_MEBIBYTE).unwrap();
//!     let key = b"Hello from \xF0\x9F\xA6\x80!";
//!     pmap.insert(key, &[72, 101, 119, 111]);
//!     assert_eq!(pmap.get(key).unwrap(), [72, 101, 119, 111]);
//!     pmap.close();
//!     # std::process::Command::new("rm")
//!     #            .arg("/tmp/demo")
//!     #            .output()
//!     #            .expect("Could not delete");
//! }
//! ```
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::{
    ffi::c_int,
    hash::{Hash, Hasher},
    ptr::NonNull,
};

#[derive(thiserror::Error, Debug)]
pub enum PMapError {
    #[error("Could not deduce path: `{0}`")]
    InvalidPath(#[from] std::ffi::NulError),
    #[error("External Error: `{0}`")]
    ExternalError(String),
    #[error("Allocation Error: `{0}`")]
    AllocationError(String),
    #[error("Key already exists.")]
    AlreadyExists,
    #[error("Key does not exist.")]
    DoesNotExist,
    #[error("Pool size too small")]
    TooSmall,
}

impl PMapError {
    fn from_insertion(from: c_int) -> Self {
        match from {
            1 => Self::AlreadyExists,
            _ => Self::ExternalError(format!("{}", errno::errno())),
        }
    }
}

/// A persistent hashmap.
///
/// Note that the struct itself does not have any specific type-ness as normally
/// in a hashmap in collections.  As all references have to be self-contained
/// introducing this abstractions hides this fact and may result in erroneous
/// behavior when reading data after reinitialization.
///
/// Additionally, this structure is not thread-safe, and may only ever be used
/// by one actor simultaneously. Changing this would require more work to avoid
/// collisions when operating on the same key.
pub struct PMap {
    inner: hashmap_tx_toid,
    pobjpool: NonNull<PMEMobjpool>,
}

/// We can guarantee that no thread-local shaenanigans are done within our
/// library.
unsafe impl Send for PMap {}

impl PMap {
    /// Open an existing hashmap. Will fail if no hashmap has been created before.
    pub fn open<P: Into<std::path::PathBuf>>(path: P) -> Result<Self, PMapError> {
        let pobjpool = {
            let path =
                std::ffi::CString::new(path.into().to_string_lossy().into_owned())?.into_raw();
            unsafe { pmemobj_open(path, std::ptr::null()) }
        };
        if let Some(valid) = NonNull::new(pobjpool) {
            Self::new(valid)
        } else {
            Err(PMapError::ExternalError(format!("{}", errno::errno())))
        }
    }

    /// Create a new hashmap. Will fail if a hashmap already exists at the specified location.
    pub fn create<P: Into<std::path::PathBuf>>(path: P, size: usize) -> Result<Self, PMapError> {
        if size < 8 * 1024 * 1024 {
            return Err(PMapError::TooSmall);
        }
        let pobjpool = {
            let path =
                std::ffi::CString::new(path.into().to_string_lossy().into_owned())?.into_raw();
            unsafe { pmemobj_create(path, std::ptr::null(), size, 0o666) }
        };
        if let Some(valid) = NonNull::new(pobjpool) {
            Self::new(valid)
        } else {
            Err(PMapError::ExternalError(format!("{}", errno::errno())))
        }
    }

    /// Initialize or Create a new hashmap. For this we check if the root obj is
    /// of the correct type and if map pointer already exists.
    ///
    /// TODO: Use a layout to guarantee compatability?
    fn new(pobjpool: NonNull<PMEMobjpool>) -> Result<Self, PMapError> {
        let root_obj = unsafe {
            access_root(pmemobj_root(
                pobjpool.as_ptr(),
                std::mem::size_of::<root_toid>(),
            ))
        };
        if unsafe { root_needs_init(root_obj) != 0 } {
            if unsafe {
                hm_tx_create(
                    pobjpool.as_ptr(),
                    &mut (*root_obj).map,
                    std::ptr::null::<std::ffi::c_void>() as *mut std::ffi::c_void,
                )
            } != 0
            {
                return Err(PMapError::ExternalError(format!("{}", errno::errno())));
            }
        } else {
            unsafe { hm_tx_init(pobjpool.as_ptr(), (*root_obj).map) };
        }

        Ok(Self {
            inner: unsafe { (*root_obj).map },
            pobjpool,
        })
    }

    /// Insert a key-value pair. The key type might vary between different
    /// insertions as it only has to be hashable. This might lead to confusion
    /// of values with the same hash value, which can be avoided by storing the
    /// original data in here as well.
    pub fn insert<K: Hash>(&mut self, key: K, val: &[u8]) -> Result<(), PMapError> {
        let k = self.hash(key);
        self.insert_hashed(k, val)
    }

    /// Raw "pre-hashed" insertion, which skips the first hashing round.
    pub fn insert_hashed(&mut self, k: u64, val: &[u8]) -> Result<(), PMapError> {
        let mut oid = std::mem::MaybeUninit::<PMEMoid>::uninit();
        if unsafe {
            pmemobj_zalloc(self.pobjpool.as_ptr(), oid.as_mut_ptr(), 8 + val.len(), 2) != 0
        } {
            return Err(PMapError::AllocationError(format!("{}", errno::errno())));
        }

        let mut mv = unsafe { access_map_value(oid.assume_init()) };
        unsafe {
            (*mv).len = val.len() as u64;
            (*mv).buf.as_mut_slice(val.len()).copy_from_slice(val);
        }

        let inserted =
            unsafe { hm_tx_insert(self.pobjpool.as_ptr(), self.inner, k, oid.assume_init()) };
        if inserted != 0 {
            return Err(PMapError::from_insertion(inserted));
        }
        Ok(())
    }

    /// Remove the specified key from the hashmap.
    pub fn remove<K: Hash>(&mut self, key: K) -> Result<(), PMapError> {
        let k = self.hash(key);
        self.remove_hashed(k)
    }

    /// Raw "pre-hashed" removal, which skips the first hashing round.
    pub fn remove_hashed(&mut self, k: u64) -> Result<(), PMapError> {
        let mut pptr = unsafe { hm_tx_remove(self.pobjpool.as_ptr(), self.inner, k) };
        if pptr.off == 0 {
            return Err(PMapError::DoesNotExist);
        }
        unsafe { pmemobj_free(&mut pptr) };
        Ok(())
    }

    /// Raw "pre-hashed" access, which skips the first hashing round.
    pub fn get_hashed(&mut self, k: u64) -> Result<&mut [u8], PMapError> {
        let val = unsafe { hm_tx_get(self.pobjpool.as_ptr(), self.inner, k) };
        if val.off == 0 {
            return Err(PMapError::DoesNotExist);
        }

        let mv = unsafe { access_map_value(val) };
        Ok(unsafe { (*mv).buf.as_mut_slice((*mv).len as usize) })
    }

    /// Return a given value from the hashmap. The key has to be valid
    pub fn get<K: Hash>(&mut self, key: K) -> Result<&mut [u8], PMapError> {
        let k = self.hash(key);
        self.get_hashed(k)
    }

    pub fn len(&mut self) -> usize {
        unsafe { hm_tx_count(self.pobjpool.as_ptr(), self.inner) }
    }

    pub fn is_empty(&mut self) -> bool {
        self.len() == 0
    }

    pub fn lookup<K: Hash>(&mut self, key: K) -> bool {
        let k = self.hash(key);
        unsafe { hm_tx_lookup(self.pobjpool.as_ptr(), self.inner, k) == 1 }
    }

    pub fn close(self) {
        unsafe { pmemobj_close(self.pobjpool.as_ptr()) };
    }

    pub fn hash<K: Hash>(&self, key: K) -> u64 {
        let mut hasher = twox_hash::XxHash64::default();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, process::Command};

    use tempfile::Builder;

    use super::*;

    struct TestFile(PathBuf);

    impl TestFile {
        pub fn new() -> Self {
            TestFile(
                Builder::new()
                    .tempfile()
                    .expect("Could not get tmpfile")
                    .path()
                    .to_path_buf(),
            )
        }

        pub fn path(&self) -> &PathBuf {
            &self.0
        }
    }
    impl Drop for TestFile {
        fn drop(&mut self) {
            if !Command::new("rm")
                .arg(self.0.to_str().expect("Could not pass tmpfile"))
                .output()
                .expect("Could not delete")
                .status
                .success()
            {
                eprintln!("Could not delete tmpfile");
            }
        }
    }

    #[test]
    fn new() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        // Test validity for paranoia
        println!("Map has {} entries", pmap.len());
    }

    #[test]
    fn lookup() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        assert!(!pmap.lookup(123));
        pmap.insert(b"foobar", &[0]).unwrap();
        assert!(pmap.lookup(b"foobar"));
    }

    #[test]
    fn get() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        let val = [1, 2, 3, 4];
        pmap.insert("foobar", &val).unwrap();
        let foo = pmap.get("foobar").unwrap();
        assert!(foo == &[1u8, 2, 3, 4] as &[_])
    }

    #[test]
    fn insert() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        pmap.insert("foobar", &[1, 2, 3, 4]).unwrap();
    }

    #[test]
    fn len() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        assert_eq!(pmap.len(), 0);
        pmap.insert("foobar", &[1, 2, 3, 4]).unwrap();
        assert_eq!(pmap.len(), 1);
        pmap.insert("barfoo", &[1, 2]).unwrap();
        assert_eq!(pmap.len(), 2);
        // pmap.insert("foobar", &[1]).unwrap();
        // assert_eq!(pmap.len(), 2);
    }

    #[test]
    fn remove() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        pmap.insert(b"foo", &[1, 2, 3]).unwrap();
        assert!(pmap.lookup(b"foo"));
        pmap.remove(b"foo").unwrap();
        assert!(!pmap.lookup(b"foo"));
    }

    #[test]
    fn reopen() {
        let file = TestFile::new();
        {
            let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
            pmap.insert(b"foo", &[1, 2, 3]).unwrap();
            assert!(pmap.lookup(b"foo"));
            pmap.close();
        }
        {
            let mut pmap = PMap::open(file.path()).unwrap();
            assert!(pmap.lookup(b"foo"));
            assert_eq!(pmap.get(b"foo").unwrap(), [1, 2, 3]);
            pmap.close();
        }
    }
}
