#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::hash::{Hash, Hasher};

use thiserror::Error;
use twox_hash;
#[derive(Error, Debug)]
pub enum PMapError {
    #[error("Could not deduce path: `{0}`")]
    InvalidPath(#[from] std::ffi::NulError),
    #[error("Error: `{0}`")]
    ExternalError(String),
}
pub struct PMap {
    inner: hashmap_tx_toid,
    pobjpool: *mut PMEMobjpool,
}

impl PMap {
    pub fn open<P: Into<std::path::PathBuf>>(path: P) -> Result<Self, PMapError> {
        let pobjpool = {
            let path =
                std::ffi::CString::new(path.into().to_string_lossy().into_owned())?.into_raw();
            unsafe { pmemobj_open(path, std::ptr::null()) }
        };
        if pobjpool.is_null() {
            return Err(PMapError::ExternalError(format!("{}", errno::errno())));
        }
        Self::new(pobjpool)
    }

    pub fn create<P: Into<std::path::PathBuf>>(path: P, size: usize) -> Result<Self, PMapError> {
        let pobjpool = {
            let path =
                std::ffi::CString::new(path.into().to_string_lossy().into_owned())?.into_raw();
            unsafe { pmemobj_create(path, std::ptr::null(), size, 0o666) }
        };
        if pobjpool.is_null() {
            return Err(PMapError::ExternalError(format!("{}", errno::errno())));
        }
        Self::new(pobjpool)
    }

    pub fn new(pobjpool: *mut PMEMobjpool) -> Result<Self, PMapError> {
        let root_obj =
            unsafe { access_root(pmemobj_root(pobjpool, std::mem::size_of::<root_toid>())) };
        if unsafe { root_needs_init(root_obj) != 0 } {
            if unsafe {
                hm_tx_create(
                    pobjpool,
                    &mut (*root_obj).map,
                    std::ptr::null::<std::ffi::c_void>() as *mut std::ffi::c_void,
                )
            } != 0
            {
                return Err(PMapError::ExternalError(format!("{}", errno::errno())));
            }
        } else {
            unsafe { hm_tx_init(pobjpool, (*root_obj).map) };
        }

        Ok(Self {
            inner: unsafe { (*root_obj).map },
            pobjpool,
        })
    }

    pub fn insert<K: Hash>(&mut self, key: K, val: &[u8]) {
        let mut hasher = twox_hash::XxHash64::default();
        key.hash(&mut hasher);
        let k = hasher.finish();

        let mut oid = std::mem::MaybeUninit::<PMEMoid>::uninit();
        unsafe { pmemobj_zalloc(self.pobjpool, oid.as_mut_ptr(), 8 + val.len(), 2) };

        let mut mv = unsafe { access_map_value(oid.assume_init()) };
        unsafe {
            (*mv).len = val.len() as u64;
            (*mv).buf.as_mut_slice(val.len()).copy_from_slice(val);
        }

        unsafe { hm_tx_insert(self.pobjpool, self.inner, k, oid.assume_init()) };
    }

    pub fn remove<K: Hash>(&mut self, key: K) {
        let mut hasher = twox_hash::XxHash64::default();
        key.hash(&mut hasher);
        let k = hasher.finish();

        unsafe {
            hm_tx_remove(self.pobjpool, self.inner, k);
        }
    }

    pub fn get<K: Hash>(&mut self, key: K) -> &mut [u8] {
        let mut hasher = twox_hash::XxHash64::default();
        key.hash(&mut hasher);
        let k = hasher.finish();
        let mv = unsafe { access_map_value(hm_tx_get(self.pobjpool, self.inner, k)) };
        unsafe { (*mv).buf.as_mut_slice((*mv).len as usize) }
    }

    pub fn len(&mut self) -> usize {
        unsafe { hm_tx_count(self.pobjpool, self.inner) }
    }

    pub fn lookup<K: Hash>(&mut self, key: K) -> bool {
        let mut hasher = twox_hash::XxHash64::default();
        key.hash(&mut hasher);
        let k = hasher.finish();
        unsafe { hm_tx_lookup(self.pobjpool, self.inner, k) == 1 }
    }

    pub fn close(self) {
        unsafe { pmemobj_close(self.pobjpool) };
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
        pmap.insert(b"foobar", &[0]);
        assert!(pmap.lookup(b"foobar"));
    }

    #[test]
    fn get() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        let val = [1, 2, 3, 4];
        pmap.insert("foobar", &val);
        let foo = pmap.get("foobar");
        assert!(foo == &[1u8, 2, 3, 4] as &[_])
    }

    #[test]
    fn insert() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        pmap.insert("foobar", &[1, 2, 3, 4]);
    }

    #[test]
    fn len() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        assert_eq!(pmap.len(), 0);
        pmap.insert("foobar", &[1, 2, 3, 4]);
        assert_eq!(pmap.len(), 1);
        pmap.insert("barfoo", &[1, 2]);
        assert_eq!(pmap.len(), 2);
        pmap.insert("foobar", &[1]);
        assert_eq!(pmap.len(), 2);
    }

    #[test]
    fn remove() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        pmap.insert(b"foo", &[1, 2, 3]);
        assert!(pmap.lookup(b"foo"));
        pmap.remove(b"foo");
        assert!(!pmap.lookup(b"foo"));
    }

    #[test]
    fn reopen() {
        let file = TestFile::new();
        {
            let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
            pmap.insert(b"foo", &[1, 2, 3]);
            assert!(pmap.lookup(b"foo"));
            pmap.close();
        }
        {
            let mut pmap = PMap::open(file.path()).unwrap();
            assert!(pmap.lookup(b"foo"));
            assert_eq!(pmap.get(b"foo"), [1, 2, 3]);
            pmap.close();
        }
    }
}
