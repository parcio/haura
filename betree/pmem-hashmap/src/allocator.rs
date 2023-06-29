use super::*;
use errno::errno;
use std::alloc::{AllocError, Allocator};
use std::{ffi::c_void, ptr::NonNull, sync::Arc};
use thiserror::Error;

// A friendly persistent memory allocator.
#[derive(Clone, Debug)]
pub struct Pal {
    pool: Arc<NonNull<pmemobjpool>>,
}

// Parallel access should be fine, but can be exploited. TODO: Add safeguard for this?
unsafe impl Sync for Pal {}
// No thread-local data
unsafe impl Send for Pal {}

impl Drop for Pal {
    fn drop(&mut self) {
        // there should exist no further reference to this resources otherwise we risk some invalid fetches
        if Arc::strong_count(&self.pool) == 0 && Arc::weak_count(&self.pool) == 0 {
            self.close()
        }
    }
}

impl Into<AllocError> for PalError {
    fn into(self) -> AllocError {
        AllocError
    }
}

unsafe impl Allocator for Pal {
    fn allocate(&self, layout: std::alloc::Layout) -> Result<NonNull<[u8]>, AllocError> {
        let ptr = self.allocate(layout.size()).map_err(|_| AllocError)?;
        Ok(NonNull::new(unsafe {
            core::slice::from_raw_parts_mut(ptr.load() as *mut u8, layout.size())
        })
        .ok_or_else(|| AllocError)?)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, _layout: std::alloc::Layout) {
        let mut oid = unsafe { pmemobj_oid(ptr.as_ptr() as *const c_void) };
        unsafe { pmemobj_free(&mut oid) }
    }
}

#[inline]
fn oid_is_null(oid: PMEMoid) -> bool {
    oid.off == 0
}

#[derive(Debug, Error)]
pub enum PalError {
    #[error("Opening failed (`{0}`). This error originates in libpmemobj.")]
    OpenFailed(String),
    #[error("Allocation failed (`{0}`). This error originates in libpmemobj.")]
    AllocationFailed(String),
    #[error("Null")]
    NullEncountered,
    #[error("Could not deduce path: `{0}`")]
    InvalidPath(#[from] std::ffi::NulError),
}

// A friendly persistent pointer. Useless without the according handle to the
// original arena.
#[derive(Debug, Clone, Copy)]
pub struct PalPtr {
    inner: PMEMoid,
    size: usize,
}

impl PalPtr {
    /// Translate this persistent ptr to a volatile one.
    pub fn load(&self) -> *mut c_void {
        unsafe { haura_direct(self.inner) }
    }

    /// Copy a range of bytes behind this pointer to a given buffer. Data is
    /// copied until the allocated area is read entirely or the given buffer is
    /// filled entirely.
    pub fn copy_to(&self, other: &mut [u8], arena: &Pal) {
        unsafe {
            pmemobj_memcpy(
                arena.pool.as_ptr(),
                other.as_mut_ptr() as *mut c_void,
                self.load(),
                self.size.min(other.len()),
                PMEMOBJ_F_MEM_NOFLUSH,
            )
        };
    }

    /// Copy a range of bytes to the location of this pointer. Data is copied
    /// until the allocated area is filled or the given buffer ends.
    pub fn copy_from(&self, other: &[u8], arena: &Pal) {
        unsafe {
            pmemobj_memcpy(
                arena.pool.as_ptr(),
                self.load(),
                other.as_ptr() as *const c_void,
                self.size.min(other.len()),
                PMEMOBJ_F_MEM_NONTEMPORAL,
            );
        };
    }

    /// Deallocate this object. Required if this value is no longer needed.
    /// There is *no* automatic deallocation logic.
    pub fn free(mut self) {
        unsafe { pmemobj_free(&mut self.inner) }
    }
}

// TODO: Impl Deref with typization?

impl Pal {
    /// Open an existing file representing an arena.
    pub fn open<P: Into<std::path::PathBuf>>(path: P) -> Result<Self, PalError> {
        let pobjpool = {
            let path =
                std::ffi::CString::new(path.into().to_string_lossy().into_owned())?.into_raw();
            unsafe { pmemobj_open(path, std::ptr::null()) }
        };
        Self::new(pobjpool)
    }

    /// Create a new arena on persistent memory.
    pub fn create<P: Into<std::path::PathBuf>>(
        path: P,
        bytes: usize,
        permissions: u32,
    ) -> Result<Self, PalError> {
        let pobjpool = {
            let path =
                std::ffi::CString::new(path.into().to_string_lossy().into_owned())?.into_raw();
            unsafe { pmemobj_create(path, std::ptr::null(), bytes, permissions) }
        };
        Self::new(pobjpool)
    }

    fn new(pool: *mut pmemobjpool) -> Result<Self, PalError> {
        NonNull::new(pool)
            .map(|valid| Pal {
                pool: Arc::new(valid),
            })
            .ok_or_else(|| {
                let err = unsafe { std::ffi::CString::from_raw(pmemobj_errormsg() as *mut i8) };
                let err_msg = format!(
                    "Failed to create memory pool. filepath: {}",
                    err.to_string_lossy()
                );
                PalError::OpenFailed(err_msg)
            })
    }

    /// Close the allocation arena. Only do this when all values you want to be
    /// lost are dropped and all values you want to be remembered are
    /// (ironically) std::mem::forget(ten).
    pub fn close(&mut self) {
        unsafe { pmemobj_close(self.pool.as_ptr()) };
    }

    /// Allocate an area of size in the persistent memory.
    pub fn allocate(&self, size: usize) -> Result<PalPtr, PalError> {
        let mut oid = std::mem::MaybeUninit::<PMEMoid>::uninit();
        if unsafe {
            haura_alloc(
                self.pool.as_ptr(),
                oid.as_mut_ptr(),
                size,
                0, // BOGUS
                std::ptr::null_mut(),
            ) != 0
        } {
            let err = unsafe { std::ffi::CString::from_raw(pmemobj_errormsg() as *mut i8) };
            let err_msg = format!(
                "Failed to create memory pool. filepath: {}",
                err.to_string_lossy()
            );
            return Err(PalError::AllocationFailed(err_msg));
        }

        if unsafe { oid_is_null(oid.assume_init_read()) } {
            return Err(PalError::NullEncountered);
        }
        Ok(PalPtr {
            inner: unsafe { oid.assume_init() },
            size,
        })
    }

    /// Access and allocate the root object if needed. The root object may be
    /// extended by calling this function again with a larger value. It may
    /// never be shrunken.
    ///
    /// If called with size 0 an existing root object might be opened, if none
    /// exists EINVAL is returned.
    pub fn root(&self, size: usize) -> Result<PalPtr, PalError> {
        let oid = unsafe { pmemobj_root(self.pool.as_ptr(), size) };
        if oid_is_null(oid) {
            return Err(PalError::AllocationFailed(format!("{}", errno())));
        }
        Ok(PalPtr { inner: oid, size })
    }

    /// Return the maximum size of the current root object.
    pub fn root_size(&self) -> usize {
        unsafe { pmemobj_root_size(self.pool.as_ptr()) }
    }
}

#[cfg(test)]
mod tests {

    extern crate alloc;

    use std::{collections::BTreeMap, path::PathBuf, process::Command};

    use alloc::collections::vec_deque::VecDeque;
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
    fn alloc_vec_deque() {
        let file = TestFile::new();
        const SIZE: usize = 64 * 1024 * 1024;
        let pal = Pal::create(file.path(), 128 * 1024 * 1024, 0o666).unwrap();
        let mut list: VecDeque<u8, Pal> = VecDeque::with_capacity_in(SIZE, pal);
        for _ in 0..SIZE {
            list.push_back(0);
        }
    }

    #[test]
    fn alloc_btree_map() {
        let file = TestFile::new();
        {
            let mut pal = Pal::create(file.path(), 128 * 1024 * 1024, 0o666).unwrap();
            let mut map: BTreeMap<u8, u8, Pal> = BTreeMap::new_in(pal.clone());
            let root_ptr = pal
                .root(std::mem::size_of::<BTreeMap<u8, u8, Pal>>())
                .unwrap();
            unsafe {
                (root_ptr.load() as *mut BTreeMap<u8, u8, Pal>)
                    .copy_from(&map, std::mem::size_of::<BTreeMap<u8, u8, Pal>>())
            };
            std::mem::forget(map);
            let map: &mut BTreeMap<u8, u8, Pal> = unsafe {
                (root_ptr.load() as *mut BTreeMap<u8, u8, Pal>)
                    .as_mut()
                    .unwrap()
            };
            for id in 0..100 {
                map.insert(id, id);
            }
            for id in 100..0 {
                assert_eq!(map.get(&id), Some(&id));
            }
            std::mem::forget(map);
            pal.close();
        }
        {
            let mut pal = Pal::open(file.path()).unwrap();
            let root_ptr = pal
                .root(std::mem::size_of::<BTreeMap<u8, u8, Pal>>())
                .unwrap();
            let map: &mut BTreeMap<u8, u8, Pal> = unsafe {
                (root_ptr.load() as *mut BTreeMap<u8, u8, Pal>)
                    .as_mut()
                    .unwrap()
            };
            for id in 100..0 {
                assert_eq!(map.get(&id), Some(&id));
            }
            std::mem::forget(map);
            pal.close();
        }
    }
}
