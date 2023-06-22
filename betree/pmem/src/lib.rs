#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::{
    ffi::{c_void, CString},
    mem::forget,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    ptr::NonNull,
};

#[derive(Debug)]
pub struct PMem {
    ptr: NonNull<c_void>,
    actually_pmem: bool,
    len: usize,
}

impl Drop for PMem {
    fn drop(&mut self) {
        self.close()
    }
}

unsafe impl Send for PMem {}
unsafe impl Sync for PMem {}

impl PMem {
    pub fn create<P: Into<PathBuf>>(filepath: P, len: usize) -> Result<Self, std::io::Error> {
        let mut mapped_len = 0;
        let mut is_pmem = 0;
        let ptr = unsafe {
            pmem_map_file(
                CString::new(filepath.into().to_string_lossy().into_owned())?.into_raw(),
                len as usize,
                (PMEM_FILE_CREATE | PMEM_FILE_EXCL) as i32,
                0666,
                &mut mapped_len,
                &mut is_pmem,
            )
        };
        Self::new(ptr, mapped_len, is_pmem)
    }

    pub fn open<P: Into<PathBuf>>(filepath: P) -> Result<Self, std::io::Error> {
        let mut mapped_len = 0;
        let mut is_pmem = 0;
        let ptr = unsafe {
            pmem_map_file(
                CString::new(filepath.into().to_string_lossy().into_owned())?.into_raw(),
                0, // Opening an existing file requires no flag(s).
                0, // No length as no flag is provided.
                0666,
                &mut mapped_len,
                &mut is_pmem,
            )
        };
        Self::new(ptr, mapped_len, is_pmem)
    }

    fn new(ptr: *mut c_void, len: usize, is_pmem: i32) -> Result<Self, std::io::Error> {
        NonNull::new(ptr)
            .and_then(|valid| {
                Some(PMem {
                    ptr: valid,
                    actually_pmem: is_pmem != 0,
                    len,
                })
            })
            .ok_or_else(|| {
                let err = unsafe { CString::from_raw(pmem_errormsg() as *mut i8) };
                let err_msg = format!(
                    "Failed to create memory pool. filepath: {}",
                    err.to_string_lossy()
                );
                forget(err);
                std::io::Error::new(std::io::ErrorKind::Other, err_msg)
            })
    }

    pub fn read(&self, offset: usize, data: &mut [u8], len: usize) -> Result<(), std::io::Error> {
        let ptr = unsafe {
            pmem_memcpy(
                data.as_ptr() as *mut c_void,
                self.ptr.as_ptr().add(offset),
                len,
                PMEM_F_MEM_NOFLUSH, /*| PMEM_F_MEM_TEMPORAL*/
            )
        };

        if ptr.is_null() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Failed to read data from  PMEM file. Offset: {}, Size:  {}",
                    offset, len
                ),
            ));
        };

        Ok(())
    }

    pub unsafe fn write(
        &self,
        offset: usize,
        data: &[u8],
        len: usize,
    ) -> Result<(), std::io::Error> {
        let _ = pmem_memcpy_persist(
            self.ptr.as_ptr().add(offset),
            data.as_ptr() as *mut c_void,
            len,
        );
        Ok(())
    }

    pub fn is_pmem(&self) -> bool {
        self.actually_pmem
    }

    pub fn len(&self) -> usize {
        self.len
    }

    fn close(&mut self) {
        unsafe {
            pmem_unmap(self.ptr.as_ptr(), self.len);
        }
    }
}
