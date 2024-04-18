#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
// The u128 types of rust and c are due to some bugs in llvm incompatible, which
// is the case for some values in the used libraries. But we don't actively use
// them now, and they spam the build log, so we deactivate the warning for now.
#![allow(improper_ctypes)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
use std::{
    ffi::{c_void, CString},
    mem::forget,
    path::PathBuf,
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

#[allow(clippy::len_without_is_empty)]
impl PMem {
    /// Create a new persistent memory pool. By default a file is created which
    /// is readable and writable by all users.
    pub fn create<P: Into<PathBuf>>(filepath: P, len: usize) -> Result<Self, std::io::Error> {
        let mut mapped_len = 0;
        let mut is_pmem = 0;
        let ptr = unsafe {
            pmem_map_file(
                CString::new(filepath.into().to_string_lossy().into_owned())?.into_raw(),
                len,
                (PMEM_FILE_CREATE | PMEM_FILE_EXCL) as i32,
                0o666,
                &mut mapped_len,
                &mut is_pmem,
            )
        };
        Self::new(ptr, mapped_len, is_pmem)
    }

    /// Open an existing persistent memory pool.
    pub fn open<P: Into<PathBuf>>(filepath: P) -> Result<Self, std::io::Error> {
        let mut mapped_len = 0;
        let mut is_pmem = 0;
        let ptr = unsafe {
            pmem_map_file(
                CString::new(filepath.into().to_string_lossy().into_owned())?.into_raw(),
                0, // Opening an existing file requires no flag(s).
                0, // No length as no flag is provided.
                0o666,
                &mut mapped_len,
                &mut is_pmem,
            )
        };
        Self::new(ptr, mapped_len, is_pmem)
    }

    pub unsafe fn get_slice(
        &self,
        offset: usize,
        len: usize,
    ) -> Result<&'static [u8], std::io::Error> {
        Ok(std::slice::from_raw_parts(
            self.ptr.as_ptr().add(offset) as *const u8,
            len,
        ))
    }

    fn new(ptr: *mut c_void, len: usize, is_pmem: i32) -> Result<Self, std::io::Error> {
        NonNull::new(ptr)
            .map(|valid| PMem {
                ptr: valid,
                actually_pmem: is_pmem != 0,
                len,
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

    /// Read a range of bytes from the specified offset.
    pub fn read(&self, offset: usize, data: &mut [u8]) {
        unsafe {
            self.ptr
                .as_ptr()
                .add(offset)
                .copy_to(data.as_mut_ptr() as *mut c_void, data.len())
        }
    }

    /// Write a range of bytes to the specified offset.
    ///
    /// By default, we always perform a persisting write here, equivalent to a
    /// direct & sync in traditional interfaces.
    ///
    /// # Safety
    /// It is possible to issue multiple write requests to the same area at the
    /// same time. What happens then is undefined and might lead to
    /// inconsistencies.
    pub unsafe fn write(&self, offset: usize, data: &[u8]) {
        let _ = pmem_memcpy(
            self.ptr.as_ptr().add(offset),
            data.as_ptr() as *mut c_void,
            data.len(),
            PMEM_F_MEM_NONTEMPORAL,
        );
    }

    /// Returns whether or not the underlying storage is either fsdax or devdax.
    pub fn is_pmem(&self) -> bool {
        self.actually_pmem
    }

    /// The total length of the memory pool in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    fn close(&mut self) {
        unsafe {
            // TODO: Read out error correctly. Atleast let the output know that something went wrong.
            pmem_unmap(self.ptr.as_ptr(), self.len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{path::PathBuf, process::Command};
    use tempfile::Builder;

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
    fn basic_io_session() {
        let file = TestFile::new();
        let pmem = PMem::create(file.path(), 8 * 1024 * 1024).unwrap();
        let buf = vec![42u8; 4 * 1024 * 1024];
        unsafe {
            pmem.write(0, &buf);
        }
        let mut rbuf = vec![0u8; buf.len()];
        pmem.read(0, &mut rbuf);
        assert_eq!(rbuf, buf);
    }

    #[test]
    fn basic_io_persist() {
        let file = TestFile::new();
        let buf = vec![42u8; 4 * 1024 * 1024];
        let offseted = [43u8];
        {
            let pmem = PMem::create(file.path(), 8 * 1024 * 1024).unwrap();
            unsafe {
                pmem.write(0, &buf);
                pmem.write(buf.len(), &offseted);
            }
        }
        {
            let pmem = PMem::open(file.path()).unwrap();
            let mut rbuf = vec![42u8; buf.len()];
            pmem.read(0, &mut rbuf);
            let mut single = [0u8];
            pmem.read(buf.len(), &mut single);
            assert_eq!(rbuf, buf);
            assert_eq!(single, offseted);
        }
    }
}
