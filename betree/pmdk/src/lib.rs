#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::os::raw::c_void;

#[derive(Debug)]
pub struct PMem {
    pub ptr: *mut c_void
}
    
unsafe impl Send for PMem {}
unsafe impl Sync for PMem {}

impl PMem {
    pub fn create(filepath : &str, len: u64, mapped_len : &mut u64, is_pmem : &mut i32) -> Result<Self, std::io::Error> {
        let mut ptr = unsafe {
            pmem_map_file(filepath.as_ptr() as *const i8,
                            len,
                            (PMEM_FILE_CREATE|PMEM_FILE_EXCL) as i32,
                            0666,
                            mapped_len,
                            is_pmem)
        };

        if ptr.is_null() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other,
                        format!("Failed to create memory pool. filepath: {}", filepath)));
        }
            
        Ok(PMem {
            ptr : ptr
        })
    }

    pub fn open(filepath: &str, mapped_len: &mut u64, is_pmem: &mut i32) -> Result<Self, std::io::Error> {
        let mut ptr = unsafe {
        pmem_map_file(filepath.as_ptr() as *const i8, 
                            0, // Opening an existing file requires no flag(s).
                            0, // No length as no flag is provided.
                            0666, 
                            mapped_len, 
                            is_pmem)
        };

        if ptr.is_null() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other,
                        format!("Failed to open the memory pool. filepath: {}", filepath)));
        }

        Ok(PMem { 
            ptr: ptr
        })
    }

    pub fn read(&self, offset: usize, data: &mut [u8], len: u64) -> Result<(), std::io::Error>{
        if self.ptr.is_null() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other,
                        format!("File handle is missing for the PMEM file.")));
        }

        let ptr = unsafe {
            pmem_memcpy(data.as_ptr() as *mut c_void, self.ptr.add(offset), len, PMEM_F_MEM_NOFLUSH /*| PMEM_F_MEM_TEMPORAL*/)
        };
        
        if ptr.is_null() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other,
                        format!("Failed to read data from  PMEM file. Offset: {}, Size:  {}", offset, len)));
        };

        Ok(())
    }

    pub unsafe fn get_slice(&self, offset: usize, len: usize) -> Result<&'static [u8], std::io::Error>{
        if self.ptr.is_null() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other,
                        format!("File handle is missing for the PMEM file.")));
        }

        Ok(slice::from_raw_parts(voidp_to_ref::<u8>(self.ptr.add(offset)), len))
    }

    pub unsafe fn write(&self, offset: usize, data: &[u8], len: usize) -> Result<(), std::io::Error>{
        if self.ptr.is_null() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other,
                        format!("File handle is missing for the PMEM file.")));
        }

        let ptr = pmem_memcpy_persist( self.ptr.add(offset), data.as_ptr() as *mut c_void, len as u64);
           
        if self.ptr.is_null() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other,
                        format!("Failed to write data to PMEM file. Offset: {}, Size:  {}", offset, len)))
        };

        Ok(())
    }

    pub fn close(&self, mapped_len: &u64) {
        unsafe {
            pmem_unmap(self.ptr, *mapped_len);
        }
    }
}

