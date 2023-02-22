#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::sync::{Arc, Mutex};
use std::os::raw::c_void;
use std::ptr::NonNull;

#[derive(Debug)]
//pub struct ptr_to_pmem(Arc<Mutex<*mut c_void>>);
pub struct ptr_to_pmem(Arc<*mut c_void>);

unsafe impl Send for ptr_to_pmem {}
unsafe impl Sync for ptr_to_pmem {}

pub mod libpmem {
    use super::*;

    pub fn pmem_file_create(filepath : &str, len: u64, mapped_len : &mut u64, is_pmem : &mut i32) -> Option<ptr_to_pmem> {
        unsafe {
            let mut ptr = pmem_map_file(filepath.as_ptr() as *const i8,
                                     len,
                                     (PMEM_FILE_CREATE|PMEM_FILE_EXCL) as i32,
                                     0666,
                                     mapped_len,
                                     is_pmem);

            match NonNull::new(ptr) {
                //Some(value) => Some(ptr_to_pmem( Arc::new(Mutex::new(ptr)))),
                Some(value) => Some(ptr_to_pmem( Arc::new(ptr))),
                None => None
            }
        }
    }

    pub fn pmem_file_open(filepath: &str, mapped_len: &mut u64, is_pmem: &mut i32) -> Option<ptr_to_pmem> {
        unsafe {
            let mut ptr = pmem_map_file(filepath.as_ptr() as *const i8, 
                                     0, // Opening an existing file requires no flag(s).
                                     0, // No length as no flag is provided.
                                     0666, 
                                     mapped_len, 
                                     is_pmem);

            match NonNull::new(ptr) {
                //Some(value) => Some(ptr_to_pmem( Arc::new(Mutex::new(ptr)))),
                Some(value) => Some(ptr_to_pmem( Arc::new(ptr))),
                None => None
            }
        }
    }

    pub fn pmem_file_read(begin_pos: &ptr_to_pmem, offset: usize, data: &mut [u8], len: u64) -> Result<(), std::io::Error>{
        //match NonNull::new(*begin_pos.0.lock().unwrap()) {
        match NonNull::new(*begin_pos.0) {
            None => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other,
                                       format!("File handle is missing for the PMEM file.")
                                       ));
            },
            _ => ()
        };

        unsafe {
            //let ptr = pmem_memcpy(data.as_ptr() as *mut c_void, begin_pos.0.lock().unwrap().add(offset), len, PMEM_F_MEM_NOFLUSH /*| PMEM_F_MEM_TEMPORAL*/);
            let ptr = pmem_memcpy(data.as_ptr() as *mut c_void, begin_pos.0.add(offset), len, PMEM_F_MEM_NOFLUSH /*| PMEM_F_MEM_TEMPORAL*/);
            match NonNull::new(ptr) {
                Some(value) => Ok(()),
                None => Err(std::io::Error::new(std::io::ErrorKind::Other,
                                                    format!("Failed to read data from  PMEM file. Offset: {}, Size:  {}",
                                                            offset, len)))
            };
            Ok(())
        }
    }

    pub fn pmem_file_write(begin_pos: &ptr_to_pmem, offset: usize, data: &[u8], len: usize) -> Result<(), std::io::Error>{
        //match NonNull::new(*begin_pos.0.lock().unwrap()) {
        match NonNull::new(*begin_pos.0) {
            None => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other,
                                       format!("File handle is missing for the PMEM file.")
                                       ));
            },
            _ => ()
        };

        unsafe{
            //let ptr = pmem_memcpy_persist( begin_pos.0.lock().unwrap().add(offset), data.as_ptr() as *mut c_void, len as u64);
            let ptr = pmem_memcpy_persist( begin_pos.0.add(offset), data.as_ptr() as *mut c_void, len as u64);
            match NonNull::new(ptr) {
                Some(value) => Ok(()),
                None => Err(std::io::Error::new(std::io::ErrorKind::Other,
                                                    format!("Failed to write data to PMEM file. Offset: {}, Size:  {}",
                                                            offset, len)))
            };
            Ok(())
        }
    }

    pub fn pmem_file_close(pmem: &ptr_to_pmem, mapped_len: &u64) {
        unsafe {
            //pmem_unmap(*pmem.0.lock().unwrap(), *mapped_len);
            pmem_unmap(*pmem.0, *mapped_len);
        }
    }   
}

#[cfg(test)]
mod libpmem_tests {
    use super::*;

    const BUFFER_SIZE: usize = 4096;
    const DEST_FILEPATH: &str = "/pmem0/pmempool0\0";
    const TEXT: &str = "The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog.\0";
    const TEXT2: &str = "hello world!";

    #[test]
    fn basic_read_write_test() {
        unsafe {
            let mut src_filehandle : i32;
            let mut buf =  vec![0; BUFFER_SIZE];

            let mut dest_filehandle;

            let mut is_pmem : i32 = 0;
            let mut mapped_len : u64 = 0;

            dest_filehandle = match libpmem::pmem_file_create(&DEST_FILEPATH, 64*1024*1024*1024, &mut mapped_len, &mut is_pmem) {
                        Some(existing_handle) => existing_handle,
                        None => match libpmem::pmem_file_open(&DEST_FILEPATH, &mut mapped_len, &mut is_pmem) {
                            Some(new_handle) => new_handle,
                            None => panic!("\n Failed to create or open pmem file handle.")
                        }
                    };


            //dest_filehandle = libpmem::pmem_file_open(&DEST_FILEPATH, &mut mapped_len, &mut is_pmem);
            //dest_filehandle = libpmem::pmem_file_create(&DEST_FILEPATH, 16*1024*1024, &mut mapped_len, &mut is_pmem);

            let mut text_array = [0u8; 4096];
            TEXT.bytes().zip(text_array.iter_mut()).for_each(|(b, ptr)| *ptr = b);
            libpmem::pmem_file_write(&dest_filehandle, 0, &text_array, TEXT.chars().count());

            TEXT2.bytes().zip(text_array.iter_mut()).for_each(|(b, ptr)| *ptr = b);
            libpmem::pmem_file_write(&dest_filehandle, TEXT.chars().count(), &text_array, TEXT2.chars().count());

            let mut buffer = vec![0; TEXT.chars().count()];
            libpmem::pmem_file_read(&dest_filehandle, 0, &mut buffer, TEXT.chars().count() as u64);
            println!("\n TEXT length: {}", TEXT.chars().count());

            let mut buffer2 = vec![0; TEXT2.chars().count()];
            libpmem::pmem_file_read(&dest_filehandle, TEXT.chars().count(), &mut buffer2, TEXT2.chars().count() as u64);
            println!("\n TEXT2 length: {}", TEXT2.chars().count());

            TEXT.bytes().zip(text_array.iter_mut()).for_each(|(b, ptr)| *ptr = b);
            libpmem::pmem_file_write(&dest_filehandle, 1000, &text_array, TEXT.chars().count());

            let mut buffer3 = vec![0; TEXT.chars().count()];
            libpmem::pmem_file_read(&dest_filehandle, 1000, &mut buffer3, TEXT.chars().count() as u64);
            println!("\n TEXT length: {}", TEXT.chars().count());


            libpmem::pmem_file_close(&dest_filehandle, &mapped_len);

            let read_string = match std::str::from_utf8(&buffer) {
                Ok(string) => string,
                Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
            };

            assert_eq!(TEXT, read_string);
            println!("{}", read_string);

            let read_string2 = match std::str::from_utf8(&buffer2) {
                Ok(string) => string,
                Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
            };

            assert_eq!(TEXT2, read_string2);


            let read_string3 = match std::str::from_utf8(&buffer3) {
                Ok(string) => string,
                Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
            };

            assert_eq!(TEXT, read_string3);            
        }
    }
}
