#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::sync::{Arc};
use std::os::raw::c_void;

#[derive(Debug)]
pub struct PMem {
    pub ptr: Arc<*mut c_void>
}

unsafe impl Send for PMem {}
unsafe impl Sync for PMem {}

pub mod libpmem {
    pub use super::*;
    
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
                ptr : Arc::new(ptr)
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
             ptr: Arc::new(ptr) 
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

        pub fn write(&self, offset: usize, data: &[u8], len: usize) -> Result<(), std::io::Error>{
            if self.ptr.is_null() {
                return Err(std::io::Error::new(std::io::ErrorKind::Other,
                            format!("File handle is missing for the PMEM file.")));
            }

            let ptr = unsafe{
                pmem_memcpy_persist( self.ptr.add(offset), data.as_ptr() as *mut c_void, len as u64)
            };
           
            if self.ptr.is_null() {
                return Err(std::io::Error::new(std::io::ErrorKind::Other,
                            format!("Failed to write data to PMEM file. Offset: {}, Size:  {}", offset, len)))
            };

            Ok(())
        }

        pub fn close(&self, mapped_len: &u64) {
            unsafe {
                pmem_unmap(*self.ptr, *mapped_len);
            }
        }
    }
}

#[cfg(test)]
mod libpmem_tests {
    use super::*;

    const BUFFER_SIZE: usize = 4096;
    const DEST_FILEPATH: &str = "/pmem0/pmempool0\0";
    const TEXT: &str = " Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec dictum, massa sit amet tempus blandit, mi purus suscipit arcu, a egestas erat orci et ipsum. Phasellus vel urna non urna cursus imperdiet. Aliquam turpis ex, maximus id tortor eget, tincidunt feugiat metus. Ut ultrices auctor massa, quis convallis lectus vulputate et. Maecenas at mi orci. Donec id leo vitae risus tempus imperdiet ut a elit. Mauris quis dolor urna. Mauris dictum enim vel turpis aliquam tincidunt. Pellentesque et eros ac quam lobortis hendrerit non ut nulla. Quisque maximus magna tristique risus lacinia, et facilisis erat molestie.

Morbi eget sapien accumsan, rhoncus metus in, interdum libero. Nam gravida mi et viverra porttitor. Sed malesuada odio semper sapien bibendum ornare. Curabitur scelerisque lacinia ex, a rhoncus magna viverra eu. Maecenas sed libero vel ex dictum congue at sed nulla. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aliquam erat volutpat. Proin condimentum augue eu nulla consequat efficitur. Vivamus sodales pretium erat, id iaculis risus pellentesque sit amet. Integer tempus porta diam ac facilisis. Duis ex eros, mattis nec ultrices vel, varius vel lectus. Proin varius sapien est, nec euismod ex varius nec. Quisque in sem sit amet metus scelerisque ornare at a nisi. Maecenas ac scelerisque metus. In ut velit placerat, fringilla eros non, semper risus. Cras sed ante maximus, vestibulum nunc nec, rutrum leo. \0";
    const TEXT2: &str = "hello world!";

    #[test]
    fn basic_read_write_test() {
        unsafe {
            let mut src_filehandle : i32;
            let mut buf =  vec![0; BUFFER_SIZE];

            let mut is_pmem : i32 = 0;
            let mut mapped_len : u64 = 0;

            let mut pmem = match PMem::create(&DEST_FILEPATH, 64*1024*1024*1024, &mut mapped_len, &mut is_pmem) {
                        Ok(value) => value,
                        Err(e) => match PMem::open(&DEST_FILEPATH, &mut mapped_len, &mut is_pmem) {
                            Ok(value) => value,
                            Err(e) => panic!("\n Failed to create or open pmem file handle.")
                        }
                    };


            //dest_filehandle = libpmem::pmem_file_open(&DEST_FILEPATH, &mut mapped_len, &mut is_pmem);
            //dest_filehandle = libpmem::pmem_file_create(&DEST_FILEPATH, 16*1024*1024, &mut mapped_len, &mut is_pmem);

            let mut text_array = [0u8; 4096];
            TEXT.bytes().zip(text_array.iter_mut()).for_each(|(b, ptr)| *ptr = b);
            pmem.write(0, &text_array, TEXT.chars().count());

            TEXT2.bytes().zip(text_array.iter_mut()).for_each(|(b, ptr)| *ptr = b);
            pmem.write(TEXT.chars().count(), &text_array, TEXT2.chars().count());

            let mut buffer = vec![0; TEXT.chars().count()];
            pmem.read(0, &mut buffer, TEXT.chars().count() as u64);
            println!("\n TEXT length: {}", TEXT.chars().count());

            let mut buffer2 = vec![0; TEXT2.chars().count()];
            pmem.read(TEXT.chars().count(), &mut buffer2, TEXT2.chars().count() as u64);
            println!("\n TEXT2 length: {}", TEXT2.chars().count());

            TEXT.bytes().zip(text_array.iter_mut()).for_each(|(b, ptr)| *ptr = b);
            pmem.write(1000, &text_array, TEXT.chars().count());

            let mut buffer3 = vec![0; TEXT.chars().count()];
            pmem.read(1000, &mut buffer3, TEXT.chars().count() as u64);
            println!("\n TEXT length: {}", TEXT.chars().count());


            pmem.close(&mapped_len);

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

