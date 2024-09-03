use super::{
    errors::*, AtomicStatistics, Block, Result, ScrubResult, Statistics, Vdev, VdevLeafRead,
    VdevLeafWrite, VdevRead,
};
use crate::{buffer::Buf, checksum::Checksum};
use async_trait::async_trait;
use libc::{c_ulong, ioctl};
use pmdk;
use std::{fs, io, os::unix::io::AsRawFd, sync::atomic::Ordering};

/// `LeafVdev` which is backed by NVM and uses `pmdk`.
#[derive(Debug)]
pub struct PMemFile {
    file: pmdk::PMem,
    id: String,
    size: Block<u64>,
    stats: AtomicStatistics,
}

impl PMemFile {
    /// Creates a new `PMEMFile`.
    pub fn new(file: pmdk::PMem, id: String) -> io::Result<Self> {
        let size = Block::from_bytes(file.len() as u64);
        Ok(PMemFile {
            file,
            id,
            size,
            stats: Default::default(),
        })
    }
}

#[cfg(target_os = "linux")]
fn get_block_device_size(file: &fs::File) -> io::Result<Block<u64>> {
    const BLKGETSIZE64: c_ulong = 2148012658;
    let mut size: u64 = 0;
    let result = unsafe { ioctl(file.as_raw_fd(), BLKGETSIZE64, &mut size) };

    if result == 0 {
        Ok(Block::from_bytes(size))
    } else {
        Err(io::Error::last_os_error())
    }
}

#[async_trait]
impl VdevRead for PMemFile {
    async fn get_slice(
        &self,
        offset: Block<u64>,
        start: usize,
        end: usize,
    ) -> Result<&'static [u8]> {
        unsafe {
            match self
                .file
                .get_slice(offset.to_bytes() as usize + start, end - start)
            {
                Ok(val) => Ok(val),
                Err(e) => {
                    self.stats
                        .failed_reads
                        .fetch_add(end as u64, Ordering::Relaxed);
                    bail!(e)
                }
            }
        }
    }

    async fn read<C: Checksum>(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Result<Buf> {
        self.stats.read.fetch_add(size.as_u64(), Ordering::Relaxed);
        let buf = unsafe {
            let slice = self
                .file
                .get_slice(offset.to_bytes() as usize, size.to_bytes() as usize)?;
            // # SAFETY
            // Since Bufs are read only anyways we ensure the safety of this
            // step by re-packing this forced mutable pointer into one.
            Buf::from_raw(
                std::ptr::NonNull::new(slice.as_ptr() as *mut u8)
                    .expect("Pmem pointer was null when trying to read from offset."),
                size,
            )
        };

        // let buf = {
        //     let mut buf = Buf::zeroed(size).into_full_mut();
        //     self.file.read(offset.to_bytes() as usize, buf.as_mut());
        //     buf.into_full_buf()
        // };

        match checksum.verify(&buf).map_err(VdevError::from) {
            Ok(()) => Ok(buf),
            Err(e) => {
                self.stats
                    .checksum_errors
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
                Err(e)
            }
        }
    }

    async fn scrub<C: Checksum>(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Result<ScrubResult> {
        let data = self.read(size, offset, checksum).await?;
        Ok(ScrubResult {
            data,
            repaired: Block(0),
            faulted: Block(0),
        })
    }

    async fn read_raw(&self, size: Block<u32>, offset: Block<u64>) -> Result<Vec<Buf>> {
        self.stats.read.fetch_add(size.as_u64(), Ordering::Relaxed);
        // let mut buf = Buf::zeroed(size).into_full_mut();

        let buf = unsafe {
            let slice = self
                .file
                .get_slice(offset.to_bytes() as usize, size.to_bytes() as usize)?;
            // # SAFETY
            // Since Bufs are read only anyways we ensure the safety of this
            // step by re-packing this forced mutable pointer into one.
            Buf::from_raw(
                std::ptr::NonNull::new(slice.as_ptr() as *mut u8)
                    .expect("Pmem pointer was null when trying to read from offset."),
                size,
            )
        };

        // self.file.read(offset.to_bytes() as usize, buf.as_mut());
        Ok(vec![buf])
    }
}

impl Vdev for PMemFile {
    fn actual_size(&self, size: Block<u32>) -> Block<u32> {
        size
    }

    fn num_disks(&self) -> usize {
        1
    }

    fn size(&self) -> Block<u64> {
        self.size
    }

    fn effective_free_size(&self, free_size: Block<u64>) -> Block<u64> {
        free_size
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn stats(&self) -> Statistics {
        self.stats.as_stats()
    }

    fn for_each_child(&self, _f: &mut dyn FnMut(&dyn Vdev)) {}
}

#[async_trait]
impl VdevLeafRead for PMemFile {
    async fn read_raw<T: AsMut<[u8]> + Send>(&self, mut buf: T, offset: Block<u64>) -> Result<T> {
        let size = Block::from_bytes(buf.as_mut().len() as u32);
        self.stats.read.fetch_add(size.as_u64(), Ordering::Relaxed);

        self.file.read(offset.to_bytes() as usize, buf.as_mut());
        Ok(buf)
    }

    fn checksum_error_occurred(&self, size: Block<u32>) {
        self.stats
            .checksum_errors
            .fetch_add(size.as_u64(), Ordering::Relaxed);
    }
}

#[async_trait]
impl VdevLeafWrite for PMemFile {
    async fn write_raw<W: AsRef<[u8]> + Send>(
        &self,
        data: W,
        offset: Block<u64>,
        _is_repair: bool,
    ) -> Result<()> {
        let block_cnt = Block::from_bytes(data.as_ref().len() as u64).as_u64();
        self.stats.written.fetch_add(block_cnt, Ordering::Relaxed);

        unsafe { self.file.write(offset.to_bytes() as usize, data.as_ref()) };
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }
}
