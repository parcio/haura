use super::{
    errors::*, AtomicStatistics, Block, Result, ScrubResult, Statistics, Vdev, VdevLeafRead,
    VdevLeafWrite, VdevRead,
};
use crate::{buffer::Buf, checksum::Checksum};
use async_trait::async_trait;
use parking_lot::RwLock;
use std::{
    io,
    ops::{Deref, DerefMut},
    sync::atomic::Ordering,
};

/// `LeafVdev` that is backed by memory.
pub struct Memory {
    mem: RwLock<Box<[u8]>>,
    id: String,
    size: Block<u64>,
    stats: AtomicStatistics,
}

impl Memory {
    /// Creates a new `File`.
    pub fn new(size: usize, id: String) -> io::Result<Self> {
        Ok(Memory {
            mem: RwLock::new(vec![0; size].into_boxed_slice()),
            id,
            size: Block::from_bytes(size as u64),
            stats: Default::default(),
        })
    }

    fn slice(&self, size: usize, offset: usize) -> Result<impl Deref<Target = [u8]> + '_> {
        parking_lot::RwLockReadGuard::try_map(self.mem.read(), |mem| mem.get(offset..offset + size))
            .map_err(|_| VdevError::Read(self.id.clone()))
    }

    fn slice_blocks(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
    ) -> Result<impl Deref<Target = [u8]> + '_> {
        self.slice(size.to_bytes() as usize, offset.to_bytes() as usize)
    }

    fn slice_mut(&self, size: usize, offset: usize) -> Result<impl DerefMut<Target = [u8]> + '_> {
        parking_lot::RwLockWriteGuard::try_map(self.mem.write(), |mem| {
            mem.get_mut(offset..offset + size)
        })
        .map_err(|_| VdevError::Write(self.id.clone()))
    }

    fn ref_to_slice(&self, offset: Block<u64>, start: usize, end: usize) -> Result<&'static [u8]> {
        let inner_offset = offset.to_bytes() as usize + start;
        let size = end - start;

        let x = &self.mem.read()[inner_offset];

        Ok(unsafe { std::slice::from_raw_parts(x, size) })
    }

    fn slice_read(&self, size: Block<u32>, offset: Block<u64>) -> Result<Buf> {
        self.stats.read.fetch_add(size.as_u64(), Ordering::Relaxed);
        #[cfg(feature = "latency_metrics")]
        let start = std::time::Instant::now();

        match self.slice_blocks(size, offset) {
            Ok(slice) => {
                let buf = unsafe {
                    Buf::from_raw(
                        std::ptr::NonNull::new(slice.as_ptr() as *mut u8)
                            .expect("Pointer in Memory vdev was null."),
                        size,
                    )
                };
                #[cfg(feature = "latency_metrics")]
                self.stats.read_op_latency.fetch_add(
                    start
                        .elapsed()
                        .as_nanos()
                        .try_into()
                        .unwrap_or(u32::MAX as u64),
                    Ordering::Relaxed,
                );
                Ok(buf)
            }
            Err(e) => {
                #[cfg(feature = "latency_metrics")]
                self.stats.read_op_latency.fetch_add(
                    start
                        .elapsed()
                        .as_nanos()
                        .try_into()
                        .unwrap_or(u32::MAX as u64),
                    Ordering::Relaxed,
                );
                self.stats
                    .failed_reads
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
                Err(e)
            }
        }
    }
}

#[async_trait]
impl VdevRead for Memory {
    async fn get_slice(
        &self,
        offset: Block<u64>,
        start: usize,
        end: usize,
    ) -> Result<&'static [u8]> {
        // println!("1> {:?}, {}, {}", offset, start, end);

        self.ref_to_slice(offset, start, end)
    }

    async fn read<C: Checksum>(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Result<Buf> {
        let buf = self.slice_read(size, offset)?;
        match checksum
            .verify(&buf)
            .map_err(|_| VdevError::Read(self.id.clone()))
        {
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
        Ok(vec![self.slice_read(size, offset)?])
    }
}

impl Vdev for Memory {
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
impl VdevLeafRead for Memory {
    async fn read_raw<T: AsMut<[u8]> + Send>(&self, mut buf: T, offset: Block<u64>) -> Result<T> {
        let size = Block::from_bytes(buf.as_mut().len() as u32);
        self.stats.read.fetch_add(size.as_u64(), Ordering::Relaxed);
        let buf_mut = buf.as_mut();
        #[cfg(feature = "latency_metrics")]
        let start = std::time::Instant::now();
        match self.slice(buf_mut.len(), offset.to_bytes() as usize) {
            Ok(src) => {
                buf_mut.copy_from_slice(&src);
                #[cfg(feature = "latency_metrics")]
                self.stats.read_op_latency.fetch_add(
                    start
                        .elapsed()
                        .as_nanos()
                        .try_into()
                        .unwrap_or(u32::MAX as u64),
                    Ordering::Relaxed,
                );
                Ok(buf)
            }
            Err(e) => {
                #[cfg(feature = "latency_metrics")]
                self.stats.read_op_latency.fetch_add(
                    start
                        .elapsed()
                        .as_nanos()
                        .try_into()
                        .unwrap_or(u32::MAX as u64),
                    Ordering::Relaxed,
                );
                self.stats
                    .failed_reads
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
                Err(e)
            }
        }
    }

    fn checksum_error_occurred(&self, size: Block<u32>) {
        self.stats
            .checksum_errors
            .fetch_add(size.as_u64(), Ordering::Relaxed);
    }
}

#[async_trait]
impl VdevLeafWrite for Memory {
    async fn write_raw<W: AsRef<[u8]> + Send>(
        &self,
        data: W,
        offset: Block<u64>,
        is_repair: bool,
    ) -> Result<()> {
        let block_cnt = Block::from_bytes(data.as_ref().len() as u64).as_u64();
        self.stats.written.fetch_add(block_cnt, Ordering::Relaxed);
        match self
            .slice_mut(data.as_ref().len(), offset.to_bytes() as usize)
            .map(|mut dst| dst.copy_from_slice(data.as_ref()))
        {
            Ok(()) => {
                if is_repair {
                    self.stats.repaired.fetch_add(block_cnt, Ordering::Relaxed);
                }
                Ok(())
            }
            Err(e) => {
                self.stats
                    .failed_writes
                    .fetch_add(block_cnt, Ordering::Relaxed);
                Err(e)
            }
        }
    }
    fn flush(&self) -> Result<()> {
        Ok(())
    }
}
