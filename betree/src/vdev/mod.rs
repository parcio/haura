//! This module provides an abstraction over various 'virtual devices' (short
//! *vdev*)
//! that are built on top of storage devices.

use crate::{buffer::Buf, checksum::Checksum};
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use std::sync::atomic::{AtomicU64, Ordering};

/// Internal block size (4KiB)
pub const BLOCK_SIZE: usize = 4096;

/// Provides statistics about (failed) requests performed by vdevs.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct Statistics {
    /// The total number of blocks of issued read requests
    pub read: Block<u64>,
    /// The total number of blocks of issued write requests
    pub written: Block<u64>,
    /// The total number of blocks of failed read requests due to read failures
    pub failed_reads: Block<u64>,
    /// The total number of blocks of failed read requests due to checksum
    /// errors
    pub checksum_errors: Block<u64>,
    /// The total number of blocks of failed write requests
    pub failed_writes: Block<u64>,
    #[cfg(feature = "memory_metrics")]
    /// The total number of bytes accessed directly from memory (Memory storage only)
    pub memory_read: Block<u64>,
    #[cfg(feature = "memory_metrics")]
    /// The total number of direct memory access operations (Memory storage only)
    pub memory_read_count: u64,
    #[cfg(feature = "compression_metrics")]
    /// The total number of bytes passed to compression algorithms
    pub bytes_to_compressed: Block<u64>,
    #[cfg(feature = "compression_metrics")]
    /// The total number of bytes after compression
    pub compressed_bytes: Block<u64>,
    #[cfg(feature = "compression_metrics")]
    /// The total time spent in compression operations (nanoseconds)
    pub compression_time: u64,
    #[cfg(feature = "compression_metrics")]
    /// The total number of bytes passed to decompression algorithms
    pub bytes_to_decompress: Block<u64>,
    #[cfg(feature = "compression_metrics")]
    /// The total number of bytes after decompression
    pub bytes_after_decompression: Block<u64>,
    #[cfg(feature = "compression_metrics")]
    /// The total time spent in decompression operations (nanoseconds)
    pub decompression_time: u64,
    #[cfg(feature = "latency_metrics")]
    /// The average latency over all read operations
    pub read_latency: u64,
}

#[derive(Default, Debug)]
pub(crate) struct AtomicStatistics {
    read: AtomicU64,
    written: AtomicU64,
    failed_reads: AtomicU64,
    checksum_errors: AtomicU64,
    repaired: AtomicU64,
    failed_writes: AtomicU64,
    #[cfg(feature = "memory_metrics")]
    pub(crate) memory_read: AtomicU64,
    #[cfg(feature = "memory_metrics")]
    pub(crate) memory_read_count: AtomicU64,
    #[cfg(feature = "compression_metrics")]
    pub(crate) bytes_to_compressed: AtomicU64,
    #[cfg(feature = "compression_metrics")]
    pub(crate) compressed_bytes: AtomicU64,
    #[cfg(feature = "compression_metrics")]
    pub(crate) compression_time: AtomicU64,
    #[cfg(feature = "compression_metrics")]
    pub(crate) bytes_to_decompress: AtomicU64,
    #[cfg(feature = "compression_metrics")]
    pub(crate) bytes_after_decompression: AtomicU64,
    #[cfg(feature = "compression_metrics")]
    pub(crate) decompression_time: AtomicU64,
    #[cfg(feature = "latency_metrics")]
    prev_read: AtomicU64,
    #[cfg(feature = "latency_metrics")]
    read_op_latency: AtomicU64,
}

impl AtomicStatistics {
    #[cfg(feature = "compression_metrics")]
    pub(crate) fn update_compression_metrics(&self, bytes_to_compressed: u64, compressed_bytes: u64, compression_time: u64) {
        self.bytes_to_compressed.fetch_add(bytes_to_compressed, Ordering::Relaxed);
        self.compressed_bytes.fetch_add(compressed_bytes, Ordering::Relaxed);
        self.compression_time.fetch_add(compression_time, Ordering::Relaxed);
    }

    #[cfg(feature = "compression_metrics")]
    pub(crate) fn update_decompression_metrics(&self, bytes_to_decompress: u64, bytes_after_decompression: u64, decompression_time: u64) {
        self.bytes_to_decompress.fetch_add(bytes_to_decompress, Ordering::Relaxed);
        self.bytes_after_decompression.fetch_add(bytes_after_decompression, Ordering::Relaxed);
        self.decompression_time.fetch_add(decompression_time, Ordering::Relaxed);
    }

    fn as_stats(&self) -> Statistics {
        #[cfg(feature = "latency_metrics")]
        {
            self.prev_read
                .store(self.read.load(Ordering::Relaxed), Ordering::Relaxed)
        }
        
        #[cfg(feature = "memory_metrics")]
        let memory_read_val = self.memory_read.load(Ordering::Relaxed);
        #[cfg(feature = "memory_metrics")]
        let memory_read_count_val = self.memory_read_count.load(Ordering::Relaxed);
        
        // Get compression metrics from global instance and local vdev counters
        #[cfg(feature = "compression_metrics")]
        let (global_bytes_to_compressed, global_compressed_bytes, global_compression_time, 
             global_bytes_to_decompress, global_bytes_after_decompression, global_decompression_time) = 
            crate::compression::metrics::get_compression_metrics();
        
        #[cfg(feature = "compression_metrics")]
        let bytes_to_compressed_val = self.bytes_to_compressed.load(Ordering::Relaxed) + global_bytes_to_compressed;
        #[cfg(feature = "compression_metrics")]
        let compressed_bytes_val = self.compressed_bytes.load(Ordering::Relaxed) + global_compressed_bytes;
        #[cfg(feature = "compression_metrics")]
        let compression_time_val = self.compression_time.load(Ordering::Relaxed) + global_compression_time;
        #[cfg(feature = "compression_metrics")]
        let bytes_to_decompress_val = self.bytes_to_decompress.load(Ordering::Relaxed) + global_bytes_to_decompress;
        #[cfg(feature = "compression_metrics")]
        let bytes_after_decompression_val = self.bytes_after_decompression.load(Ordering::Relaxed) + global_bytes_after_decompression;
        #[cfg(feature = "compression_metrics")]
        let decompression_time_val = self.decompression_time.load(Ordering::Relaxed) + global_decompression_time;
        
        Statistics {
            read: Block(self.read.load(Ordering::Relaxed)),
            written: Block(self.written.load(Ordering::Relaxed)),
            failed_reads: Block(self.failed_reads.load(Ordering::Relaxed)),
            checksum_errors: Block(self.checksum_errors.load(Ordering::Relaxed)),
            failed_writes: Block(self.failed_writes.load(Ordering::Relaxed)),
            #[cfg(feature = "memory_metrics")]
            memory_read: Block(memory_read_val),
            #[cfg(feature = "memory_metrics")]
            memory_read_count: memory_read_count_val,
            #[cfg(feature = "compression_metrics")]
            bytes_to_compressed: Block(bytes_to_compressed_val),
            #[cfg(feature = "compression_metrics")]
            compressed_bytes: Block(compressed_bytes_val),
            #[cfg(feature = "compression_metrics")]
            compression_time: compression_time_val,
            #[cfg(feature = "compression_metrics")]
            bytes_to_decompress: Block(bytes_to_decompress_val),
            #[cfg(feature = "compression_metrics")]
            bytes_after_decompression: Block(bytes_after_decompression_val),
            #[cfg(feature = "compression_metrics")]
            decompression_time: decompression_time_val,
            #[cfg(feature = "latency_metrics")]
            read_latency: self
                .read_op_latency
                .load(Ordering::Relaxed)
                .checked_div(
                    self.read
                        .load(Ordering::Relaxed)
                        .saturating_sub(self.prev_read.load(Ordering::Relaxed)),
                )
                .unwrap_or(0),
        }
    }
}

impl Clone for AtomicStatistics {
    fn clone(&self) -> Self {
        use std::sync::atomic::Ordering;
        Self {
            read: AtomicU64::new(self.read.load(Ordering::Relaxed)),
            written: AtomicU64::new(self.written.load(Ordering::Relaxed)),
            failed_reads: AtomicU64::new(self.failed_reads.load(Ordering::Relaxed)),
            checksum_errors: AtomicU64::new(self.checksum_errors.load(Ordering::Relaxed)),
            repaired: AtomicU64::new(self.repaired.load(Ordering::Relaxed)),
            failed_writes: AtomicU64::new(self.failed_writes.load(Ordering::Relaxed)),
            #[cfg(feature = "memory_metrics")]
            memory_read: AtomicU64::new(self.memory_read.load(Ordering::Relaxed)),
            #[cfg(feature = "memory_metrics")]
            memory_read_count: AtomicU64::new(self.memory_read_count.load(Ordering::Relaxed)),
            #[cfg(feature = "compression_metrics")]
            bytes_to_compressed: AtomicU64::new(self.bytes_to_compressed.load(Ordering::Relaxed)),
            #[cfg(feature = "compression_metrics")]
            compressed_bytes: AtomicU64::new(self.compressed_bytes.load(Ordering::Relaxed)),
            #[cfg(feature = "compression_metrics")]
            compression_time: AtomicU64::new(self.compression_time.load(Ordering::Relaxed)),
            #[cfg(feature = "compression_metrics")]
            bytes_to_decompress: AtomicU64::new(self.bytes_to_decompress.load(Ordering::Relaxed)),
            #[cfg(feature = "compression_metrics")]
            bytes_after_decompression: AtomicU64::new(self.bytes_after_decompression.load(Ordering::Relaxed)),
            #[cfg(feature = "compression_metrics")]
            decompression_time: AtomicU64::new(self.decompression_time.load(Ordering::Relaxed)),
            #[cfg(feature = "latency_metrics")]
            prev_read: AtomicU64::new(self.prev_read.load(Ordering::Relaxed)),
            #[cfg(feature = "latency_metrics")]
            read_op_latency: AtomicU64::new(self.read_op_latency.load(Ordering::Relaxed)),
        }
    }
}

/// Result of a successful scrub request
#[derive(Debug)]
pub struct ScrubResult {
    /// The actual data scrubbed
    pub data: Buf,
    /// The total number of faulted blocks detected
    pub faulted: Block<u32>,
    /// The total number of successfully rewritten blocks
    ///
    /// Note: The actual data on disk may still be faulted,
    /// but the underlying disk signaled a successful write.
    pub repaired: Block<u32>,
}

impl From<ScrubResult> for Buf {
    fn from(x: ScrubResult) -> Self {
        x.data
    }
}

/// Trait for reading blocks of data.
#[async_trait]
#[enum_dispatch]
pub trait VdevRead: Send + Sync {
    /// Reads `size` data blocks at `offset` and verifies the data with the
    /// `checksum`.
    /// May issue write operations to repair faulted data blocks of components.
    async fn read<C: Checksum>(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Result<Buf>;

    /// Reads `size` blocks at `offset` and verifies the data with the
    /// `checksum`.
    /// In contrast to `read`, this function will read and verify data from
    /// every child vdev.
    /// May issue write operations to repair faulted data blocks of child vdevs.
    async fn scrub<C: Checksum>(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Result<ScrubResult>;

    /// Reads `size` blocks at `offset` of every child vdev. Does not verify
    /// the data.
    async fn read_raw(&self, size: Block<u32>, offset: Block<u64>) -> Result<Vec<Buf>>;
}

/// Trait for writing blocks of data.
#[async_trait]
#[enum_dispatch]
pub trait VdevWrite {
    /// Writes the `data` at `offset`. Returns success if the data has been
    /// written to
    /// enough replicas so that the data can be retrieved later on.
    ///
    /// Note: `data.len()` must be a multiple of `BLOCK_SIZE`.
    async fn write(&self, data: Buf, offset: Block<u64>) -> Result<()>;

    /// Flushes pending data (in caches) to disk.
    fn flush(&self) -> Result<()>;

    /// Writes the `data` at `offset` on all child vdevs like mirroring.
    /// Returns success
    /// if the data has been written to enough replicas so that the data can be
    /// retrieved later on.
    ///
    /// Note: `data.len()` must be a multiple of `BLOCK_SIZE`.
    async fn write_raw(&self, data: Buf, offset: Block<u64>) -> Result<()>;
}

#[enum_dispatch]
/// Trait for general information about a vdev.
pub trait Vdev: Send + Sync {
    /// Returns the actual size of a data block which may be larger due to
    /// parity data.
    fn actual_size(&self, size: Block<u32>) -> Block<u32>;

    /// Returns the number of underlying block devices.
    fn num_disks(&self) -> usize;

    /// Returns the total size of this vdev.
    fn size(&self) -> Block<u64>;

    /// Returns the effective free size which may be smaller due to parity data.
    fn effective_free_size(&self, free_size: Block<u64>) -> Block<u64>;

    /// Returns the (unique) ID of this vdev.
    fn id(&self) -> &str;

    /// Returns statistics about this vedv
    fn stats(&self) -> Statistics;

    /// Executes `f` for each child vdev.
    fn for_each_child(&self, f: &mut dyn FnMut(&dyn Vdev));
}

/// Trait for reading from a leaf vdev.
#[async_trait]
#[enum_dispatch]
pub trait VdevLeafRead: Send + Sync {
    /// Reads `buffer.as_mut().len()` bytes at `offset`. Does not verify the
    /// data.
    async fn read_raw<R: AsMut<[u8]> + Send>(&self, buffer: R, offset: Block<u64>) -> Result<R>;

    /// Shall be called if this vdev returned faulty data for a read request
    /// so that the statistics for this vdev show this incident.
    fn checksum_error_occurred(&self, size: Block<u32>);
}

/// Trait for writing to a leaf vdev.
#[async_trait]
#[enum_dispatch]
pub trait VdevLeafWrite: Send + Sync {
    /// Writes the `data` at `offset`.
    ///
    /// Note: `data.as_mut().len()` must be a multiple of `BLOCK_SIZE`.
    /// `is_repair` shall be set to `true` if this write request is a rewrite
    /// of data
    /// because of a failed or faulty read so that the statistics for this vdev
    /// can be updated.
    async fn write_raw<W: AsRef<[u8]> + Send + 'static>(
        &self,
        data: W,
        offset: Block<u64>,
        is_repair: bool,
    ) -> Result<()>;

    /// Flushes pending data (in caches) to disk.
    fn flush(&self) -> Result<()>;
}

#[async_trait]
impl<T: VdevLeafWrite> VdevWrite for T {
    async fn write(&self, data: Buf, offset: Block<u64>) -> Result<()> {
        VdevLeafWrite::write_raw(self, data, offset, false).await
    }

    fn flush(&self) -> Result<()> {
        VdevLeafWrite::flush(self)
    }

    async fn write_raw(&self, data: Buf, offset: Block<u64>) -> Result<()> {
        VdevLeafWrite::write_raw(self, data, offset, false).await
    }
}

#[cfg(test)]
#[macro_use]
pub mod test;

mod block;
pub use self::block::Block;

mod errors;
pub(crate) type Result<T> = std::result::Result<T, errors::VdevError>;
pub use errors::VdevError as Error;

mod file;
pub use self::file::File;

mod parity1;
pub use self::parity1::Parity1;

mod mirror;
pub use self::mirror::Mirror;

mod mem;
pub use self::mem::Memory;

#[cfg(feature = "nvm")]
mod pmemfile;
#[cfg(feature = "nvm")]
pub use self::pmemfile::PMemFile;

#[enum_dispatch(Vdev, VdevRead, VdevLeafWrite, VdevLeafRead)]
pub(crate) enum Leaf {
    File,
    Memory,
    #[cfg(feature = "nvm")]
    PMemFile,
}

#[enum_dispatch(Vdev, VdevWrite, VdevRead)]
pub(crate) enum Dev {
    Leaf(Leaf),
    Mirror(Mirror<Leaf>),
    Parity1(Parity1<Leaf>),
}
