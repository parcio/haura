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
    #[cfg(feature = "latency_metrics")]
    /// The average latency over all read operations
    pub read_latency: u64,
}

#[derive(Default, Debug)]
struct AtomicStatistics {
    read: AtomicU64,
    written: AtomicU64,
    failed_reads: AtomicU64,
    checksum_errors: AtomicU64,
    repaired: AtomicU64,
    failed_writes: AtomicU64,
    #[cfg(feature = "latency_metrics")]
    prev_read: AtomicU64,
    #[cfg(feature = "latency_metrics")]
    read_op_latency: AtomicU64,
}

impl AtomicStatistics {
    fn as_stats(&self) -> Statistics {
        #[cfg(feature = "latency_metrics")]
        {
            self.prev_read
                .store(self.read.load(Ordering::Relaxed), Ordering::Relaxed)
        }
        Statistics {
            read: Block(self.read.load(Ordering::Relaxed)),
            written: Block(self.written.load(Ordering::Relaxed)),
            failed_reads: Block(self.failed_reads.load(Ordering::Relaxed)),
            checksum_errors: Block(self.checksum_errors.load(Ordering::Relaxed)),
            failed_writes: Block(self.failed_writes.load(Ordering::Relaxed)),
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

    async fn get_slice(
        &self,
        offset: Block<u64>,
        start: usize,
        end: usize,
    ) -> Result<&'static [u8]>;

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
