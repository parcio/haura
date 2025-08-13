//! Compression metrics tracking module
//! 
//! This module provides functionality to track compression and decompression
//! metrics when the `compression_metrics` feature is enabled.
//! 
//! ## Overview
//! 
//! The Haura betree storage system uses compression at two levels:
//! 
//! 1. **Value-level compression** (Memory storage): Individual values are compressed
//!    when stored in Memory storage kind, allowing for fine-grained compression control.
//! 
//! 2. **Block-level compression** (SSD/HDD storage): Entire nodes are compressed
//!    when stored in SSD or HDD storage kinds, optimizing for I/O efficiency.
//! 
//! ## Metrics Tracked
//! 
//! When the `compression_metrics` feature is enabled, the following metrics are tracked:
//! 
//! - `bytes_to_compressed`: Total bytes passed to compression algorithms
//! - `compressed_bytes`: Total bytes after compression
//! - `compression_time`: Total time spent in compression operations (nanoseconds)
//! - `bytes_to_decompress`: Total bytes passed to decompression algorithms  
//! - `bytes_after_decompression`: Total bytes after decompression
//! - `decompression_time`: Total time spent in decompression operations (nanoseconds)
//! 
//! ## Usage
//! 
//! Enable the feature in your `Cargo.toml`:
//! 
//! ```toml
//! [dependencies]
//! betree_storage_stack = { version = "0.3.1-alpha", features = ["compression_metrics"] }
//! ```
//! 
//! Access metrics through the dataset statistics:
//! 
//! ```rust,no_run
//! # use betree_storage_stack::*;
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = Database::build(DatabaseConfiguration::default())?;
//! let ds = db.open_or_create_dataset("test", DatasetConfiguration::default())?;
//! 
//! // Perform some operations...
//! ds.insert(b"key".to_vec(), b"value", 0)?;
//! let _value = ds.get(b"key")?;
//! 
//! // Get compression statistics
//! let stats = ds.statistics();
//! println!("Compression ratio: {:.2}%", 
//!          (stats.compressed_bytes.to_bytes() as f64 / 
//!           stats.bytes_to_compressed.to_bytes() as f64) * 100.0);
//! # Ok(())
//! # }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Global compression metrics storage
#[cfg(feature = "compression_metrics")]
pub struct CompressionMetrics {
    /// Total bytes passed to compression algorithms
    pub bytes_to_compressed: AtomicU64,
    /// Total bytes after compression
    pub compressed_bytes: AtomicU64,
    /// Total time spent in compression operations (nanoseconds)
    pub compression_time: AtomicU64,
    /// Total bytes passed to decompression algorithms
    pub bytes_to_decompress: AtomicU64,
    /// Total bytes after decompression
    pub bytes_after_decompression: AtomicU64,
    /// Total time spent in decompression operations (nanoseconds)
    pub decompression_time: AtomicU64,
}

#[cfg(feature = "compression_metrics")]
impl CompressionMetrics {
    /// Create a new compression metrics instance
    pub const fn new() -> Self {
        Self {
            bytes_to_compressed: AtomicU64::new(0),
            compressed_bytes: AtomicU64::new(0),
            compression_time: AtomicU64::new(0),
            bytes_to_decompress: AtomicU64::new(0),
            bytes_after_decompression: AtomicU64::new(0),
            decompression_time: AtomicU64::new(0),
        }
    }

    /// Record compression operation metrics
    pub fn record_compression(&self, input_bytes: usize, output_bytes: usize, duration_ns: u64) {
        self.bytes_to_compressed.fetch_add(input_bytes as u64, Ordering::Relaxed);
        self.compressed_bytes.fetch_add(output_bytes as u64, Ordering::Relaxed);
        self.compression_time.fetch_add(duration_ns, Ordering::Relaxed);
    }

    /// Record decompression operation metrics
    pub fn record_decompression(&self, input_bytes: usize, output_bytes: usize, duration_ns: u64) {
        self.bytes_to_decompress.fetch_add(input_bytes as u64, Ordering::Relaxed);
        self.bytes_after_decompression.fetch_add(output_bytes as u64, Ordering::Relaxed);
        self.decompression_time.fetch_add(duration_ns, Ordering::Relaxed);
    }

    /// Get current compression statistics as a tuple
    /// Returns: (bytes_to_compressed, compressed_bytes, compression_time, bytes_to_decompress, bytes_after_decompression, decompression_time)
    pub fn get_stats(&self) -> (u64, u64, u64, u64, u64, u64) {
        (
            self.bytes_to_compressed.load(Ordering::Relaxed),
            self.compressed_bytes.load(Ordering::Relaxed),
            self.compression_time.load(Ordering::Relaxed),
            self.bytes_to_decompress.load(Ordering::Relaxed),
            self.bytes_after_decompression.load(Ordering::Relaxed),
            self.decompression_time.load(Ordering::Relaxed),
        )
    }
}

#[cfg(feature = "compression_metrics")]
lazy_static::lazy_static! {
    /// Global compression metrics instance
    pub static ref COMPRESSION_METRICS: CompressionMetrics = CompressionMetrics::new();
}

/// Record compression metrics if the feature is enabled
#[cfg(feature = "compression_metrics")]
pub fn record_compression_metrics(input_bytes: usize, output_bytes: usize, duration_ns: u64) {
    COMPRESSION_METRICS.record_compression(input_bytes, output_bytes, duration_ns);
}

/// Record decompression metrics if the feature is enabled
#[cfg(feature = "compression_metrics")]
pub fn record_decompression_metrics(input_bytes: usize, output_bytes: usize, duration_ns: u64) {
    COMPRESSION_METRICS.record_decompression(input_bytes, output_bytes, duration_ns);
}

/// Get compression metrics for integration with vdev statistics
#[cfg(feature = "compression_metrics")]
pub fn get_compression_metrics() -> (u64, u64, u64, u64, u64, u64) {
    COMPRESSION_METRICS.get_stats()
}

/// No-op versions when the feature is disabled
#[cfg(not(feature = "compression_metrics"))]
pub fn record_compression_metrics(_input_bytes: usize, _output_bytes: usize, _duration_ns: u64) {
    // No-op when feature is disabled
}

/// No-op version when the feature is disabled
#[cfg(not(feature = "compression_metrics"))]
pub fn record_decompression_metrics(_input_bytes: usize, _output_bytes: usize, _duration_ns: u64) {
    // No-op when feature is disabled
}

/// Helper macro to time compression operations
#[macro_export]
macro_rules! time_compression {
    ($input_size:expr, $compression_op:expr) => {{
        #[cfg(feature = "compression_metrics")]
        let start = std::time::Instant::now();
        
        let result = $compression_op;
        
        #[cfg(feature = "compression_metrics")]
        {
            let duration = start.elapsed().as_nanos() as u64;
            if let Ok(ref output) = result {
                let output_size = output.len();
                $crate::compression::metrics::record_compression_metrics($input_size, output_size, duration);
            }
        }
        
        result
    }};
}

/// Helper macro to time decompression operations
#[macro_export]
macro_rules! time_decompression {
    ($input_size:expr, $decompression_op:expr) => {{
        #[cfg(feature = "compression_metrics")]
        let start = std::time::Instant::now();
        
        let result = $decompression_op;
        
        #[cfg(feature = "compression_metrics")]
        {
            let duration = start.elapsed().as_nanos() as u64;
            if let Ok(ref output) = result {
                let output_size = output.len();
                $crate::compression::metrics::record_decompression_metrics($input_size, output_size, duration);
            }
        }
        
        result
    }};
}