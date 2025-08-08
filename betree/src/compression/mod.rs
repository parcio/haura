//! This module provides the `Compression` trait for compressing and
//! decompressing data.
//! Supports multiple compression algorithms optimized for different storage kinds.

use crate::{
    buffer::Buf,
    cow_bytes::{CowBytes, SlicedCowBytes},
    size::{Size, StaticSize},
    vdev::Block,
};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, mem};

#[macro_use]
mod errors;
pub use errors::*;

// Database-specific compression modules
mod snappy;
mod dictionary;
mod rle;
mod delta;
mod gorilla;
mod toast;

pub use snappy::Snappy;
pub use dictionary::Dictionary;
pub use rle::Rle;
pub use delta::Delta;
pub use gorilla::Gorilla;
pub use toast::Toast;

const DEFAULT_BUFFER_SIZE: Block<u32> = Block(1);

/// Determine the used compression algorithm.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CompressionConfiguration {
    /// No-op.
    None,
    Lz4(Lz4),
    /// Configurable Zstd algorithm.
    Zstd(Zstd),
    /// Google's Snappy compression - very fast with decent ratios.
    Snappy(Snappy),
    /// Dictionary encoding - replace frequent values with indices.
    Dictionary(Dictionary),
    /// Run-Length Encoding - compress runs of identical values.
    Rle(Rle),
    /// Delta encoding - store differences between consecutive values.
    Delta(Delta),
    /// Gorilla compression - specialized for time series data.
    Gorilla(Gorilla),
    /// PostgreSQL TOAST with pglz compression.
    Toast(Toast),
}

impl CompressionConfiguration {
    /// Check if compression is enabled (avoids compression overhead when disabled)
    pub fn is_compression_enabled(&self) -> bool {
        !matches!(self, CompressionConfiguration::None)
    }

    /// Get the compression type ID for metadata storage
    pub fn compression_type_id(&self) -> u8 {
        match self {
            CompressionConfiguration::None => 0,
            CompressionConfiguration::Zstd(_) => 1,
            CompressionConfiguration::Lz4(_) => 2,
            CompressionConfiguration::Snappy(_) => 3,
            CompressionConfiguration::Dictionary(_) => 4,
            CompressionConfiguration::Rle(_) => 5,
            CompressionConfiguration::Delta(_) => 6,
            CompressionConfiguration::Gorilla(_) => 7,
            CompressionConfiguration::Toast(_) => 8,
        }
    }

    /// Get the decompression tag from compression type ID
    pub fn decompression_tag_from_id(compression_type_id: u8) -> DecompressionTag {
        match compression_type_id {
            0 => DecompressionTag::None,
            1 => DecompressionTag::Zstd,
            2 => DecompressionTag::Lz4,
            3 => DecompressionTag::Snappy,
            4 => DecompressionTag::Dictionary,
            5 => DecompressionTag::Rle,
            6 => DecompressionTag::Delta,
            7 => DecompressionTag::Gorilla,
            8 => DecompressionTag::Toast,
            _ => panic!("Unknown compression type ID: {}", compression_type_id),
        }
    }

    /// Create a compression state directly (high performance)
    pub fn create_compressor(&self) -> Result<Box<dyn CompressionState>> {
        match self {
            CompressionConfiguration::None => {
                None.create_compressor()
            }
            CompressionConfiguration::Lz4(lz4) => {
                lz4.create_compressor()
            }
            CompressionConfiguration::Zstd(zstd) => {
                zstd.create_compressor()
            }
            CompressionConfiguration::Snappy(snappy) => {
                snappy.create_compressor()
            }
            CompressionConfiguration::Dictionary(dict) => {
                dict.create_compressor()
            }
            CompressionConfiguration::Rle(rle) => {
                rle.create_compressor()
            }
            CompressionConfiguration::Delta(delta) => {
                delta.create_compressor()
            }
            CompressionConfiguration::Gorilla(gorilla) => {
                gorilla.create_compressor()
            }
            CompressionConfiguration::Toast(toast) => {
                toast.create_compressor()
            }
        }
    }

    /// Get decompression tag for storage
    pub fn decompression_tag(&self) -> DecompressionTag {
        match self {
            CompressionConfiguration::None => DecompressionTag::None,
            CompressionConfiguration::Lz4(_) => DecompressionTag::Lz4,
            CompressionConfiguration::Zstd(_) => DecompressionTag::Zstd,
            CompressionConfiguration::Snappy(_) => DecompressionTag::Snappy,
            CompressionConfiguration::Dictionary(_) => DecompressionTag::Dictionary,
            CompressionConfiguration::Rle(_) => DecompressionTag::Rle,
            CompressionConfiguration::Delta(_) => DecompressionTag::Delta,
            CompressionConfiguration::Gorilla(_) => DecompressionTag::Gorilla,
            CompressionConfiguration::Toast(_) => DecompressionTag::Toast,
        }
    }

    /// Legacy compatibility - create builder (deprecated)
    pub fn to_builder(&self) -> Box<dyn CompressionBuilder> {
        match self {
            CompressionConfiguration::None => Box::new(None),
            CompressionConfiguration::Lz4(lz4) => Box::new(*lz4),
            CompressionConfiguration::Zstd(zstd) => Box::new(*zstd),
            CompressionConfiguration::Snappy(snappy) => Box::new(*snappy),
            CompressionConfiguration::Dictionary(dict) => Box::new(*dict),
            CompressionConfiguration::Rle(rle) => Box::new(*rle),
            CompressionConfiguration::Delta(delta) => Box::new(*delta),
            CompressionConfiguration::Gorilla(gorilla) => Box::new(*gorilla),
            CompressionConfiguration::Toast(toast) => Box::new(*toast),
        }
    }
}

/// This tag is stored alongside compressed blobs, to select the appropriate decompression
/// method. This differs from a CompressionConfiguration, in that it is not configurable, as
/// all methods will decompress just fine without knowing at which compression level it was
/// originally written, so there's no advantage in storing the compression level with each object.
#[derive(
    Debug,
    Copy,
    Clone,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive(check_bytes)]
#[repr(u8)]
pub enum DecompressionTag {
    /// No-op.
    None,
    /// Decompress using Lz4.
    Lz4,
    /// Decompress using Zstd.
    Zstd,
    /// Decompress using Snappy.
    Snappy,
    /// Decompress using Dictionary encoding.
    Dictionary,
    /// Decompress using RLE.
    Rle,
    /// Decompress using Delta encoding.
    Delta,
    /// Decompress using Gorilla.
    Gorilla,
    /// Decompress using Toast/pglz.
    Toast,
}

impl DecompressionTag {
    /// Check if decompression is needed
    pub fn is_decompression_needed(&self) -> bool {
        !matches!(self, DecompressionTag::None)
    }

    /// Start a new decompression. The resulting structure consumes a buffer to decompress the data.
    pub fn new_decompression(&self) -> Result<Box<dyn DecompressionState>> {
        use DecompressionTag as Tag;
        match self {
            Tag::None => Ok(None::new_decompression()?),
            Tag::Lz4 => Ok(Lz4::new_decompression()?),
            Tag::Zstd => Ok(Zstd::new_decompression()?),
            Tag::Snappy => Ok(Snappy::new_decompression()?),
            Tag::Dictionary => Ok(Dictionary::new_decompression()?),
            Tag::Rle => Ok(Rle::new_decompression()?),
            Tag::Delta => Ok(Delta::new_decompression()?),
            Tag::Gorilla => Ok(Gorilla::new_decompression()?),
            Tag::Toast => Ok(Toast::new_decompression()?),
        }
    }
}

impl StaticSize for DecompressionTag {
    fn static_size() -> usize {
        mem::size_of::<DecompressionTag>()
    }
}

/// High-performance compression interface - no locks, no shared state
pub trait CompressionBuilder: Debug + Size + Send + Sync + 'static {
    /// Create a lightweight compression state without shared locking (high performance)
    fn create_compressor(&self) -> Result<Box<dyn CompressionState>>;
    /// Which decompression algorithm needs to be used.
    fn decompression_tag(&self) -> DecompressionTag;
    
    /// Legacy compatibility - returns an object for compressing data (deprecated)
    fn new_compression(&self) -> Result<Box<dyn CompressionState>> {
        self.create_compressor()
    }
}

/// Trait for the object that compresses data.
pub trait CompressionState {
    /// Compress data from slice and return the compressed data as a Vec<u8>
    /// Used for individual values during Memory storage kind packing
    fn compress_val(&mut self, data: &[u8]) -> Result<Vec<u8>>;
    
    /// Compress data from Buf and return the compressed data as a Buf
    /// Used for block-level compression during SSD storage kind packing
    fn compress_buf(&mut self, data: Buf) -> Result<Buf>;
    
    /// Legacy compatibility - finishes the compression stream (deprecated)
    fn finish(&mut self, data: Buf) -> Result<Buf> {
        self.compress_buf(data)
    }
}

/// An implementation of consumption-based decompression.
pub trait DecompressionState {
    /// Decompress data from slice and return the decompressed data as SlicedCowBytes
    /// Used for individual values during Memory storage kind unpacking
    fn decompress_val(&mut self, data: &[u8]) -> Result<SlicedCowBytes>;
    
    /// Decompress data from Buf and return the decompressed data as a Buf
    /// Used for block-level decompression during SSD storage kind unpacking
    fn decompress_buf(&mut self, data: Buf) -> Result<Buf>;
    
    /// Legacy compatibility - decompress the given [Buf] (deprecated)
    fn decompress(&mut self, data: Buf) -> Result<Buf> {
        self.decompress_buf(data)
    }
}

mod none;
pub use self::none::None;

mod lz4;
pub use self::lz4::Lz4;

mod zstd;
pub use self::zstd::Zstd;

pub mod metrics;
