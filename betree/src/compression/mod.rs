//! This module provides the `Compression` trait for compressing and
//! decompressing data.
//! `None` and `Lz4` are provided as implementation.

use crate::{
    buffer::{Buf, BufWrite},
    size::{Size, StaticSize},
    vdev::Block,
};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, io::Write, mem};

mod errors;
pub use errors::*;

use std::sync::{Arc, Mutex};

const DEFAULT_BUFFER_SIZE: Block<u32> = Block(1);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CompressionConfiguration {
    None,
    // Lz4,
    Zstd(Zstd),
}

impl CompressionConfiguration {
    pub fn to_builder(&self) -> Arc<std::sync::RwLock<dyn CompressionBuilder>> {
        match self {
            CompressionConfiguration::None => Arc::new(std::sync::RwLock::new(None)),
            CompressionConfiguration::Zstd(zstd) => Arc::new(std::sync::RwLock::new(*zstd)),
        }
    }
}

/// This tag is stored alongside compressed blobs, to select the appropriate decompression
/// method. This differs from a CompressionConfiguration, in that it is not configurable, as
/// all methods will decompress just fine without knowing at which compression level it was
/// originally written, so there's no advantage in storing the compression level with each object.
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[archive(check_bytes)]
#[repr(u8)]
pub enum DecompressionTag {
    None,
    Lz4,
    Zstd,
}

impl DecompressionTag {
    pub fn new_decompression(&self) -> Result<Box<dyn DecompressionState>> {
        use DecompressionTag as Tag;
        match self {
            Tag::None => Ok(None::new_decompression()?),
            Tag::Lz4 => todo!(), //Ok(Lz4::new_decompression()?),
            Tag::Zstd => Ok(Zstd::new_decompression()?),
        }
    }
}

impl StaticSize for DecompressionTag {
    fn static_size() -> usize {
        mem::size_of::<DecompressionTag>()
    }
}

/// Trait for compressing and decompressing data. Only compression is configurable, decompression
/// must be able to decompress anything ever compressed in any configuration.
pub trait CompressionBuilder: Debug + Size + Send + Sync + 'static {
    /// Returns an object for compressing data into a `Box<[u8]>`.
    fn new_compression(&self) -> Result<Arc<std::sync::RwLock<dyn CompressionState>>>;
    fn decompression_tag(&self) -> DecompressionTag;
}

/// Trait for the object that compresses data.
pub trait CompressionState: Write {
    /// Finishes the compression stream and returns a buffer that contains the
    /// compressed data.
    fn finish(&mut self, data: Buf) -> Result<Buf>;
    fn finishext(&mut self, data: &[u8]) -> Result<Vec<u8>>;
}

pub trait DecompressionState {
    fn decompress(&mut self, data: Buf) -> Result<Buf>;
}

mod none;
pub use self::none::None;

//mod lz4;
//pub use self::lz4::Lz4;

mod zstd;
pub use self::zstd::Zstd;


lazy_static::lazy_static! {
    pub static ref COMPRESSION_VAR: Arc<std::sync::RwLock<dyn CompressionBuilder>> =
    Arc::new(std::sync::RwLock::new(None));
}