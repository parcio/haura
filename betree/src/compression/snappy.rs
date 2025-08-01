//! Snappy compression implementation - very fast compression/decompression
//! Ideal for: General-purpose compression where speed is more important than compression ratio

use super::{CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result};
use crate::{
    buffer::Buf,
    cow_bytes::SlicedCowBytes,
    size::StaticSize,
};
use serde::{Deserialize, Serialize};
use snap::raw::{Encoder, Decoder};

/// Snappy compression configuration
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Snappy {
    // No configuration needed for Snappy - it's designed to be fast with fixed algorithms
}

impl Default for Snappy {
    fn default() -> Self {
        Self {}
    }
}

impl StaticSize for Snappy {
    fn static_size() -> usize {
        0 // No configuration parameters
    }
}

/// Snappy compression state
#[derive(Debug)]
pub struct SnappyCompression;

/// Snappy decompression state
#[derive(Debug)]
pub struct SnappyDecompression;

impl CompressionBuilder for Snappy {
    fn create_compressor(&self) -> Result<Box<dyn CompressionState>> {
        Ok(Box::new(SnappyCompression))
    }

    fn decompression_tag(&self) -> DecompressionTag {
        DecompressionTag::Snappy
    }
}

impl Snappy {
    /// Create a new Snappy decompression state
    pub fn new_decompression() -> Result<Box<dyn DecompressionState>> {
        Ok(Box::new(SnappyDecompression))
    }
}

impl CompressionState for SnappyCompression {
    fn compress_val(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        let compressed_data = encoder.compress_vec(data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Snappy compression failed: {}", e)))?;

        let size = data.len() as u32;
        let comlen = compressed_data.len() as u32;

        let mut result = Vec::with_capacity(4 + 4 + compressed_data.len());
        result.extend_from_slice(&size.to_le_bytes());
        result.extend_from_slice(&comlen.to_le_bytes());
        result.extend_from_slice(&compressed_data);

        Ok(result)
    }

    fn compress_buf(&mut self, data: Buf) -> Result<Buf> {
        use crate::buffer::BufWrite;
        use crate::vdev::Block;
        use std::io::Write;
        
        let compressed_data = self.compress_val(data.as_ref())?;

        let size = data.as_ref().len() as u32;
        let comlen = compressed_data.len() as u32;

        let mut buf = BufWrite::with_capacity(Block::round_up_from_bytes(
            4 + 4 + comlen, // total metadata and compressed payload
        ));

        buf.write_all(&size.to_le_bytes())?;
        buf.write_all(&comlen.to_le_bytes())?;
        buf.write_all(&compressed_data)?;

        Ok(buf.into_buf())
    }
}

impl DecompressionState for SnappyDecompression {
    fn decompress_val(&mut self, data: &[u8]) -> Result<SlicedCowBytes> {
        if data.len() < 8 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Input too short").into());
        }

        let uncomp_size = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let comp_len = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;

        if data.len() < 8 + comp_len {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Compressed payload truncated").into());
        }

        let compressed = &data[8..8 + comp_len];

        let mut decoder = Decoder::new();
        let decompressed = decoder.decompress_vec(compressed)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Snappy decompression failed: {}", e)))?;
        
        Ok(SlicedCowBytes::from(decompressed))
    }

    fn decompress_buf(&mut self, data: Buf) -> Result<Buf> {
        use crate::buffer::BufWrite;
        use crate::vdev::Block;
        use std::io::Write;
        
        if data.len() < 8 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Input too short").into());
        }

        let uncomp_size = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let comp_len = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;

        if data.len() < 8 + comp_len {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Compressed payload truncated").into());
        }

        let compressed = &data[8..8 + comp_len];

        let decompressed = self.decompress_val(compressed)?;

        let mut buf = BufWrite::with_capacity(Block::round_up_from_bytes(uncomp_size as u32));
        buf.write_all(decompressed.as_ref())?;
        Ok(buf.into_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snappy_for_val_compression() {
        let data = b"Hello, world! This is a test of Snappy compression.".repeat(10);
        let snappy = Snappy::default();
        
        let mut compressor = snappy.create_compressor().unwrap();
        let compressed = compressor.compress_val(&data).unwrap();
        
        let mut decompressor = Snappy::new_decompression().unwrap();
        let decompressed = decompressor.decompress_val(&compressed).unwrap();
        
        assert_eq!(data, decompressed.as_ref());
        println!("Snappy val compression - Original: {}, Compressed: {}", data.len(), compressed.len());
    }

    #[test]
    fn test_snappy_for_buf_compression() {
        let data = b"Test data with some repeated patterns for Snappy compression.".repeat(20);
        let buf = Buf::from_zero_padded(data.clone());
        let snappy = Snappy::default();
        
        let mut compressor = snappy.create_compressor().unwrap();
        let compressed_buf = compressor.compress_buf(buf.clone()).unwrap();
        
        let mut decompressor = Snappy::new_decompression().unwrap();
        let decompressed_buf = decompressor.decompress_buf(compressed_buf).unwrap();
        
        assert_eq!(buf.as_ref(), decompressed_buf.as_ref());
        println!("Snappy buf compression - Original: {}, Compressed: {}", buf.len(), decompressed_buf.len());
    }
}