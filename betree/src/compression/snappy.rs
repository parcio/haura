//! Snappy compression implementation - very fast compression/decompression
//! Ideal for: General-purpose compression where speed is more important than compression ratio

use super::{CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result};
use crate::{
    buffer::Buf,
    cow_bytes::SlicedCowBytes,
    size::StaticSize,
};
use serde::{Deserialize, Serialize};

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
    fn finish_ext(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        // For now, use a simple LZ77-style compression as a Snappy placeholder
        // In production, you would use the actual Snappy algorithm
        Ok(simple_lz_compress(data))
    }

    fn finish(&mut self, data: Buf) -> Result<Buf> {
        let compressed_data = simple_lz_compress(data.as_ref());
        Ok(Buf::from_zero_padded(compressed_data))
    }
}

impl DecompressionState for SnappyDecompression {
    fn decompress_ext(&mut self, data: &[u8], _len: usize) -> Result<SlicedCowBytes> {
        let decompressed = simple_lz_decompress(data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Snappy decompression failed: {:?}", e)))?;
        
        Ok(SlicedCowBytes::from(decompressed))
    }

    fn decompress(&mut self, data: Buf) -> Result<Buf> {
        let decompressed = simple_lz_decompress(data.as_ref())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Snappy decompression failed: {:?}", e)))?;
        
        Ok(Buf::from_zero_padded(decompressed))
    }
}

// Simple LZ77-style compression (placeholder for actual Snappy)
fn simple_lz_compress(data: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    let mut pos = 0;
    
    while pos < data.len() {
        let window_start = if pos >= 256 { pos - 256 } else { 0 };
        let mut best_match_len = 0;
        let mut best_match_dist = 0;
        
        // Find longest match in sliding window
        for start in window_start..pos {
            let mut match_len = 0;
            while pos + match_len < data.len() 
                && start + match_len < pos 
                && data[start + match_len] == data[pos + match_len] 
                && match_len < 63 {
                match_len += 1;
            }
            
            if match_len > best_match_len && match_len >= 3 {
                best_match_len = match_len;
                best_match_dist = pos - start;
            }
        }
        
        if best_match_len >= 3 {
            // Encode match: [length_dist_flag][length][distance]
            result.push(0x80 | (best_match_len as u8));
            result.push(best_match_dist as u8);
            pos += best_match_len;
        } else {
            // Encode literal
            result.push(data[pos]);
            pos += 1;
        }
    }
    
    result
}

fn simple_lz_decompress(data: &[u8]) -> std::result::Result<Vec<u8>, &'static str> {
    let mut result = Vec::new();
    let mut pos = 0;
    
    while pos < data.len() {
        let byte = data[pos];
        pos += 1;
        
        if byte & 0x80 != 0 {
            // Match
            let length = (byte & 0x7F) as usize;
            if pos >= data.len() { return Err("Truncated match distance"); }
            let distance = data[pos] as usize;
            pos += 1;
            
            if distance > result.len() { return Err("Invalid match distance"); }
            
            let start = result.len() - distance;
            for i in 0..length {
                let byte = result[start + i];
                result.push(byte);
            }
        } else {
            // Literal
            result.push(byte);
        }
    }
    
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snappy_round_trip() {
        let data = b"Hello, world! This is a test of Snappy compression.".repeat(10);
        let snappy = Snappy::default();
        
        let mut compressor = snappy.create_compressor().unwrap();
        let compressed = compressor.finish_ext(&data).unwrap();
        
        let mut decompressor = Snappy::new_decompression().unwrap();
        let decompressed = decompressor.decompress_ext(&compressed, data.len()).unwrap();
        
        assert_eq!(data, decompressed.as_ref());
        println!("Original size: {}, Compressed size: {}", data.len(), compressed.len());
    }

    #[test]
    fn test_simple_lz_round_trip() {
        let data = b"Test data with some repeated patterns. Test data again.";
        let compressed = simple_lz_compress(data);
        let decompressed = simple_lz_decompress(&compressed).unwrap();
        assert_eq!(data, decompressed.as_slice());
    }
}