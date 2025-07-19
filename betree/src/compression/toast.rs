//! TOAST (The Oversized-Attribute Storage Technique) with pglz compression
//! Ideal for: Large text/binary objects, similar to PostgreSQL's approach

use super::{CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result};
use crate::{
    buffer::Buf,
    cow_bytes::SlicedCowBytes,
    size::StaticSize,
};
use serde::{Deserialize, Serialize};

/// TOAST compression configuration
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Toast {
    /// Minimum size threshold to attempt compression
    pub min_compress_size: u32,
    /// Maximum compression ratio to accept (if worse, store uncompressed)
    pub max_ratio_percent: u8,
}

impl Default for Toast {
    fn default() -> Self {
        Self {
            min_compress_size: 32,    // Don't compress very small objects
            max_ratio_percent: 95,    // Must achieve at least 5% compression
        }
    }
}

impl StaticSize for Toast {
    fn static_size() -> usize {
        std::mem::size_of::<Toast>()
    }
}

/// TOAST compression state
#[derive(Debug)]
pub struct ToastCompression {
    config: Toast,
}

/// TOAST decompression state
#[derive(Debug)]
pub struct ToastDecompression;

impl CompressionBuilder for Toast {
    fn create_compressor(&self) -> Result<Box<dyn CompressionState>> {
        Ok(Box::new(ToastCompression { config: *self }))
    }

    fn decompression_tag(&self) -> DecompressionTag {
        DecompressionTag::Toast
    }
}

impl Toast {
    /// Create a new TOAST decompression state
    pub fn new_decompression() -> Result<Box<dyn DecompressionState>> {
        Ok(Box::new(ToastDecompression))
    }
}

/// TOAST format:
/// [compressed: u8][original_size: u32][data...]
/// compressed=0: data is uncompressed
/// compressed=1: data is compressed with simplified LZ-style compression
impl CompressionState for ToastCompression {
    fn finish_ext(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        if data.len() < self.config.min_compress_size as usize {
            // Too small to compress
            let mut result = Vec::new();
            result.push(0u8); // Not compressed
            result.extend_from_slice(&(data.len() as u32).to_le_bytes());
            result.extend_from_slice(data);
            return Ok(result);
        }

        // Try simplified LZ-style compression (similar to pglz approach)
        let compressed = pglz_compress(data);
        
        let compression_ratio = (compressed.len() * 100) / data.len();
        
        if compression_ratio >= self.config.max_ratio_percent as usize {
            // Compression not worthwhile
            let mut result = Vec::new();
            result.push(0u8); // Not compressed
            result.extend_from_slice(&(data.len() as u32).to_le_bytes());
            result.extend_from_slice(data);
            Ok(result)
        } else {
            // Use compressed version
            let mut result = Vec::new();
            result.push(1u8); // Compressed
            result.extend_from_slice(&(data.len() as u32).to_le_bytes());
            result.extend_from_slice(&compressed);
            Ok(result)
        }
    }

    fn finish(&mut self, data: Buf) -> Result<Buf> {
        let compressed_data = self.finish_ext(data.as_ref())?;
        Ok(Buf::from_zero_padded(compressed_data))
    }
}

impl DecompressionState for ToastDecompression {
    fn decompress_ext(&mut self, data: &[u8], _len: usize) -> Result<SlicedCowBytes> {
        if data.len() < 5 {
            return Ok(SlicedCowBytes::from(data.to_vec()));
        }

        let mut pos = 0;
        let compressed = data[pos] != 0;
        pos += 1;

        let original_size = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        if compressed {
            let decompressed = pglz_decompress(&data[pos..], original_size)?;
            Ok(SlicedCowBytes::from(decompressed))
        } else {
            Ok(SlicedCowBytes::from(data[pos..].to_vec()))
        }
    }

    fn decompress(&mut self, data: Buf) -> Result<Buf> {
        let decompressed = self.decompress_ext(data.as_ref(), 0)?;
        Ok(Buf::from_zero_padded(decompressed.as_ref().to_vec()))
    }
}

/// Simplified LZ-style compression (inspired by PostgreSQL's pglz)
/// This is a basic implementation - real pglz is more sophisticated
fn pglz_compress(data: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    let mut pos = 0;
    
    while pos < data.len() {
        // Look for matches in previous data (simplified sliding window)
        let window_start = if pos >= 1024 { pos - 1024 } else { 0 };
        let mut best_match_len = 0;
        let mut best_match_dist = 0;
        
        // Find longest match
        for start in window_start..pos {
            let mut match_len = 0;
            while pos + match_len < data.len() 
                && start + match_len < pos 
                && data[start + match_len] == data[pos + match_len] 
                && match_len < 255 {
                match_len += 1;
            }
            
            if match_len > best_match_len && match_len >= 3 {
                best_match_len = match_len;
                best_match_dist = pos - start;
            }
        }
        
        if best_match_len >= 3 {
            // Encode match: [flag=1][distance:u16][length:u8]
            result.push(1u8);
            result.extend_from_slice(&(best_match_dist as u16).to_le_bytes());
            result.push(best_match_len as u8);
            pos += best_match_len;
        } else {
            // Encode literal: [flag=0][byte]
            result.push(0u8);
            result.push(data[pos]);
            pos += 1;
        }
    }
    
    result
}

/// Decompress pglz-style data
fn pglz_decompress(data: &[u8], expected_size: usize) -> Result<Vec<u8>> {
    let mut result = Vec::with_capacity(expected_size);
    let mut pos = 0;
    
    while pos < data.len() && result.len() < expected_size {
        if pos >= data.len() { break; }
        
        let flag = data[pos];
        pos += 1;
        
        if flag == 0 {
            // Literal byte
            if pos >= data.len() { break; }
            result.push(data[pos]);
            pos += 1;
        } else {
            // Match
            if pos + 2 >= data.len() { break; }
            let distance = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            let length = data[pos] as usize;
            pos += 1;
            
            if distance > result.len() { break; }
            
            let start = result.len() - distance;
            for i in 0..length {
                if result.len() >= expected_size { break; }
                let byte = result[start + i];
                result.push(byte);
            }
        }
    }
    
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_toast_compression() {
        // Create test data with repeated patterns (good for LZ-style compression)
        let pattern = b"This is a test pattern that repeats. ";
        let mut data = Vec::new();
        for _ in 0..50 {
            data.extend_from_slice(pattern);
        }

        let toast = Toast::default();
        let mut compressor = toast.create_compressor().unwrap();
        let compressed = compressor.finish_ext(&data).unwrap();
        
        let mut decompressor = Toast::new_decompression().unwrap();
        let decompressed = decompressor.decompress_ext(&compressed, data.len()).unwrap();
        
        assert_eq!(data, decompressed.as_ref());
        println!("Original size: {}, Compressed size: {}", data.len(), compressed.len());
    }

    #[test]
    fn test_pglz_round_trip() {
        let data = b"Hello world! This is a test of LZ compression. Hello world! Repeat test.";
        let compressed = pglz_compress(data);
        let decompressed = pglz_decompress(&compressed, data.len()).unwrap();
        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_small_data_no_compression() {
        let small_data = b"small";
        let toast = Toast::default();
        let mut compressor = toast.create_compressor().unwrap();
        let result = compressor.finish_ext(small_data).unwrap();
        
        // Should be stored uncompressed
        assert_eq!(result[0], 0u8); // Not compressed flag
    }
}