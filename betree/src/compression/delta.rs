//! Delta encoding implementation
//! Ideal for: Sequential/sorted integer data, timestamps, auto-incrementing IDs

use super::{CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result};
use crate::{
    buffer::Buf,
    cow_bytes::SlicedCowBytes,
    size::StaticSize,
};
use serde::{Deserialize, Serialize};

/// Delta encoding configuration
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Delta {
    /// Integer size in bytes (1, 2, 4, or 8)
    pub value_size: u8,
    /// Whether values are signed integers
    pub signed: bool,
}

impl Default for Delta {
    fn default() -> Self {
        Self {
            value_size: 8,  // Default to 64-bit integers
            signed: true,   // Default to signed
        }
    }
}

impl StaticSize for Delta {
    fn static_size() -> usize {
        std::mem::size_of::<Delta>()
    }
}

/// Delta compression state
#[derive(Debug)]
pub struct DeltaCompression {
    config: Delta,
}

/// Delta decompression state
#[derive(Debug)]
pub struct DeltaDecompression;

impl CompressionBuilder for Delta {
    fn create_compressor(&self) -> Result<Box<dyn CompressionState>> {
        Ok(Box::new(DeltaCompression { config: *self }))
    }

    fn decompression_tag(&self) -> DecompressionTag {
        DecompressionTag::Delta
    }
}

impl Delta {
    /// Create a new Delta decompression state
    pub fn new_decompression() -> Result<Box<dyn DecompressionState>> {
        Ok(Box::new(DeltaDecompression))
    }
}

/// Delta format:
/// [value_size: u8][signed: u8][base_value][delta_count: u32][deltas...]
/// Where deltas are variable-length encoded (smaller deltas use fewer bytes)
impl CompressionState for DeltaCompression {
    fn compress_val(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let value_size = self.config.value_size as usize;
        if data.len() % value_size != 0 || data.len() == 0 {
            return Ok(data.to_vec());
        }

        let count = data.len() / value_size;
        if count < 2 {
            return Ok(data.to_vec()); // Need at least 2 values for delta encoding
        }

        let mut result = Vec::new();
        result.push(value_size as u8);
        result.push(if self.config.signed { 1u8 } else { 0u8 });

        // Store first value as base
        result.extend_from_slice(&data[0..value_size]);
        
        // Calculate and store deltas
        result.extend_from_slice(&((count - 1) as u32).to_le_bytes());

        let mut prev_value = read_value(&data[0..value_size], value_size, self.config.signed);

        for i in 1..count {
            let current_value = read_value(&data[i * value_size..(i + 1) * value_size], value_size, self.config.signed);
            let delta = current_value.wrapping_sub(prev_value);
            
            // Variable-length encode the delta
            encode_varint(delta, &mut result);
            prev_value = current_value;
        }

        // Add size headers like other compression algorithms
        let size = data.len() as u32;
        let comlen = result.len() as u32;

        let mut final_result = Vec::with_capacity(4 + 4 + result.len());
        final_result.extend_from_slice(&size.to_le_bytes());
        final_result.extend_from_slice(&comlen.to_le_bytes());
        final_result.extend_from_slice(&result);

        Ok(final_result)
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

impl DecompressionState for DeltaDecompression {
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

        if compressed.len() < 6 {
            return Ok(SlicedCowBytes::from(compressed.to_vec()));
        }

        let mut pos = 0;
        let value_size = compressed[pos] as usize;
        pos += 1;
        let signed = compressed[pos] != 0;
        pos += 1;

        if pos + value_size > compressed.len() {
            return Ok(SlicedCowBytes::from(compressed.to_vec()));
        }

        // Read base value
        let mut current_value = read_value(&compressed[pos..pos + value_size], value_size, signed);
        pos += value_size;

        if pos + 4 > compressed.len() {
            return Ok(SlicedCowBytes::from(compressed.to_vec()));
        }

        let delta_count = u32::from_le_bytes([compressed[pos], compressed[pos + 1], compressed[pos + 2], compressed[pos + 3]]) as usize;
        pos += 4;

        let mut result = Vec::new();
        
        // Add base value
        result.extend_from_slice(&write_value(current_value, value_size));

        // Decode deltas
        for _ in 0..delta_count {
            if pos >= compressed.len() { break; }
            
            let (delta, bytes_read) = decode_varint(&compressed[pos..]);
            if bytes_read == 0 { break; }
            pos += bytes_read;

            current_value = current_value.wrapping_add(delta);
            result.extend_from_slice(&write_value(current_value, value_size));
        }

        Ok(SlicedCowBytes::from(result))
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

// Helper functions
fn read_value(data: &[u8], size: usize, signed: bool) -> i64 {
    match (size, signed) {
        (1, true) => data[0] as i8 as i64,
        (1, false) => data[0] as i64,
        (2, true) => i16::from_le_bytes([data[0], data[1]]) as i64,
        (2, false) => u16::from_le_bytes([data[0], data[1]]) as i64,
        (4, true) => i32::from_le_bytes([data[0], data[1], data[2], data[3]]) as i64,
        (4, false) => u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as i64,
        (8, _) => i64::from_le_bytes([
            data[0], data[1], data[2], data[3],
            data[4], data[5], data[6], data[7]
        ]),
        _ => 0,
    }
}

fn write_value(value: i64, size: usize) -> Vec<u8> {
    match size {
        1 => vec![value as u8],
        2 => (value as u16).to_le_bytes().to_vec(),
        4 => (value as u32).to_le_bytes().to_vec(),
        8 => value.to_le_bytes().to_vec(),
        _ => vec![],
    }
}

// Variable-length integer encoding (LEB128-style)
fn encode_varint(mut value: i64, output: &mut Vec<u8>) {
    // Use zigzag encoding for signed values
    let unsigned = if value >= 0 {
        (value as u64) << 1
    } else {
        (((-value - 1) as u64) << 1) | 1
    };

    let mut n = unsigned;
    loop {
        let byte = (n & 0x7F) as u8;
        n >>= 7;
        if n == 0 {
            output.push(byte);
            break;
        } else {
            output.push(byte | 0x80);
        }
    }
}

fn decode_varint(data: &[u8]) -> (i64, usize) {
    let mut result = 0u64;
    let mut shift = 0;
    let mut pos = 0;

    for &byte in data {
        pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        shift += 7;
        
        if byte & 0x80 == 0 {
            break;
        }
        
        if shift >= 64 {
            return (0, 0); // Overflow
        }
    }

    // Decode zigzag
    let signed_result = if result & 1 == 0 {
        (result >> 1) as i64
    } else {
        -((result >> 1) as i64) - 1
    };

    (signed_result, pos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_for_val_compression() {
        // Create test data with sequential values (good for delta encoding)
        let mut data = Vec::new();
        for i in 1000i64..1100i64 {
            data.extend_from_slice(&i.to_le_bytes());
        }

        let delta = Delta::default();
        let mut compressor = delta.create_compressor().unwrap();
        let compressed = compressor.compress_val(&data).unwrap();
        
        let mut decompressor = Delta::new_decompression().unwrap();
        let decompressed = decompressor.decompress_val(&compressed).unwrap();
        
        assert_eq!(data, decompressed.as_ref());
        println!("Delta val compression - Original: {}, Compressed: {}", data.len(), compressed.len());
    }

    #[test]
    fn test_delta_for_buf_compression() {
        // Create test data with sequential values
        let mut data = Vec::new();
        for i in 500i64..600i64 {
            data.extend_from_slice(&i.to_le_bytes());
        }

        let buf = Buf::from_zero_padded(data.clone());
        let delta = Delta::default();
        
        let mut compressor = delta.create_compressor().unwrap();
        let compressed_buf = compressor.compress_buf(buf.clone()).unwrap();
        
        let mut decompressor = Delta::new_decompression().unwrap();
        let decompressed_buf = decompressor.decompress_buf(compressed_buf).unwrap();
        
        assert_eq!(buf.as_ref(), decompressed_buf.as_ref());
        println!("Delta buf compression - Original: {}, Compressed: {}", buf.len(), decompressed_buf.len());
    }

    #[test]
    fn test_varint_encoding() {
        let test_values = vec![0, 1, -1, 127, -127, 128, -128, 16383, -16383];
        
        for &value in &test_values {
            let mut encoded = Vec::new();
            encode_varint(value, &mut encoded);
            let (decoded, bytes_read) = decode_varint(&encoded);
            assert_eq!(value, decoded);
            assert_eq!(encoded.len(), bytes_read);
        }
    }
}