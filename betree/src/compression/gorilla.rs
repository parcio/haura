//! Gorilla compression implementation - specialized for time series data
//! Ideal for: Time series of floating point values with small changes between consecutive values

use super::{CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result};
use crate::{
    buffer::Buf,
    cow_bytes::SlicedCowBytes,
    size::StaticSize,
};
use serde::{Deserialize, Serialize};

/// Gorilla compression configuration  
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Gorilla {
    /// Whether to use 64-bit (f64) or 32-bit (f32) floats
    pub use_f64: bool,
}

impl Default for Gorilla {
    fn default() -> Self {
        Self {
            use_f64: true, // Default to double precision
        }
    }
}

impl StaticSize for Gorilla {
    fn static_size() -> usize {
        std::mem::size_of::<Gorilla>()
    }
}

/// Gorilla compression state
#[derive(Debug)]
pub struct GorillaCompression {
    config: Gorilla,
}

/// Gorilla decompression state
#[derive(Debug)]
pub struct GorillaDecompression;

impl CompressionBuilder for Gorilla {
    fn create_compressor(&self) -> Result<Box<dyn CompressionState>> {
        Ok(Box::new(GorillaCompression { config: *self }))
    }

    fn decompression_tag(&self) -> DecompressionTag {
        DecompressionTag::Gorilla
    }
}

impl Gorilla {
    /// Create a new Gorilla decompression state
    pub fn new_decompression() -> Result<Box<dyn DecompressionState>> {
        Ok(Box::new(GorillaDecompression))
    }
}

/// Simplified Gorilla format:
/// [use_f64: u8][count: u32][first_value][compressed_data...]
/// 
/// For each subsequent value:
/// - If XOR with previous == 0: store single bit '0'
/// - If XOR has same leading/trailing zeros as previous XOR: store '10' + middle bits
/// - Otherwise: store '11' + leading_zeros_count + meaningful_bits + trailing_zeros_count
impl CompressionState for GorillaCompression {
    fn compress_val(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let value_size = if self.config.use_f64 { 8 } else { 4 };
        
        if data.len() % value_size != 0 || data.len() == 0 {
            return Ok(data.to_vec());
        }

        let count = data.len() / value_size;
        if count < 2 {
            return Ok(data.to_vec()); // Need at least 2 values
        }

        let mut result = Vec::new();
        result.push(if self.config.use_f64 { 1u8 } else { 0u8 });
        result.extend_from_slice(&(count as u32).to_le_bytes());

        // Store first value uncompressed
        result.extend_from_slice(&data[0..value_size]);

        let mut bit_writer = BitWriter::new();
        let mut prev_xor = 0u64;
        let mut prev_leading_zeros = 0u8;
        let mut prev_trailing_zeros = 0u8;

        let mut prev_value = read_float_bits(&data[0..value_size], self.config.use_f64);

        for i in 1..count {
            let current_value = read_float_bits(&data[i * value_size..(i + 1) * value_size], self.config.use_f64);
            let xor = prev_value ^ current_value;

            if xor == 0 {
                // Value unchanged
                bit_writer.write_bit(false);
            } else {
                bit_writer.write_bit(true);
                
                let leading_zeros = xor.leading_zeros() as u8;
                let trailing_zeros = xor.trailing_zeros() as u8;
                
                if leading_zeros >= prev_leading_zeros && trailing_zeros >= prev_trailing_zeros {
                    // Use previous block info
                    bit_writer.write_bit(false);
                    let meaningful_bits = 64 - prev_leading_zeros - prev_trailing_zeros;
                    let shifted_xor = xor >> prev_trailing_zeros;
                    bit_writer.write_bits(shifted_xor, meaningful_bits);
                } else {
                    // New block info
                    bit_writer.write_bit(true);
                    bit_writer.write_bits(leading_zeros as u64, 5); // 5 bits for leading zeros (0-31)
                    
                    let meaningful_bits = 64 - leading_zeros - trailing_zeros;
                    bit_writer.write_bits(meaningful_bits as u64, 6); // 6 bits for length (0-63)
                    
                    let shifted_xor = xor >> trailing_zeros;
                    bit_writer.write_bits(shifted_xor, meaningful_bits);
                    
                    prev_leading_zeros = leading_zeros;
                    prev_trailing_zeros = trailing_zeros;
                }
                
                prev_xor = xor;
            }

            prev_value = current_value;
        }

        result.extend_from_slice(&bit_writer.compress_buf());
        
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

impl DecompressionState for GorillaDecompression {
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
        let use_f64 = compressed[pos] != 0;
        pos += 1;
        
        let count = u32::from_le_bytes([compressed[pos], compressed[pos + 1], compressed[pos + 2], compressed[pos + 3]]) as usize;
        pos += 4;

        let value_size = if use_f64 { 8 } else { 4 };
        
        if pos + value_size > compressed.len() {
            return Ok(SlicedCowBytes::from(compressed.to_vec()));
        }

        let mut result = Vec::new();
        
        // Read first value
        let mut current_value = read_float_bits(&compressed[pos..pos + value_size], use_f64);
        result.extend_from_slice(&write_float_bits(current_value, use_f64));
        pos += value_size;

        let mut bit_reader = BitReader::new(&compressed[pos..]);
        let mut prev_leading_zeros = 0u8;
        let mut prev_trailing_zeros = 0u8;

        for _ in 1..count {
            if let Some(control_bit) = bit_reader.read_bit() {
                if !control_bit {
                    // Value unchanged
                    // current_value stays the same
                } else {
                    // Value changed
                    if let Some(use_prev_block) = bit_reader.read_bit() {
                        if !use_prev_block {
                            // Use previous block info
                            let meaningful_bits = 64 - prev_leading_zeros - prev_trailing_zeros;
                            if let Some(xor_bits) = bit_reader.read_bits(meaningful_bits) {
                                let xor = xor_bits << prev_trailing_zeros;
                                current_value ^= xor;
                            }
                        } else {
                            // New block info
                            if let Some(leading_zeros) = bit_reader.read_bits(5) {
                                if let Some(meaningful_bits) = bit_reader.read_bits(6) {
                                    if let Some(xor_bits) = bit_reader.read_bits(meaningful_bits as u8) {
                                        prev_leading_zeros = leading_zeros as u8;
                                        prev_trailing_zeros = 64 - prev_leading_zeros - meaningful_bits as u8;
                                        let xor = xor_bits << prev_trailing_zeros;
                                        current_value ^= xor;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            result.extend_from_slice(&write_float_bits(current_value, use_f64));
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
fn read_float_bits(data: &[u8], use_f64: bool) -> u64 {
    if use_f64 {
        f64::from_le_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]).to_bits()
    } else {
        f32::from_le_bytes([data[0], data[1], data[2], data[3]]).to_bits() as u64
    }
}

fn write_float_bits(bits: u64, use_f64: bool) -> Vec<u8> {
    if use_f64 {
        f64::from_bits(bits).to_le_bytes().to_vec()
    } else {
        f32::from_bits(bits as u32).to_le_bytes().to_vec()
    }
}

// Simple bit writer/reader for Gorilla encoding
struct BitWriter {
    buffer: Vec<u8>,
    current_byte: u8,
    bit_count: u8,
}

impl BitWriter {
    fn new() -> Self {
        Self {
            buffer: Vec::new(),
            current_byte: 0,
            bit_count: 0,
        }
    }

    fn write_bit(&mut self, bit: bool) {
        if bit {
            self.current_byte |= 1 << (7 - self.bit_count);
        }
        self.bit_count += 1;
        
        if self.bit_count == 8 {
            self.buffer.push(self.current_byte);
            self.current_byte = 0;
            self.bit_count = 0;
        }
    }

    fn write_bits(&mut self, value: u64, count: u8) {
        for i in (0..count).rev() {
            let bit = (value >> i) & 1 != 0;
            self.write_bit(bit);
        }
    }

    fn compress_buf(mut self) -> Vec<u8> {
        if self.bit_count > 0 {
            self.buffer.push(self.current_byte);
        }
        self.buffer
    }
}

struct BitReader<'a> {
    data: &'a [u8],
    byte_pos: usize,
    bit_pos: u8,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            byte_pos: 0,
            bit_pos: 0,
        }
    }

    fn read_bit(&mut self) -> Option<bool> {
        if self.byte_pos >= self.data.len() {
            return None;
        }

        let bit = (self.data[self.byte_pos] >> (7 - self.bit_pos)) & 1 != 0;
        self.bit_pos += 1;
        
        if self.bit_pos == 8 {
            self.byte_pos += 1;
            self.bit_pos = 0;
        }
        
        Some(bit)
    }

    fn read_bits(&mut self, count: u8) -> Option<u64> {
        let mut result = 0u64;
        for _ in 0..count {
            if let Some(bit) = self.read_bit() {
                result = (result << 1) | (if bit { 1 } else { 0 });
            } else {
                return None;
            }
        }
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gorilla_for_val_compression() {
        // Create test data with slowly changing floating point values
        let mut data = Vec::new();
        let mut value = 100.0f64;
        
        for i in 0..50 {
            value += (i as f64) * 0.01; // Small incremental changes
            data.extend_from_slice(&value.to_le_bytes());
        }

        let gorilla = Gorilla::default();
        let mut compressor = gorilla.create_compressor().unwrap();
        let compressed = compressor.compress_val(&data).unwrap();
        
        let mut decompressor = Gorilla::new_decompression().unwrap();
        let decompressed = decompressor.decompress_val(&compressed).unwrap();
        
        assert_eq!(data, decompressed.as_ref());
        println!("Gorilla val compression - Original: {}, Compressed: {}", data.len(), compressed.len());
    }

    #[test]
    fn test_gorilla_for_buf_compression() {
        // Create test data with time series floating point values
        let mut data = Vec::new();
        let mut value = 50.0f64;
        
        for i in 0..30 {
            value += (i as f64) * 0.05; // Small incremental changes
            data.extend_from_slice(&value.to_le_bytes());
        }

        let buf = Buf::from_zero_padded(data.clone());
        let gorilla = Gorilla::default();
        
        let mut compressor = gorilla.create_compressor().unwrap();
        let compressed_buf = compressor.compress_buf(buf.clone()).unwrap();
        
        let mut decompressor = Gorilla::new_decompression().unwrap();
        let decompressed_buf = decompressor.decompress_buf(compressed_buf).unwrap();
        
        assert_eq!(buf.as_ref(), decompressed_buf.as_ref());
        println!("Gorilla buf compression - Original: {}, Compressed: {}", buf.len(), decompressed_buf.len());
    }
}