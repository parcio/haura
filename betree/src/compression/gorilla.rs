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
    fn finish_ext(&mut self, data: &[u8]) -> Result<Vec<u8>> {
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

        result.extend_from_slice(&bit_writer.finish());
        Ok(result)
    }

    fn finish(&mut self, data: Buf) -> Result<Buf> {
        let compressed_data = self.finish_ext(data.as_ref())?;
        Ok(Buf::from_zero_padded(compressed_data))
    }
}

impl DecompressionState for GorillaDecompression {
    fn decompress_ext(&mut self, data: &[u8], _len: usize) -> Result<SlicedCowBytes> {
        if data.len() < 6 {
            return Ok(SlicedCowBytes::from(data.to_vec()));
        }

        let mut pos = 0;
        let use_f64 = data[pos] != 0;
        pos += 1;
        
        let count = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        let value_size = if use_f64 { 8 } else { 4 };
        
        if pos + value_size > data.len() {
            return Ok(SlicedCowBytes::from(data.to_vec()));
        }

        let mut result = Vec::new();
        
        // Read first value
        let mut current_value = read_float_bits(&data[pos..pos + value_size], use_f64);
        result.extend_from_slice(&write_float_bits(current_value, use_f64));
        pos += value_size;

        let mut bit_reader = BitReader::new(&data[pos..]);
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

    fn decompress(&mut self, data: Buf) -> Result<Buf> {
        let decompressed = self.decompress_ext(data.as_ref(), 0)?;
        Ok(Buf::from_zero_padded(decompressed.as_ref().to_vec()))
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

    fn finish(mut self) -> Vec<u8> {
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
    fn test_gorilla_compression() {
        // Create test data with slowly changing floating point values
        let mut data = Vec::new();
        let mut value = 100.0f64;
        
        for i in 0..50 {
            value += (i as f64) * 0.01; // Small incremental changes
            data.extend_from_slice(&value.to_le_bytes());
        }

        let gorilla = Gorilla::default();
        let mut compressor = gorilla.create_compressor().unwrap();
        let compressed = compressor.finish_ext(&data).unwrap();
        
        let mut decompressor = Gorilla::new_decompression().unwrap();
        let decompressed = decompressor.decompress_ext(&compressed, data.len()).unwrap();
        
        assert_eq!(data, decompressed.as_ref());
        println!("Original size: {}, Compressed size: {}", data.len(), compressed.len());
    }
}