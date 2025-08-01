//! Run-Length Encoding implementation
//! Ideal for: Data with many consecutive repeated values (sorted columns, sparse data)

use super::{CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result};
use crate::{
    buffer::Buf,
    cow_bytes::SlicedCowBytes,
    size::StaticSize,
};
use serde::{Deserialize, Serialize};

/// RLE configuration
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Rle {
    /// Minimum run length to compress (shorter runs are stored uncompressed)
    pub min_run_length: u8,
    /// Value size in bytes (1, 2, 4, or 8)
    pub value_size: u8,
}

impl Default for Rle {
    fn default() -> Self {
        Self {
            min_run_length: 3, // Compress runs of 3+ identical values
            value_size: 8,     // Default to 8-byte values
        }
    }
}

impl StaticSize for Rle {
    fn static_size() -> usize {
        std::mem::size_of::<Rle>()
    }
}

/// RLE compression state
#[derive(Debug)]
pub struct RleCompression {
    config: Rle,
}

/// RLE decompression state
#[derive(Debug)]
pub struct RleDecompression;

impl CompressionBuilder for Rle {
    fn create_compressor(&self) -> Result<Box<dyn CompressionState>> {
        Ok(Box::new(RleCompression { config: *self }))
    }

    fn decompression_tag(&self) -> DecompressionTag {
        DecompressionTag::Rle
    }
}

impl Rle {
    /// Create a new RLE decompression state
    pub fn new_decompression() -> Result<Box<dyn DecompressionState>> {
        Ok(Box::new(RleDecompression))
    }
}

/// RLE format:
/// [value_size: u8][run_count: u32][runs...]
/// Each run: [type: u8][data...]
/// type=0: literal run [count: u16][values...]
/// type=1: repeated run [count: u32][value]
impl CompressionState for RleCompression {
    fn compress_val(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let value_size = self.config.value_size as usize;
        if data.len() % value_size != 0 {
            // Fall back to no compression if data doesn't fit value size
            return Ok(data.to_vec());
        }

        let value_count = data.len() / value_size;
        if value_count == 0 {
            return Ok(data.to_vec());
        }

        let mut result = Vec::new();
        result.push(value_size as u8); // Store value size
        
        let run_count_pos = result.len();
        result.extend_from_slice(&0u32.to_le_bytes()); // Placeholder for run count
        
        let mut run_count = 0u32;
        let mut pos = 0;

        while pos < value_count {
            let current_value = &data[pos * value_size..(pos + 1) * value_size];
            let mut run_length = 1;

            // Count consecutive identical values
            while pos + run_length < value_count {
                let next_value = &data[(pos + run_length) * value_size..(pos + run_length + 1) * value_size];
                if current_value == next_value {
                    run_length += 1;
                } else {
                    break;
                }
            }

            if run_length >= self.config.min_run_length as usize {
                // Repeated run
                result.push(1u8); // Type: repeated
                result.extend_from_slice(&(run_length as u32).to_le_bytes());
                result.extend_from_slice(current_value);
            } else {
                // Literal run - find end of non-repeating sequence
                let mut literal_length = run_length;
                while pos + literal_length < value_count {
                    let start_check = pos + literal_length;
                    let check_value = &data[start_check * value_size..(start_check + 1) * value_size];
                    
                    // Check if we're starting a new repeating sequence
                    let mut check_run = 1;
                    while start_check + check_run < value_count {
                        let next_check = &data[(start_check + check_run) * value_size..(start_check + check_run + 1) * value_size];
                        if check_value == next_check {
                            check_run += 1;
                        } else {
                            break;
                        }
                    }

                    if check_run >= self.config.min_run_length as usize {
                        // Found a repeating sequence, end literal run here
                        break;
                    } else {
                        literal_length += check_run;
                    }
                }

                // Literal run
                result.push(0u8); // Type: literal
                result.extend_from_slice(&(literal_length as u16).to_le_bytes());
                for i in 0..literal_length {
                    let value = &data[(pos + i) * value_size..(pos + i + 1) * value_size];
                    result.extend_from_slice(value);
                }
                run_length = literal_length;
            }

            pos += run_length;
            run_count += 1;
        }

        // Update run count
        result[run_count_pos..run_count_pos + 4].copy_from_slice(&run_count.to_le_bytes());

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

impl DecompressionState for RleDecompression {
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

        if compressed.len() < 5 {
            return Ok(SlicedCowBytes::from(compressed.to_vec()));
        }

        let mut pos = 0;
        let value_size = compressed[pos] as usize;
        pos += 1;

        let run_count = u32::from_le_bytes([compressed[pos], compressed[pos + 1], compressed[pos + 2], compressed[pos + 3]]) as usize;
        pos += 4;

        let mut result = Vec::new();

        for _ in 0..run_count {
            if pos >= compressed.len() { break; }
            
            let run_type = compressed[pos];
            pos += 1;

            match run_type {
                0 => {
                    // Literal run
                    if pos + 2 > compressed.len() { break; }
                    let count = u16::from_le_bytes([compressed[pos], compressed[pos + 1]]) as usize;
                    pos += 2;

                    if pos + count * value_size > compressed.len() { break; }
                    result.extend_from_slice(&compressed[pos..pos + count * value_size]);
                    pos += count * value_size;
                }
                1 => {
                    // Repeated run
                    if pos + 4 > compressed.len() { break; }
                    let count = u32::from_le_bytes([compressed[pos], compressed[pos + 1], compressed[pos + 2], compressed[pos + 3]]) as usize;
                    pos += 4;

                    if pos + value_size > compressed.len() { break; }
                    let value = &compressed[pos..pos + value_size];
                    for _ in 0..count {
                        result.extend_from_slice(value);
                    }
                    pos += value_size;
                }
                _ => {
                    // Unknown run type
                    break;
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rle_for_val_compression() {
        // Create test data with runs of repeated values
        let mut data = Vec::new();
        let value1 = b"aaaaaaaa";
        let value2 = b"bbbbbbbb";
        let value3 = b"cccccccc";
        
        // Pattern: 5 a's, 1 b, 3 c's, 1 a, 4 b's
        for _ in 0..5 { data.extend_from_slice(value1); }
        data.extend_from_slice(value2);
        for _ in 0..3 { data.extend_from_slice(value3); }
        data.extend_from_slice(value1);
        for _ in 0..4 { data.extend_from_slice(value2); }

        let rle = Rle::default();
        let mut compressor = rle.create_compressor().unwrap();
        let compressed = compressor.compress_val(&data).unwrap();
        
        let mut decompressor = Rle::new_decompression().unwrap();
        let decompressed = decompressor.decompress_val(&compressed).unwrap();
        
        assert_eq!(data, decompressed.as_ref());
        println!("RLE val compression - Original: {}, Compressed: {}", data.len(), compressed.len());
    }

    #[test]
    fn test_rle_for_buf_compression() {
        // Create test data with runs of repeated values
        let mut data = Vec::new();
        let value1 = b"testval1";
        let value2 = b"testval2";
        
        // Pattern with good RLE compression potential
        for _ in 0..10 { data.extend_from_slice(value1); }
        for _ in 0..8 { data.extend_from_slice(value2); }
        for _ in 0..6 { data.extend_from_slice(value1); }

        let buf = Buf::from_zero_padded(data.clone());
        let rle = Rle::default();
        
        let mut compressor = rle.create_compressor().unwrap();
        let compressed_buf = compressor.compress_buf(buf.clone()).unwrap();
        
        let mut decompressor = Rle::new_decompression().unwrap();
        let decompressed_buf = decompressor.decompress_buf(compressed_buf).unwrap();
        
        assert_eq!(buf.as_ref(), decompressed_buf.as_ref());
        println!("RLE buf compression - Original: {}, Compressed: {}", buf.len(), decompressed_buf.len());
    }
}