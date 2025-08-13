//! Dictionary encoding implementation
//! Ideal for: Columns with repeated values (strings, enums, small integer sets)
//! Replaces frequent values with shorter dictionary indices

use super::{CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result};
use crate::{
    buffer::Buf,
    cow_bytes::SlicedCowBytes,
    size::StaticSize,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Dictionary encoding configuration
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Dictionary {
    /// Maximum dictionary size (number of unique values to track)
    pub max_dict_size: u16,
    /// Minimum value frequency to include in dictionary
    pub min_frequency: u8,
}

impl Default for Dictionary {
    fn default() -> Self {
        Self {
            max_dict_size: 256, // Can use single byte indices
            min_frequency: 2,   // Must appear at least twice
        }
    }
}

impl StaticSize for Dictionary {
    fn static_size() -> usize {
        std::mem::size_of::<Dictionary>()
    }
}

/// Dictionary compression state
#[derive(Debug)]
pub struct DictionaryCompression {
    config: Dictionary,
}

/// Dictionary decompression state
#[derive(Debug)]
pub struct DictionaryDecompression;

impl CompressionBuilder for Dictionary {
    fn create_compressor(&self) -> Result<Box<dyn CompressionState>> {
        Ok(Box::new(DictionaryCompression { config: *self }))
    }

    fn decompression_tag(&self) -> DecompressionTag {
        DecompressionTag::Dictionary
    }
}

impl Dictionary {
    /// Create a new Dictionary decompression state
    pub fn new_decompression() -> Result<Box<dyn DecompressionState>> {
        Ok(Box::new(DictionaryDecompression))
    }
}

/// Dictionary encoding format:
/// [dict_size: u16][value_size: u8][index_size: u8][dictionary][indices]
/// Where:
/// - dict_size: number of dictionary entries
/// - value_size: size of each value in bytes  
/// - index_size: size of each index (1 or 2 bytes)
/// - dictionary: concatenated values
/// - indices: array of indices into dictionary
impl CompressionState for DictionaryCompression {
    fn compress_val(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        // For text data, treat each byte as a value (byte-level dictionary)
        let value_size = 1usize;
        if data.is_empty() {
            return Ok(data.to_vec());
        }

        let value_count = data.len() / value_size;
        let mut value_freq: HashMap<&[u8], u32> = HashMap::new();
        
        // Count frequencies
        for i in 0..value_count {
            let value = &data[i * value_size..(i + 1) * value_size];
            *value_freq.entry(value).or_insert(0) += 1;
        }

        // Build dictionary of frequent values
        let mut dictionary: Vec<&[u8]> = value_freq
            .iter()
            .filter(|(_, &freq)| freq >= self.config.min_frequency as u32)
            .map(|(&value, _)| value)
            .collect();

        // Limit dictionary size
        dictionary.truncate(self.config.max_dict_size as usize);

        if dictionary.is_empty() {
            // No benefit from dictionary encoding
            return Ok(data.to_vec());
        }

        // Create value -> index mapping
        let value_to_index: HashMap<&[u8], u16> = dictionary
            .iter()
            .enumerate()
            .map(|(i, &value)| (value, i as u16))
            .collect();

        // Choose index size (1 or 2 bytes)
        let index_size = if dictionary.len() <= 256 { 1u8 } else { 2u8 };

        // Build compressed format
        let mut result = Vec::new();
        
        // Header
        result.extend_from_slice(&(dictionary.len() as u16).to_le_bytes());
        result.push(value_size as u8);
        result.push(index_size);

        // Dictionary
        for &value in &dictionary {
            result.extend_from_slice(value);
        }

        // Indices
        for i in 0..value_count {
            let value = &data[i * value_size..(i + 1) * value_size];
            if let Some(&index) = value_to_index.get(value) {
                // Value is in dictionary
                if index_size == 1 {
                    result.push(index as u8);
                } else {
                    result.extend_from_slice(&index.to_le_bytes());
                }
            } else {
                // Value not in dictionary - store special index + full value
                let escape_index = dictionary.len() as u16;
                if index_size == 1 {
                    result.push(escape_index as u8);
                } else {
                    result.extend_from_slice(&escape_index.to_le_bytes());
                }
                result.extend_from_slice(value);
            }
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

impl DecompressionState for DictionaryDecompression {
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

        if compressed.len() < 4 {
            return Ok(SlicedCowBytes::from(compressed.to_vec()));
        }

        let mut pos = 0;
        
        // Read header
        let dict_size = u16::from_le_bytes([compressed[pos], compressed[pos + 1]]) as usize;
        pos += 2;
        let value_size = compressed[pos] as usize;
        pos += 1;
        let index_size = compressed[pos] as usize;
        pos += 1;

        // Read dictionary
        let dict_bytes = dict_size * value_size;
        if pos + dict_bytes > compressed.len() {
            return Ok(SlicedCowBytes::from(compressed.to_vec()));
        }
        
        let dict_data = &compressed[pos..pos + dict_bytes];
        pos += dict_bytes;

        // Build dictionary
        let mut dictionary = Vec::with_capacity(dict_size);
        for i in 0..dict_size {
            let start = i * value_size;
            dictionary.push(&dict_data[start..start + value_size]);
        }

        // Decode indices
        let mut result = Vec::new();
        while pos < compressed.len() {
            let index = if index_size == 1 {
                if pos >= compressed.len() { break; }
                let idx = compressed[pos] as usize;
                pos += 1;
                idx
            } else {
                if pos + 1 >= compressed.len() { break; }
                let idx = u16::from_le_bytes([compressed[pos], compressed[pos + 1]]) as usize;
                pos += 2;
                idx
            };

            if index < dictionary.len() {
                // Regular dictionary lookup
                result.extend_from_slice(dictionary[index]);
            } else {
                // Escaped value
                if pos + value_size > compressed.len() { break; }
                result.extend_from_slice(&compressed[pos..pos + value_size]);
                pos += value_size;
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
    fn test_dictionary_for_val_compression() {
        // Create test data with repeated 8-byte values
        let mut data = Vec::new();
        let values = [
            b"value001", b"value002", b"value001", b"value003", 
            b"value001", b"value002", b"value004", b"value001"
        ];
        
        for &value in &values {
            data.extend_from_slice(value);
        }

        let dictionary = Dictionary::default();
        let mut compressor = dictionary.create_compressor().unwrap();
        let compressed = compressor.compress_val(&data).unwrap();
        
        let mut decompressor = Dictionary::new_decompression().unwrap();
        let decompressed = decompressor.decompress_val(&compressed).unwrap();
        
        assert_eq!(data, decompressed.as_ref());
        println!("Dictionary val compression - Original: {}, Compressed: {}", data.len(), compressed.len());
    }

    #[test]
    fn test_dictionary_for_buf_compression() {
        // Create test data with repeated 8-byte values
        let mut data = Vec::new();
        let values = [
            b"dictval1", b"dictval2", b"dictval1", b"dictval3", 
            b"dictval2", b"dictval1", b"dictval4", b"dictval2"
        ];
        
        for &value in &values {
            data.extend_from_slice(value);
        }

        let buf = Buf::from_zero_padded(data.clone());
        let dictionary = Dictionary::default();
        
        let mut compressor = dictionary.create_compressor().unwrap();
        let compressed_buf = compressor.compress_buf(buf.clone()).unwrap();
        
        let mut decompressor = Dictionary::new_decompression().unwrap();
        let decompressed_buf = decompressor.decompress_buf(compressed_buf).unwrap();
        
        assert_eq!(buf.as_ref(), decompressed_buf.as_ref());
        println!("Dictionary buf compression - Original: {}, Compressed: {}", buf.len(), decompressed_buf.len());
    }
}