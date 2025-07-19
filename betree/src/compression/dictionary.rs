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
    fn finish_ext(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        // For simplicity, assume fixed-size values (like 8-byte values)
        let value_size = 8usize;
        if data.len() % value_size != 0 {
            // Fall back to no compression if data doesn't fit fixed-size pattern
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

        Ok(result)
    }

    fn finish(&mut self, data: Buf) -> Result<Buf> {
        let compressed_data = self.finish_ext(data.as_ref())?;
        Ok(Buf::from_zero_padded(compressed_data))
    }
}

impl DecompressionState for DictionaryDecompression {
    fn decompress_ext(&mut self, data: &[u8], _len: usize) -> Result<SlicedCowBytes> {
        if data.len() < 4 {
            return Ok(SlicedCowBytes::from(data.to_vec()));
        }

        let mut pos = 0;
        
        // Read header
        let dict_size = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        let value_size = data[pos] as usize;
        pos += 1;
        let index_size = data[pos] as usize;
        pos += 1;

        // Read dictionary
        let dict_bytes = dict_size * value_size;
        if pos + dict_bytes > data.len() {
            return Ok(SlicedCowBytes::from(data.to_vec()));
        }
        
        let dict_data = &data[pos..pos + dict_bytes];
        pos += dict_bytes;

        // Build dictionary
        let mut dictionary = Vec::with_capacity(dict_size);
        for i in 0..dict_size {
            let start = i * value_size;
            dictionary.push(&dict_data[start..start + value_size]);
        }

        // Decode indices
        let mut result = Vec::new();
        while pos < data.len() {
            let index = if index_size == 1 {
                if pos >= data.len() { break; }
                let idx = data[pos] as usize;
                pos += 1;
                idx
            } else {
                if pos + 1 >= data.len() { break; }
                let idx = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                pos += 2;
                idx
            };

            if index < dictionary.len() {
                // Regular dictionary lookup
                result.extend_from_slice(dictionary[index]);
            } else {
                // Escaped value
                if pos + value_size > data.len() { break; }
                result.extend_from_slice(&data[pos..pos + value_size]);
                pos += value_size;
            }
        }

        Ok(SlicedCowBytes::from(result))
    }

    fn decompress(&mut self, data: Buf) -> Result<Buf> {
        let decompressed = self.decompress_ext(data.as_ref(), 0)?;
        Ok(Buf::from_zero_padded(decompressed.as_ref().to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dictionary_compression() {
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
        let compressed = compressor.finish_ext(&data).unwrap();
        
        let mut decompressor = Dictionary::new_decompression().unwrap();
        let decompressed = decompressor.decompress_ext(&compressed, data.len()).unwrap();
        
        assert_eq!(data, decompressed.as_ref());
        println!("Original size: {}, Compressed size: {}", data.len(), compressed.len());
    }
}