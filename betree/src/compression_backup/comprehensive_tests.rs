//! Comprehensive unit tests for all compression techniques
//! Tests both finish/decompress and finish_ext/decompress_ext methods

use super::*;
use crate::buffer::Buf;
use rand::RngCore;

/// Test data generator for different compression scenarios
struct TestDataGenerator;

impl TestDataGenerator {
    /// Generate random data (worst case for most compression algorithms)
    fn random_data(size: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut data = vec![0u8; size];
        rng.fill_bytes(&mut data);
        data
    }

    /// Generate highly compressible data (repeated patterns)
    fn repeated_data(pattern: &[u8], repetitions: usize) -> Vec<u8> {
        pattern.repeat(repetitions)
    }

    /// Generate sequential data (good for delta compression)
    fn sequential_data(start: i64, count: usize) -> Vec<u8> {
        let mut data = Vec::new();
        for i in 0..count {
            let value = start + i as i64;
            data.extend_from_slice(&value.to_le_bytes());
        }
        data
    }

    /// Generate floating point time series data (good for Gorilla compression)
    fn time_series_data(base: f64, count: usize) -> Vec<u8> {
        let mut data = Vec::new();
        let mut value = base;
        for i in 0..count {
            value += (i as f64) * 0.01; // Small incremental changes
            data.extend_from_slice(&value.to_le_bytes());
        }
        data
    }

    /// Generate dictionary-friendly data (repeated 8-byte values)
    fn dictionary_data() -> Vec<u8> {
        let mut data = Vec::new();
        let values = [
            b"value001", b"value002", b"value001", b"value003", 
            b"value001", b"value002", b"value004", b"value001",
            b"value005", b"value001", b"value002", b"value006"
        ];
        
        for &value in &values {
            data.extend_from_slice(value);
        }
        data
    }

    /// Generate RLE-friendly data (runs of repeated values)
    fn rle_data() -> Vec<u8> {
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
        data
    }

    /// Generate text data with patterns (good for LZ-style compression)
    fn text_pattern_data() -> Vec<u8> {
        let pattern = b"This is a test pattern that repeats often. ";
        let mut data = Vec::new();
        for _ in 0..50 {
            data.extend_from_slice(pattern);
        }
        data
    }
}

/// Generic test function for compression round-trip using finish_ext/decompress_ext
fn test_compression_ext_round_trip<T>(
    compression_config: T,
    test_data: &[u8],
    test_name: &str,
) where
    T: CompressionBuilder + Clone + std::fmt::Debug,
{
    println!("Testing {} with finish_ext/decompress_ext - {}", 
             std::any::type_name::<T>(), test_name);

    // Test compression
    let mut compressor = compression_config.create_compressor()
        .expect("Failed to create compressor");
    let compressed = compressor.finish_ext(test_data)
        .expect("Compression failed");

    // Test decompression
    let mut decompressor = compression_config.decompression_tag().new_decompression()
        .expect("Failed to create decompressor");
    let decompressed = decompressor.decompress_ext(&compressed, test_data.len())
        .expect("Decompression failed");

    // Verify round-trip
    assert_eq!(test_data, decompressed.as_ref(), 
               "Round-trip failed for {} with finish_ext/decompress_ext", test_name);

    println!("  Original size: {}, Compressed size: {}, Ratio: {:.2}%", 
             test_data.len(), compressed.len(), 
             (compressed.len() as f64 / test_data.len() as f64) * 100.0);
}

/// Generic test function for compression round-trip using finish/decompress
fn test_compression_buf_round_trip<T>(
    compression_config: T,
    test_data: &[u8],
    test_name: &str,
) where
    T: CompressionBuilder + Clone + std::fmt::Debug,
{
    println!("Testing {} with finish/decompress - {}", 
             std::any::type_name::<T>(), test_name);

    let buf = Buf::from_zero_padded(test_data.to_vec());

    // Test compression
    let mut compressor = compression_config.create_compressor()
        .expect("Failed to create compressor");
    let compressed_buf = compressor.finish(buf.clone())
        .expect("Compression failed");

    // Test decompression
    let mut decompressor = compression_config.decompression_tag().new_decompression()
        .expect("Failed to create decompressor");
    let decompressed_buf = decompressor.decompress(compressed_buf)
        .expect("Decompression failed");

    // Verify round-trip
    assert_eq!(buf.as_ref(), decompressed_buf.as_ref(), 
               "Round-trip failed for {} with finish/decompress", test_name);

    println!("  Original size: {}, Compressed size: {}, Ratio: {:.2}%", 
             buf.len(), decompressed_buf.len(), 
             (decompressed_buf.len() as f64 / buf.len() as f64) * 100.0);
}

/// Test both ext and buf methods for a compression technique
fn test_compression_both_methods<T>(
    compression_config: T,
    test_data: &[u8],
    test_name: &str,
) where
    T: CompressionBuilder + Clone + std::fmt::Debug,
{
    test_compression_ext_round_trip(compression_config.clone(), test_data, test_name);
    test_compression_buf_round_trip(compression_config, test_data, test_name);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none_compression() {
        let test_cases = vec![
            (TestDataGenerator::random_data(1024), "random_data"),
            (TestDataGenerator::repeated_data(b"test", 100), "repeated_data"),
            (b"Hello, World!".to_vec(), "simple_text"),
            (Vec::new(), "empty_data"),
        ];

        for (data, name) in test_cases {
            test_compression_both_methods(None, &data, name);
        }
    }

    #[test]
    fn test_lz4_compression() {
        let lz4_configs = vec![
            Lz4 { level: 1 },
            Lz4 { level: 8 },
            Lz4 { level: 16 },
        ];

        let test_cases = vec![
            (TestDataGenerator::random_data(4096), "random_data"),
            (TestDataGenerator::repeated_data(b"LZ4 test pattern ", 200), "repeated_pattern"),
            (TestDataGenerator::text_pattern_data(), "text_pattern"),
            (b"Short text".to_vec(), "short_text"),
        ];

        for lz4_config in lz4_configs {
            for (data, name) in &test_cases {
                let test_name = format!("{}_level_{}", name, lz4_config.level);
                test_compression_both_methods(lz4_config, data, &test_name);
            }
        }
    }

    #[test]
    fn test_zstd_compression() {
        let zstd_configs = vec![
            Zstd { level: 1 },
            Zstd { level: 6 },
            Zstd { level: 15 },
        ];

        let test_cases = vec![
            (TestDataGenerator::random_data(8192), "random_data"),
            (TestDataGenerator::repeated_data(b"Zstd compression test ", 300), "repeated_pattern"),
            (TestDataGenerator::text_pattern_data(), "text_pattern"),
            (b"Zstd short test".to_vec(), "short_text"),
        ];

        for zstd_config in zstd_configs {
            for (data, name) in &test_cases {
                let test_name = format!("{}_level_{}", name, zstd_config.level);
                test_compression_both_methods(zstd_config, data, &test_name);
            }
        }
    }

    #[test]
    fn test_snappy_compression() {
        let snappy = Snappy::default();

        let test_cases = vec![
            (TestDataGenerator::random_data(2048), "random_data"),
            (TestDataGenerator::repeated_data(b"Snappy fast compression ", 150), "repeated_pattern"),
            (TestDataGenerator::text_pattern_data(), "text_pattern"),
            (b"Snappy test".to_vec(), "short_text"),
        ];

        for (data, name) in test_cases {
            test_compression_both_methods(snappy, &data, name);
        }
    }

    #[test]
    fn test_dictionary_compression() {
        let dictionary = Dictionary::default();

        let test_cases = vec![
            (TestDataGenerator::dictionary_data(), "dictionary_optimized"),
            (TestDataGenerator::repeated_data(b"dictword", 50), "repeated_8byte"),
            (TestDataGenerator::random_data(512), "random_data"),
        ];

        for (data, name) in test_cases {
            test_compression_both_methods(dictionary, &data, name);
        }
    }

    #[test]
    fn test_rle_compression() {
        let rle = Rle::default();

        let test_cases = vec![
            (TestDataGenerator::rle_data(), "rle_optimized"),
            (TestDataGenerator::repeated_data(b"sameval8", 100), "highly_repetitive"),
            (TestDataGenerator::random_data(512), "random_data"),
        ];

        for (data, name) in test_cases {
            test_compression_both_methods(rle, &data, name);
        }
    }

    #[test]
    fn test_delta_compression() {
        let delta = Delta::default();

        let test_cases = vec![
            (TestDataGenerator::sequential_data(1000, 100), "sequential_optimized"),
            (TestDataGenerator::sequential_data(0, 50), "sequential_from_zero"),
            (TestDataGenerator::random_data(400), "random_data"),
        ];

        for (data, name) in test_cases {
            test_compression_both_methods(delta, &data, name);
        }
    }

    #[test]
    fn test_gorilla_compression() {
        let gorilla = Gorilla::default();

        let test_cases = vec![
            (TestDataGenerator::time_series_data(100.0, 50), "time_series_optimized"),
            (TestDataGenerator::time_series_data(0.0, 25), "time_series_from_zero"),
            (TestDataGenerator::random_data(400), "random_data"),
        ];

        for (data, name) in test_cases {
            test_compression_both_methods(gorilla, &data, name);
        }
    }

    #[test]
    fn test_toast_compression() {
        let toast = Toast::default();

        let test_cases = vec![
            (TestDataGenerator::text_pattern_data(), "text_pattern_optimized"),
            (TestDataGenerator::repeated_data(b"TOAST compression test ", 100), "repeated_pattern"),
            (TestDataGenerator::random_data(1024), "random_data"),
            (b"small".to_vec(), "small_data"),
        ];

        for (data, name) in test_cases {
            test_compression_both_methods(toast, &data, name);
        }
    }

    #[test]
    fn test_edge_cases() {
        let compressions: Vec<Box<dyn CompressionBuilder>> = vec![
            Box::new(None),
            Box::new(Lz4 { level: 1 }),
            Box::new(Zstd { level: 1 }),
            Box::new(Snappy::default()),
            Box::new(Dictionary::default()),
            Box::new(Rle::default()),
            Box::new(Delta::default()),
            Box::new(Gorilla::default()),
            Box::new(Toast::default()),
        ];

        let edge_cases = vec![
            (Vec::new(), "empty_data"),
            (vec![0u8; 1], "single_byte"),
            (vec![255u8; 10], "all_same_bytes"),
            (TestDataGenerator::random_data(1), "single_random_byte"),
        ];

        for compression in &compressions {
            for (data, name) in &edge_cases {
                let compression_name = format!("{:?}", compression);
                let test_name = format!("{}_{}", compression_name, name);
                
                // Test ext methods
                let mut compressor = compression.create_compressor().unwrap();
                let compressed = compressor.finish_ext(data).unwrap();
                
                let mut decompressor = compression.decompression_tag().new_decompression().unwrap();
                let decompressed = decompressor.decompress_ext(&compressed, data.len()).unwrap();
                
                assert_eq!(data, decompressed.as_ref(), "Edge case failed: {}", test_name);

                // Test buf methods
                let buf = Buf::from_zero_padded(data.clone());
                let mut compressor = compression.create_compressor().unwrap();
                let compressed_buf = compressor.finish(buf.clone()).unwrap();
                
                let mut decompressor = compression.decompression_tag().new_decompression().unwrap();
                let decompressed_buf = decompressor.decompress(compressed_buf).unwrap();
                
                assert_eq!(buf.as_ref(), decompressed_buf.as_ref(), "Edge case buf failed: {}", test_name);
            }
        }
    }

    #[test]
    fn test_large_data() {
        let large_data = TestDataGenerator::repeated_data(b"Large data test pattern for compression efficiency. ", 1000);
        
        let compressions: Vec<Box<dyn CompressionBuilder>> = vec![
            Box::new(None),
            Box::new(Lz4 { level: 8 }),
            Box::new(Zstd { level: 6 }),
            Box::new(Snappy::default()),
            Box::new(Dictionary::default()),
            Box::new(Rle::default()),
            Box::new(Delta::default()),
            Box::new(Gorilla::default()),
            Box::new(Toast::default()),
        ];

        for compression in compressions {
            let compression_name = format!("{:?}", compression);
            println!("Testing large data with {}", compression_name);
            
            // Test ext methods
            let mut compressor = compression.create_compressor().unwrap();
            let compressed = compressor.finish_ext(&large_data).unwrap();
            
            let mut decompressor = compression.decompression_tag().new_decompression().unwrap();
            let decompressed = decompressor.decompress_ext(&compressed, large_data.len()).unwrap();
            
            assert_eq!(large_data, decompressed.as_ref(), "Large data ext failed: {}", compression_name);

            // Test buf methods
            let buf = Buf::from_zero_padded(large_data.clone());
            let mut compressor = compression.create_compressor().unwrap();
            let compressed_buf = compressor.finish(buf.clone()).unwrap();
            
            let mut decompressor = compression.decompression_tag().new_decompression().unwrap();
            let decompressed_buf = decompressor.decompress(compressed_buf).unwrap();
            
            assert_eq!(buf.as_ref(), decompressed_buf.as_ref(), "Large data buf failed: {}", compression_name);
            
            println!("  Original: {} bytes, Compressed: {} bytes, Ratio: {:.2}%",
                     large_data.len(), compressed.len(),
                     (compressed.len() as f64 / large_data.len() as f64) * 100.0);
        }
    }

    #[test]
    fn test_compression_consistency() {
        // Test that finish_ext and finish produce equivalent results when decompressed
        let test_data = TestDataGenerator::text_pattern_data();
        
        let compressions: Vec<Box<dyn CompressionBuilder>> = vec![
            Box::new(None),
            Box::new(Lz4 { level: 4 }),
            Box::new(Zstd { level: 3 }),
            Box::new(Snappy::default()),
            Box::new(Dictionary::default()),
            Box::new(Rle::default()),
            Box::new(Delta::default()),
            Box::new(Gorilla::default()),
            Box::new(Toast::default()),
        ];

        for compression in compressions {
            let compression_name = format!("{:?}", compression);
            
            // Compress with finish_ext
            let mut compressor1 = compression.create_compressor().unwrap();
            let compressed_ext = compressor1.finish_ext(&test_data).unwrap();
            
            // Compress with finish
            let buf = Buf::from_zero_padded(test_data.clone());
            let mut compressor2 = compression.create_compressor().unwrap();
            let compressed_buf = compressor2.finish(buf).unwrap();
            
            // Decompress both with decompress_ext
            let mut decompressor1 = compression.decompression_tag().new_decompression().unwrap();
            let decompressed_ext = decompressor1.decompress_ext(&compressed_ext, test_data.len()).unwrap();
            
            let mut decompressor2 = compression.decompression_tag().new_decompression().unwrap();
            let decompressed_buf_as_ext = decompressor2.decompress_ext(compressed_buf.as_ref(), test_data.len()).unwrap();
            
            // Both should decompress to the original data
            assert_eq!(test_data, decompressed_ext.as_ref(), 
                       "finish_ext consistency failed for {}", compression_name);
            assert_eq!(test_data, decompressed_buf_as_ext.as_ref(), 
                       "finish consistency failed for {}", compression_name);
        }
    }
}