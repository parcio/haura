use super::{
    CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result,
    DEFAULT_BUFFER_SIZE,
};
use crate::{
    buffer::{Buf, BufWrite},
    size::StaticSize,
};
use serde::{Deserialize, Serialize};
use serde_json::to_vec;
use std::io;
use std::sync::{Arc, Mutex};
use crate::cow_bytes::{CowBytes, SlicedCowBytes};

/// No-op compression.
#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub struct None;
pub struct NoneCompression {
    buf: BufWrite,
}
pub struct NoneDecompression;

impl StaticSize for None {
    fn static_size() -> usize {
        0
    }
}

impl CompressionBuilder for None {
    fn create_compressor(&self) -> Result<Box<dyn CompressionState>> {
        Ok(Box::new(NoneCompression {
            buf: BufWrite::with_capacity(DEFAULT_BUFFER_SIZE),
        }))
    }

    fn decompression_tag(&self) -> DecompressionTag {
        DecompressionTag::None
    }
}

impl None {
    /// Start no-op decompression.
    pub fn new_decompression() -> Result<Box<dyn DecompressionState>> {
        Ok(Box::new(NoneDecompression))
    }
}

impl io::Write for NoneCompression {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.buf.write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl CompressionState for NoneCompression {
    fn compress_val(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let input_size = data.len();
        let result = data.to_vec();
        
        #[cfg(feature = "compression_metrics")]
        {
            // For None compression, compression time is essentially zero
            super::metrics::record_compression_metrics(input_size, result.len(), 0);
        }
        
        Ok(result)
    }

    fn compress_buf(&mut self, buf: Buf) -> Result<Buf> {
        let input_size = buf.len();
        
        #[cfg(feature = "compression_metrics")]
        {
            // For None compression, compression time is essentially zero
            super::metrics::record_compression_metrics(input_size, input_size, 0);
        }
        
        Ok(buf)
    }
}

impl DecompressionState for NoneDecompression {
    fn decompress_val(&mut self, data: &[u8]) -> Result<SlicedCowBytes> {
        let input_size = data.len();
        let result = SlicedCowBytes::from(data.to_vec());
        
        #[cfg(feature = "compression_metrics")]
        {
            // For None decompression, decompression time is essentially zero
            super::metrics::record_decompression_metrics(input_size, result.len(), 0);
        }
        
        Ok(result)
    }

    fn decompress_buf(&mut self, data: Buf) -> Result<Buf> {
        let input_size = data.len();
        
        #[cfg(feature = "compression_metrics")]
        {
            // For None decompression, decompression time is essentially zero
            super::metrics::record_decompression_metrics(input_size, input_size, 0);
        }
        
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none_for_val_compression() {
        let data = b"No compression test data - should pass through unchanged.";
        let none = None;
        
        let mut compressor = none.create_compressor().unwrap();
        let compressed = compressor.compress_val(data).unwrap();
        
        let mut decompressor = None::new_decompression().unwrap();
        let decompressed = decompressor.decompress_val(&compressed).unwrap();
        
        assert_eq!(data, decompressed.as_ref());
        assert_eq!(data.len(), compressed.len()); // No compression should mean same size
        println!("None val compression - Original: {}, 'Compressed': {}", data.len(), compressed.len());
    }

    #[test]
    fn test_none_for_buf_compression() {
        let data = b"No compression test with Buf interface - pass through.";
        let buf = Buf::from_zero_padded(data.to_vec());
        let none = None;
        
        let mut compressor = none.create_compressor().unwrap();
        let compressed_buf = compressor.compress_buf(buf.clone()).unwrap();
        
        let mut decompressor = None::new_decompression().unwrap();
        let decompressed_buf = decompressor.decompress_buf(compressed_buf).unwrap();
        
        assert_eq!(buf.as_ref(), decompressed_buf.as_ref());
        println!("None buf compression - Original: {}, 'Compressed': {}", buf.len(), decompressed_buf.len());
    }

    #[test]
    fn test_none_empty_data() {
        let data = b"";
        let none = None;
        
        let mut compressor = none.create_compressor().unwrap();
        let compressed = compressor.compress_val(data).unwrap();
        
        let mut decompressor = None::new_decompression().unwrap();
        let decompressed = decompressor.decompress_val(&compressed).unwrap();
        
        assert_eq!(data, decompressed.as_ref());
        assert_eq!(0, compressed.len());
    }
}
