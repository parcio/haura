use super::{ CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result };
use crate::size::StaticSize;
use crate::buffer::Buf;
use crate::cow_bytes::SlicedCowBytes;




use serde::{Deserialize, Serialize};






use lz4::block;
use lz4::block::CompressionMode;
/// LZ4 compression. (<https://github.com/lz4/lz4>)
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Lz4 {
    /// The compression level which describes the trade-off between
    /// compression ratio and compression speed.
    /// Lower level provides faster compression at a worse ratio.
    /// Maximum level is 16, higher values will count as 16.
    pub level: u8,
}

pub struct Lz4Compression {
    level: u8,
}

pub struct Lz4Decompression;

impl StaticSize for Lz4 {
    fn static_size() -> usize {
        1
    }
}

impl CompressionBuilder for Lz4 {
    fn create_compressor(&self) -> Result<Box<dyn CompressionState>> {
        // Just store the level, create encoder only when needed
        Ok(Box::new(Lz4Compression { level: self.level }))
    }

    fn decompression_tag(&self) -> DecompressionTag {
        DecompressionTag::Lz4
    }
}

impl Lz4 {
    pub fn new_decompression() -> Result<Box<dyn DecompressionState>> {
        Ok(Box::new(Lz4Decompression))
    }
}







impl CompressionState for Lz4Compression {
    fn finish_ext(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let mode = CompressionMode::HIGHCOMPRESSION(self.level as i32);
        // Use block-level compression - much more efficient than creating encoder each time
        block::compress(data, Some(mode), false)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("LZ4 compression failed: {:?}", e)).into())
    }

    fn finish(&mut self, data: Buf) -> Result<Buf> {
        // Use block-level compression - much more efficient than creating encoder each time
        let mode = CompressionMode::HIGHCOMPRESSION(self.level as i32);
        let compressed_data = block::compress(data.as_ref(), Some(mode), false)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("LZ4 compression failed: {:?}", e)))?;
        
        Ok(Buf::from_zero_padded(compressed_data))
    }
}


impl DecompressionState for Lz4Decompression {
    fn decompress_ext(&mut self, data: &[u8], len: usize) -> Result<SlicedCowBytes> {
        // Use block-level decompression to match block-level compression
        let decompressed = block::decompress(data, Some(len as i32))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("LZ4 decompression failed: {:?}", e)))?;
        
        Ok(SlicedCowBytes::from(decompressed))
    }

    fn decompress(&mut self, data: Buf) -> Result<Buf> {
        // Use block-level decompression to match block-level compression
        let decompressed = block::decompress(data.as_ref(), None)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("LZ4 decompression failed: {:?}", e)))?;
        
        Ok(Buf::from_zero_padded(decompressed))
    }
}


// impl DecompressionState for Lz4Decompression {
//     fn decompress_ext(&mut self, data: &[u8], _len: usize) -> Result<SlicedCowBytes> {
//         let mut buf = BufWrite::default(); // Let it grow as needed
//         let mut decoder = Decoder::new(data)?;

//         io::copy(&mut decoder, &mut buf)?;
//         Ok(buf.as_sliced_cow_bytes())
//     }

//     fn decompress(&mut self, data: Buf) -> Result<Buf> {
//         let mut buf = BufWrite::default(); // Let it grow as needed
//         let mut decoder = Decoder::new(data.as_ref())?;

//         io::copy(&mut decoder, &mut buf)?;
//         Ok(buf.into_buf())
//     }
// }
