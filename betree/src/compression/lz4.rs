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
        use crate::buffer::BufWrite;
        use crate::vdev::Block;
        use std::io::Write;
        
        let mode = CompressionMode::HIGHCOMPRESSION(self.level as i32);
        let compressed_data = block::compress(data.as_ref(), Some(mode), false)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("LZ4 compression failed: {:?}", e)))?;

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


impl DecompressionState for Lz4Decompression {
    fn decompress_ext(&mut self, data: &[u8], len: usize) -> Result<SlicedCowBytes> {
        // Use block-level decompression to match block-level compression
        let decompressed = block::decompress(data, Some(len as i32))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("LZ4 decompression failed: {:?}", e)))?;
        
        Ok(SlicedCowBytes::from(decompressed))
    }

    fn decompress(&mut self, data: Buf) -> Result<Buf> {
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

        let uncompressed_data = block::decompress(compressed, Some(uncomp_size as i32))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("LZ4 decompression failed: {:?}", e)))?;

        let mut buf = BufWrite::with_capacity(Block::round_up_from_bytes(uncomp_size as u32));
        buf.write_all(&uncompressed_data)?;
        Ok(buf.into_buf())
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
