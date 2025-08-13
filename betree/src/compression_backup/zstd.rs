use super::{CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result};
use crate::{
    buffer::{Buf, BufWrite},
    size::StaticSize,
    vdev::Block,
};
use serde::{Deserialize, Serialize};
use std::{io::Write, mem};
use std::io::{self, Read};
use zstd::stream::raw::{DParameter, Decoder};
use zstd_safe::FrameFormat;
use zstd::block;
// TODO: investigate pre-created dictionary payoff
use crate::cow_bytes::SlicedCowBytes;
/// Zstd compression. (<https://github.com/facebook/zstd>)
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Zstd {
    /// The compression level which describes the trade-off between
    /// compression ratio and compression speed.
    pub level: u8,
}
struct ZstdCompression {
    level: u8,
}
struct ZstdDecompression {
    writer: Decoder<'static>,
}

impl StaticSize for Zstd {
    fn static_size() -> usize {
        1
    }
}

use zstd::stream::raw::Operation;

impl CompressionBuilder for Zstd {
    fn create_compressor(&self) -> Result<Box<dyn CompressionState>> {
        // No need to create encoder here - block compression is more efficient
        Ok(Box::new(ZstdCompression { level: self.level }))
    }

    fn decompression_tag(&self) -> DecompressionTag {
        DecompressionTag::Zstd
    }
}

impl Zstd {
    /// Start Zstd decompression. The decompression level is by default encoded with the received data stream.
    pub fn new_decompression() -> Result<Box<dyn DecompressionState>> {
        let mut decoder = Decoder::new()?;
        decoder.set_parameter(DParameter::Format(FrameFormat::Magicless))?;
        // decoder.set_parameter(DParameter::ForceIgnoreChecksum(true))?;

        Ok(Box::new(ZstdDecompression { writer: decoder }))
    }
}

impl io::Write for ZstdCompression {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        unimplemented!()
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        unimplemented!()
    }

    fn flush(&mut self) -> io::Result<()> {
        unimplemented!()
    }
}

use std::time::Instant;
use speedy::{Readable, Writable};
const DATA_OFF: usize = mem::size_of::<u32>();

impl CompressionState for ZstdCompression {    
    fn compress_buf(&mut self, data: Buf) -> Result<Buf> {
        let compressed_data = block::compress(&data, self.level as i32)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;

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

    fn compress_val(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let compressed_data = block::compress(data, self.level as i32)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Compression error: {:?}", e)))?;

        let size = data.len() as u32;
        let comlen = compressed_data.len() as u32;

        let mut result = Vec::with_capacity(4 + 4 + compressed_data.len());
        result.extend_from_slice(&size.to_le_bytes());
        result.extend_from_slice(&comlen.to_le_bytes());
        result.extend_from_slice(&compressed_data);

        Ok(result)
    }
}

impl DecompressionState for ZstdDecompression {
    fn decompress_val(&mut self, data: &[u8]) -> Result<SlicedCowBytes>
    {
        if data.len() < 8 {
            bail!(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Input too short"));
        }

        let uncomp_size = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let comp_len = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;

        if data.len() < 8 + comp_len {
            bail!(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Compressed payload truncated"));
        }

        let compressed = &data[8..8 + comp_len];

        let uncompressed_data = block::decompress(compressed, uncomp_size)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Decompression error: {:?}", e)))?;

        Ok(SlicedCowBytes::from(uncompressed_data))
    }
    
    fn decompress_buf(&mut self, data: Buf) -> Result<Buf> {
        if data.len() < 8 {
            bail!(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Input too short"));
        }

        let uncomp_size = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let comp_len = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;

        if data.len() < 8 + comp_len {
            bail!(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Compressed payload truncated"));
        }

        let compressed = &data[8..8 + comp_len];

        let uncompressed_data = block::decompress(compressed, uncomp_size)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;

        let mut buf = BufWrite::with_capacity(Block::round_up_from_bytes(uncomp_size as u32));
        buf.write_all(&uncompressed_data)?;
        Ok(buf.into_buf())
    }
}

#[cfg(test)]
mod tests {
    use rand::RngCore;
    use super::*;

    #[test]
    fn test_zstd_for_val_compression() {
        let data = b"Zstd compression test with repeated patterns for better compression ratio. ".repeat(30);
        let zstd = Zstd { level: 6 };
        
        let mut compressor = zstd.create_compressor().unwrap();
        let compressed = compressor.compress_val(&data).unwrap();
        
        let mut decompressor = Zstd::new_decompression().unwrap();
        let decompressed = decompressor.decompress_val(&compressed).unwrap();
        
        assert_eq!(data, decompressed.as_ref());
        println!("Zstd val compression - Original: {}, Compressed: {}", data.len(), compressed.len());
    }

    #[test]
    fn test_zstd_for_buf_compression() {
        let data = b"Zstd test with Buf interface and compressible content. ".repeat(25);
        let buf = Buf::from_zero_padded(data.clone());
        let zstd = Zstd { level: 3 };
        
        let mut compressor = zstd.create_compressor().unwrap();
        let compressed_buf = compressor.compress_buf(buf.clone()).unwrap();
        
        let mut decompressor = Zstd::new_decompression().unwrap();
        let decompressed_buf = decompressor.decompress_buf(compressed_buf).unwrap();
        
        assert_eq!(buf.as_ref(), decompressed_buf.as_ref());
        println!("Zstd buf compression - Original: {}, Compressed: {}", buf.len(), decompressed_buf.len());
    }

    #[test]
    fn test_zstd_different_levels() {
        let data = b"Testing different Zstd compression levels with this repeated text pattern. ".repeat(15);
        
        for level in [1, 6, 15] {
            let zstd = Zstd { level };
            
            let mut compressor = zstd.create_compressor().unwrap();
            let compressed = compressor.compress_val(&data).unwrap();
            
            let mut decompressor = Zstd::new_decompression().unwrap();
            let decompressed = decompressor.decompress_val(&compressed).unwrap();
            
            assert_eq!(data, decompressed.as_ref());
            println!("Zstd level {} - Original: {}, Compressed: {}", level, data.len(), compressed.len());
        }
    }

    #[test]
    fn encode_then_decode() {
        let mut rng = rand::thread_rng();
        let mut buf = vec![42u8; 4 * 1024 * 1024];
        rng.fill_bytes(buf.as_mut());
        let buf = Buf::from_zero_padded(buf);
        let zstd = Zstd { level: 1 };
        let mut comp = zstd.create_compressor().unwrap();
        let c_buf = comp.compress_buf(buf.clone()).unwrap();
        let mut decomp = zstd.decompression_tag().new_decompression().unwrap();
        let d_buf = decomp.decompress_buf(c_buf).unwrap();
        assert_eq!(buf.as_ref().len(), d_buf.as_ref().len());
    }

    #[test]
    fn sanity() {
        let buf = [42u8, 42];
        let c_buf = zstd::stream::encode_all(&buf[..], 1).unwrap();
        let d_buf = zstd::stream::decode_all(c_buf.as_slice()).unwrap();
        assert_eq!(&buf, d_buf.as_slice());
    }
}
