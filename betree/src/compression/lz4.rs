use super::{ CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, DEFAULT_BUFFER_SIZE, Result };
use crate::size::StaticSize;
use crate::buffer::{Buf, BufWrite};

use crate::{
    vdev::Block,
};
use std::io::Write;

use serde::{Deserialize, Serialize};
use zstd_safe::WriteBuf;
use std::io::{self, BufReader, Read};

use std::{
    mem,
};
use std::sync::{Arc, Mutex};

use lz4::{Encoder, Decoder, EncoderBuilder, ContentChecksum, BlockSize, BlockMode};

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
    config: Lz4,
    encoder: Encoder<BufWrite>,
}

pub struct Lz4Decompression;

impl StaticSize for Lz4 {
    fn static_size() -> usize {
        1
    }
}

impl CompressionBuilder for Lz4 {
    fn new_compression(&self) -> Result<Arc<std::sync::RwLock<dyn CompressionState>>> {
        let mut encoder = EncoderBuilder::new()
            .level(u32::from(self.level))
            .checksum(ContentChecksum::NoChecksum)
            .block_size(BlockSize::Max4MB)
            .block_mode(BlockMode::Linked)
            .build(BufWrite::with_capacity(DEFAULT_BUFFER_SIZE))?;

        Ok(Arc::new(std::sync::RwLock::new(Lz4Compression { config: self.clone(), encoder })))

    }

    fn decompression_tag(&self) -> DecompressionTag {
        DecompressionTag::Lz4
    }
}

// impl CompressionConfiguration for Lz4 {
//     fn new_compression(&self) -> Result<Box<dyn CompressionState>> {
//         let encoder = EncoderBuilder::new()
//             .level(u32::from(self.level))
//             .checksum(ContentChecksum::NoChecksum)
//             .block_size(BlockSize::Max4MB)
//             .block_mode(BlockMode::Linked)
//             .build(BufWrite::with_capacity(DEFAULT_BUFFER_SIZE))?;

//         Ok(Box::new(Lz4Compression { config: self.clone(), encoder }))
//     }

//     fn decompression_tag(&self) -> DecompressionTag { DecompressionTag::Lz4 }
// }

impl Lz4 {
    pub fn new_decompression() -> Result<Box<dyn DecompressionState>> {
        //let mut decoder = Decoder::new(BufReader::with_capacity(DEFAULT_BUFFER_SIZE))?;
        //decoder.set_parameter(DParameter::Format(FrameFormat::Magicless))?;

        Ok(Box::new(Lz4Decompression))
    }
    // pub fn new_decompression() -> Result<Box<dyn DecompressionState>> {
    //     Ok(Box::new(Lz4Decompression))
    // }
}

impl io::Write for Lz4Compression {
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

impl CompressionState for Lz4Compression {
    fn finishext(&mut self, data: &[u8]) -> Result<Vec<u8>>
    {
        let size = data.len();
        let mut buf = BufWrite::with_capacity(Block::round_up_from_bytes(size as u32));

        let mut encoder = EncoderBuilder::new()
        .level(u32::from(self.config.level))
        .checksum(ContentChecksum::NoChecksum)
        .block_size(BlockSize::Max4MB)
        .block_mode(BlockMode::Linked)
        .build(buf)?;

        encoder.write_all(data.as_ref())?;
        let (compressed_data, result) = encoder.finish();
    
        if let Err(e) = result {
            panic!("Compression failed: {:?}", e);
        }

        let mut buf2 = BufWrite::with_capacity(Block::round_up_from_bytes(compressed_data.as_slice().len() as u32));
        buf2.write_all(compressed_data.as_slice());

//panic!("..");
        Ok(buf2.as_slice().to_vec())
    }

    fn finish(&mut self, data: Buf) -> Result<Buf> {
        let size = data.as_ref().len();
        let mut buf = BufWrite::with_capacity(Block::round_up_from_bytes(size as u32));

        let mut encoder = EncoderBuilder::new()
        .level(u32::from(self.config.level))
        .checksum(ContentChecksum::NoChecksum)
        .block_size(BlockSize::Max4MB)
        .block_mode(BlockMode::Linked)
        .build(buf)?;

        encoder.write_all(data.as_ref())?;
        let (compressed_data, result) = encoder.finish();
        
        if let Err(e) = result {
            panic!("Compression failed: {:?}", e);
        }

        let mut buf2 = BufWrite::with_capacity(Block::round_up_from_bytes(compressed_data.as_slice().len() as u32));
        buf2.write_all(compressed_data.as_slice());

        Ok(buf2.into_buf())

        //Ok(compressed_data.into_buf())
    }
    // fn finish(&mut self) -> Buf {
    //     let (v, result) = self.encoder.finish();
    //     result.unwrap();
    //     v.into_buf()
    // }
}


impl DecompressionState for Lz4Decompression {
    fn decompressext(&mut self, data: &[u8]) -> Result<Vec<u8>>
    {
        //panic!("..");
        let size = data.as_ref().len() as u32;
        let mut buf = BufWrite::with_capacity(Block::round_up_from_bytes(size));
        let mut decoder = Decoder::new(data.as_ref())?;

        io::copy(&mut decoder, &mut buf)?;
        Ok(buf.as_slice().to_vec())
    }

    fn decompress(&mut self, data: Buf) -> Result<Buf> {
        let size = data.as_ref().len() as u32;
        let mut buf = BufWrite::with_capacity(Block::round_up_from_bytes(size));
        let mut decoder = Decoder::new(data.as_ref())?;

        io::copy(&mut decoder, &mut buf)?;
        Ok(buf.into_buf())
    }
    // fn decompress(&mut self, data: &[u8]) -> Result<Box<[u8]>> {
    //     let mut output = Vec::with_capacity(DEFAULT_BUFFER_SIZE.to_bytes() as usize);
    //     Decoder::new(&data[..])?.read_to_end(&mut output)?;
    //     Ok(output.into_boxed_slice())
    // }
}
