use super::{ CompressionConfiguration, CompressionState, DecompressionState, DecompressionTag, DEFAULT_BUFFER_SIZE, Result };
use crate::size::StaticSize;
use crate::buffer::{Buf, BufWrite};

use serde::{Deserialize, Serialize};
use std::io::{self, Read};

// use lz4_sys::{ Lz4

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
    fn size() -> usize {
        1
    }
}

impl CompressionConfiguration for Lz4 {
    fn new_compression(&self) -> Result<Box<dyn CompressionState>> {
        let encoder = EncoderBuilder::new()
            .level(u32::from(self.level))
            .checksum(ContentChecksum::NoChecksum)
            .block_size(BlockSize::Max4MB)
            .block_mode(BlockMode::Linked)
            .build(BufWrite::with_capacity(DEFAULT_BUFFER_SIZE))?;

        Ok(Box::new(Lz4Compression { config: self.clone(), encoder }))
    }

    fn decompression_tag(&self) -> DecompressionTag { DecompressionTag::Lz4 }
}

impl Lz4 {
    pub fn new_decompression() -> Result<Box<dyn DecompressionState>> {
        Ok(Box::new(Lz4Decompression))
    }
}

impl io::Write for Lz4Compression {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.encoder.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.encoder.write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.encoder.flush()
    }
}

impl CompressionState for Lz4Compression {
    fn finish(&mut self) -> Buf {
        let (v, result) = self.encoder.finish();
        result.unwrap();
        v.into_buf()
    }
}

impl DecompressionState for Lz4Decompression {
    fn decompress(&mut self, data: &[u8]) -> Result<Box<[u8]>> {
        let mut output = Vec::with_capacity(DEFAULT_BUFFER_SIZE.to_bytes() as usize);
        Decoder::new(&data[..])?.read_to_end(&mut output)?;
        Ok(output.into_boxed_slice())
    }
}
