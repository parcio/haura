use super::{
    CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result,
    DEFAULT_BUFFER_SIZE,
};
use crate::{
    buffer::{Buf, BufWrite},
    size::StaticSize,
};
use serde::{Deserialize, Serialize};
use std::io;

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
    fn new_compression(&self) -> Result<Box<dyn CompressionState>> {
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
    fn finish(&mut self, buf: Buf) -> Result<Buf> {
        Ok(buf)
    }
}

impl DecompressionState for NoneDecompression {
    fn decompress(&mut self, data: Buf) -> Result<Buf> {
        Ok(data)
    }
}
