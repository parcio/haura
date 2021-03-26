use super::{
    CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result,
    DEFAULT_BUFFER_SIZE,
};
use crate::{
    buffer::{Buf, BufWrite},
    size::StaticSize,
};
use serde::{Deserialize, Serialize};
use std::{io, mem};

/// No-op compression.
#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub struct None;
pub struct NoneCompression {
    buf: BufWrite,
}
pub struct NoneDecompression;

impl StaticSize for None {
    fn size() -> usize {
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
    fn finish(&mut self) -> Buf {
        mem::replace(&mut self.buf, BufWrite::with_capacity(DEFAULT_BUFFER_SIZE)).into_buf()
    }
}

impl DecompressionState for NoneDecompression {
    fn decompress(&mut self, data: &[u8]) -> Result<Box<[u8]>> {
        // FIXME: pass-through Buf, reusing alloc
        Ok(data.to_vec().into_boxed_slice())
    }
}
