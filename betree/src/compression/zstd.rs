use super::{
    CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result,
    DEFAULT_BUFFER_SIZE,
};
use crate::{
    buffer::{Buf, BufWrite},
    size::StaticSize,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{self, Write},
    mem,
};
use zstd::stream::{
    raw::{CParameter, DParameter, Decoder, Encoder},
    zio::Writer,
};
use zstd_safe::FrameFormat;
use std::sync::{Arc, Mutex};

// TODO: investigate pre-created dictionary payoff

/// Zstd compression. (<https://github.com/facebook/zstd>)
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Zstd {
    /// The compression level which describes the trade-off between
    /// compression ratio and compression speed.
    pub level: u8,
}

struct ZstdCompression {
    writer: Writer<BufWrite, Encoder<'static>>,
}
struct ZstdDecompression {
    writer: Writer<BufWrite, Decoder<'static>>,
}

impl StaticSize for Zstd {
    fn static_size() -> usize {
        1
    }
}

impl CompressionBuilder for Zstd {
    fn new_compression(&self) -> Result<Arc<std::sync::RwLock<dyn CompressionState>>> {
        // "The library supports regular compression levels from 1 up to ZSTD_maxCLevel(),
        // which is currently 22."
        let mut encoder = Encoder::new(self.level as i32)?;

        // Compression format is stored externally, don't need to duplicate it
        encoder.set_parameter(CParameter::Format(FrameFormat::Magicless))?;
        // Integrity is handled at a different layer
        encoder.set_parameter(CParameter::ChecksumFlag(false))?;

        let buf = BufWrite::with_capacity(DEFAULT_BUFFER_SIZE);

        Ok(Arc::new(std::sync::RwLock::new(ZstdCompression {
            writer: Writer::new(buf, encoder),
        })))
    }

    fn decompression_tag(&self) -> DecompressionTag {
        DecompressionTag::Zstd
    }
}

impl Zstd {
    pub fn new_decompression() -> Result<Box<dyn DecompressionState>> {
        let mut decoder = Decoder::new()?;
        decoder.set_parameter(DParameter::Format(FrameFormat::Magicless))?;

        let buf = BufWrite::with_capacity(DEFAULT_BUFFER_SIZE);
        Ok(Box::new(ZstdDecompression {
            writer: Writer::new(buf, decoder),
        }))
    }
}

impl io::Write for ZstdCompression {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.writer.write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl CompressionState for ZstdCompression {
    fn finish(&mut self) -> Buf {
        let _ = self.writer.finish();

        mem::replace(
            self.writer.writer_mut(),
            BufWrite::with_capacity(DEFAULT_BUFFER_SIZE),
        )
        .into_buf()
    }
}

impl DecompressionState for ZstdDecompression {
    fn decompress(&mut self, data: &[u8]) -> Result<Box<[u8]>> {
        self.writer.write_all(data)?;
        self.writer.finish();

        Ok(mem::replace(
            self.writer.writer_mut(),
            BufWrite::with_capacity(DEFAULT_BUFFER_SIZE),
        )
        .into_buf()
        .into_boxed_slice())
    }
}
