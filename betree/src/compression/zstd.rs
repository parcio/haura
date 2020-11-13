use super::Compression;
use crate::size::StaticSize;
use serde::{Deserialize, Serialize};
use std::io::{self, Read};
use zstd::stream::{
    raw::{CParameter, DParameter, Decoder, Encoder},
    zio::{Reader, Writer},
};
use zstd_safe::FrameFormat;

// TODO: investigate pre-created dictionary payoff

/// Zstd compression. (<https://github.com/facebook/zstd>)
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Zstd {
    /// The compression level which describes the trade-off between
    /// compression ratio and compression speed.
    pub level: u8,
}

impl StaticSize for Zstd {
    fn size() -> usize {
        1
    }
}

impl Compression for Zstd {
    type Compress = Compress;

    fn decompress(&self, buffer: Box<[u8]>) -> io::Result<Box<[u8]>> {
        let mut decoder = Decoder::new().expect("zstd init failed"); // FIXME: API currently prevents handling this better
        decoder.set_parameter(DParameter::Format(FrameFormat::Magicless));

        let mut reader = Reader::new(&buffer[..], decoder);
        reader.set_single_frame();

        let mut output = Vec::with_capacity(buffer.len());
        reader.read_to_end(&mut output)?;
        Ok(output.into_boxed_slice())
    }

    // TODO: verify this is only called <= n_threads times
    fn compress(&self) -> Self::Compress {
        // "The library supports regular compression levels from 1 up to ZSTD_maxCLevel(),
        // which is currently 22."
        let mut encoder = Encoder::new(self.level as i32).expect("zstd init failed"); // FIXME: API currently prevents handling this better

        // Compression format is stored elsewhere, don't need to duplicate it
        encoder.set_parameter(CParameter::Format(FrameFormat::Magicless));
        // Integrity is handled at a different layer
        encoder.set_parameter(CParameter::ChecksumFlag(false));

        let writer = Writer::new(Vec::new(), encoder);

        Compress { writer }
    }
}

pub struct Compress {
    writer: Writer<Vec<u8>, Encoder>,
}

impl io::Write for Compress {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl super::Compress for Compress {
    fn finish(mut self) -> Box<[u8]> {
        self.writer.finish();
        let (result, _) = self.writer.into_inner();
        result.into_boxed_slice()
    }
}
