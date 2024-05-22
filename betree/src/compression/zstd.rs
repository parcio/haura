use super::{CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result};
use crate::{
    buffer::{Buf, BufWrite},
    size::StaticSize,
    vdev::Block,
};
use serde::{Deserialize, Serialize};
use std::{io::Write, mem};
use zstd::stream::raw::{CParameter, DParameter, Decoder, Encoder};
use zstd_safe::{FrameFormat, WriteBuf};

// TODO: investigate pre-created dictionary payoff

/// Zstd compression. (<https://github.com/facebook/zstd>)
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Zstd {
    /// The compression level which describes the trade-off between
    /// compression ratio and compression speed.
    pub level: u8,
}

struct ZstdCompression {
    writer: Encoder<'static>,
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
    fn new_compression(&self) -> Result<Box<dyn CompressionState>> {
        // "The library supports regular compression levels from 1 up to ZSTD_maxCLevel(),
        // which is currently 22."
        let mut encoder = Encoder::new(self.level as i32)?;

        // Compression format is stored externally, don't need to duplicate it
        encoder.set_parameter(CParameter::Format(FrameFormat::Magicless))?;
        // // Integrity is handled at a different layer
        encoder.set_parameter(CParameter::ChecksumFlag(false))?;

        Ok(Box::new(ZstdCompression { writer: encoder }))
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

use speedy::{Readable, Writable};
const DATA_OFF: usize = mem::size_of::<u32>();

impl CompressionState for ZstdCompression {
    fn finish(&mut self, data: Buf) -> Result<Buf> {
        let size = zstd_safe::compress_bound(data.as_ref().len());
        let mut buf = BufWrite::with_capacity(Block::round_up_from_bytes(size as u32));
        buf.write_all(&[0u8; DATA_OFF])?;

        let mut input = zstd::stream::raw::InBuffer::around(&data);
        let mut output = zstd::stream::raw::OutBuffer::around_pos(&mut buf, DATA_OFF);
        let mut finished_frame;
        loop {
            let remaining = self.writer.run(&mut input, &mut output)?;
            finished_frame = remaining == 0;
            if input.pos() > 0 || data.is_empty() {
                break;
            }
        }

        while self.writer.flush(&mut output)? > 0 {}
        self.writer.finish(&mut output, finished_frame)?;

        let og_len = data.len() as u32;
        og_len
            .write_to_buffer(&mut buf.as_mut()[..DATA_OFF])
            .unwrap();
        Ok(buf.into_buf())
    }
}

impl DecompressionState for ZstdDecompression {
    fn decompress(&mut self, data: Buf) -> Result<Buf> {
        let size = u32::read_from_buffer(data.as_ref()).unwrap();
        let mut buf = BufWrite::with_capacity(Block::round_up_from_bytes(size));

        let mut input = zstd::stream::raw::InBuffer::around(&data[DATA_OFF..]);
        let mut output = zstd::stream::raw::OutBuffer::around(&mut buf);

        let mut finished_frame;
        loop {
            let remaining = self.writer.run(&mut input, &mut output)?;
            finished_frame = remaining == 0;
            if remaining > 0 {
                if output.dst.capacity() == output.dst.as_ref().len() {
                    // append faux byte to extend in case that original was
                    // wrong for some reason (this should not happen but is a
                    // sanity guard)
                    output.dst.write(&[0])?;
                }
                continue;
            }
            if input.pos() > 0 || data.is_empty() {
                break;
            }
        }

        while self.writer.flush(&mut output)? > 0 {}
        self.writer.finish(&mut output, finished_frame)?;

        Ok(buf.into_buf())
    }
}

#[cfg(test)]
mod tests {
    use rand::RngCore;

    use super::*;

    #[test]
    fn encode_then_decode() {
        let mut rng = rand::thread_rng();
        let mut buf = vec![42u8; 4 * 1024 * 1024];
        rng.fill_bytes(buf.as_mut());
        let buf = Buf::from_zero_padded(buf);
        let zstd = Zstd { level: 1 };
        let mut comp = zstd.new_compression().unwrap();
        let c_buf = comp.finish(buf.clone()).unwrap();
        let mut decomp = zstd.decompression_tag().new_decompression().unwrap();
        let d_buf = decomp.decompress(c_buf).unwrap();
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
