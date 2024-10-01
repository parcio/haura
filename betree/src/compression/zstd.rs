use super::{
    CompressionBuilder, CompressionState, DecompressionState, DecompressionTag, Result,
    DEFAULT_BUFFER_SIZE,
};
use crate::{
    buffer::{Buf, BufWrite},
    database,
    size::StaticSize,
    vdev::Block,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{self, Cursor, Write},
    mem,
};
use zstd::{
    block::{Compressor, Decompressor},
    stream::{
        raw::{CParameter, DParameter, Decoder, Encoder},
        zio::{Reader, Writer},
    },
};
use zstd_safe::{FrameFormat, InBuffer, OutBuffer, WriteBuf};
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
    fn new_compression(&self) -> Result<Arc<std::sync::RwLock<dyn CompressionState>>> {
        // "The library supports regular compression levels from 1 up to ZSTD_maxCLevel(),
        // which is currently 22."
        let mut encoder = Encoder::new(self.level as i32)?;

        // Compression format is stored externally, don't need to duplicate it
        encoder.set_parameter(CParameter::Format(FrameFormat::Magicless))?;
        // // Integrity is handled at a different layer
        encoder.set_parameter(CParameter::ChecksumFlag(false))?;

        Ok(Arc::new(std::sync::RwLock::new(ZstdCompression { writer: encoder })))
    }

    fn decompression_tag(&self) -> DecompressionTag {
        DecompressionTag::Zstd
    }
}

impl Zstd {
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
    fn finishext2(&mut self, data: &[u8]) -> Result<Buf>
    {
        let size = zstd_safe::compress_bound(data.len());
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

        /*let og_len = data.len() as u32;
        og_len
            .write_to_buffer(&mut buf.as_mut()[..DATA_OFF])
            .unwrap();
*/
        let mut buf2 = BufWrite::with_capacity(Block::round_up_from_bytes(output.as_slice().len() as u32));
        buf2.write_all(output.as_slice());

//<<<<<<< HEAD
//        Ok(buf2.as_slice().to_vec())
//=======
        Ok(buf.into_buf())
//>>>>>>> ca604af8439c223604ef4577063059234f01173a
    }

    fn finishext(&mut self, data: &[u8]) -> Result<Vec<u8>>
    {
        let size = zstd_safe::compress_bound(data.len());
        let mut buf = BufWrite::with_capacity(Block::round_up_from_bytes(size as u32));
        //buf.write_all(&[0u8; DATA_OFF])?;

        let mut input = zstd::stream::raw::InBuffer::around(&data);
        let mut output = zstd::stream::raw::OutBuffer::around_pos(&mut buf, 0 /*DATA_OFF*/);
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

        /*
        let og_len = data.len() as u32;
        og_len
            .write_to_buffer(&mut buf.as_mut()[..DATA_OFF])
            .unwrap();
//<<<<<<< HEAD
        let duration = start.elapsed();
        //println!("Total time elapsed: {:?}", duration);
        //println!("Total time elapsed: {} {}", size, buf.get_len());
        Ok(buf.into_buf())
        */

//        let mut buf2 = BufWrite::with_capacity(Block::round_up_from_bytes(output.as_slice().len() as u32));
//        buf2.write_all(output.as_slice());
//        Ok(buf2.into_buf())
//=======

        Ok(buf.as_slice().to_vec())
    }

    fn finish(&mut self, data: Buf) -> Result<Buf> {
        //panic!("..");
        let start = Instant::now();
        let size = zstd_safe::compress_bound(data.as_ref().len());
        let mut buf = BufWrite::with_capacity(Block::round_up_from_bytes(size as u32));
        //buf.write_all(&[0u8; DATA_OFF])?;

        let mut input = zstd::stream::raw::InBuffer::around(&data);
        let mut output = zstd::stream::raw::OutBuffer::around_pos(&mut buf, 0);
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

        // let og_len = data.len() as u32;
        // og_len
        //     .write_to_buffer(&mut buf.as_mut()[..DATA_OFF])
        //     .unwrap();
        // let duration = start.elapsed();
        // let b =  buf.get_len();
        let mut buf2 = BufWrite::with_capacity(Block::round_up_from_bytes(output.as_slice().len() as u32));
        buf2.write_all(output.as_slice());

        let a = output.as_slice().len();
        let b = buf2.into_buf();
        let c = buf.into_buf();
        //println!("== {:?}", data.as_ref());
        //println!("== {:?}", b.as_ref());
        //println!("compressed....: {} {} {} {}", size, data.as_ref().len(), b.as_ref().len(), c.as_ref().len());
        Ok(b)
//>>>>>>> ca604af8439c223604ef4577063059234f01173a
    }
}


impl DecompressionState for ZstdDecompression {
    fn decompressext(&mut self, data: &[u8]) -> Result<Vec<u8>>
    {
        //panic!("shukro maula");
        let size = data.len() as u32;
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

        Ok(buf.as_slice().to_vec())
    }

    fn decompress(&mut self, data: Buf) -> Result<Buf> {
        //let start = Instant::now();
        //panic!("..why");

        let size = data.as_ref().len() as u32;
        let mut buf = BufWrite::with_capacity(Block::round_up_from_bytes(size));

//<<<<<<< HEAD
        let mut input = zstd::stream::raw::InBuffer::around(&data[/*DATA_OFF*/..]);
//=======
//        let mut input = zstd::stream::raw::InBuffer::around(&data[..]);
//>>>>>>> ca604af8439c223604ef4577063059234f01173a
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
        //let duration = start.elapsed();
        //println!("Total time elapsed: {:?}", duration);
        println!("Total time elapsed: {} {}", size, buf.get_len());
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
