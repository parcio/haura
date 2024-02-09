//! This is a small wrapper around the std BufReader,
//! which uses seek_relative when possible.
//! This prevents unnecessary buffer discards,
//! and massively improves performance of the initial parsing
//! of zip archive metadata.
use std::io::{self, BufReader, Read, Seek, SeekFrom};

pub struct BufReaderSeek<R> {
    b: BufReader<R>,
}

impl<R: Read> BufReaderSeek<R> {
    pub fn new(r: R) -> Self {
        Self {
            b: BufReader::new(r),
        }
    }
}

impl<R: Read> Read for BufReaderSeek<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.b.read(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.b.read_exact(buf)
    }
}

impl<R: Seek> Seek for BufReaderSeek<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        if let SeekFrom::Current(offset) = pos {
            self.b.seek_relative(offset)?;
            self.b.stream_position()
        } else {
            self.b.seek(pos)
        }
    }
}
