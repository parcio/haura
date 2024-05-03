#![allow(missing_docs, unused_doc_comments)]
use crate::{storage_pool::DiskOffset, vdev::Block};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("VDev failed: {source}")]
    VdevError {
        #[from]
        source: crate::vdev::Error,
    },
    #[error("Compression failed: {source}")]
    CompressionError {
        #[from]
        source: crate::compression::Error,
    },
    #[error("Decompressing serialized data failed.")]
    DecompressionError,
    #[error("Deserialization failed.")]
    DeserializationError,
    #[error("Serialization failed.")]
    SerializationError,
    #[error("The allocation handler encountered an error.")]
    HandlerError(String),
    #[error("Io failed: {source}")]
    IoError {
        #[from]
        source: std::io::Error,
    },
    #[error("Could not find fitting space to allocate data.")]
    OutOfSpaceError,
    #[error("A callback function to the cache has errored.")]
    CallbackError,
    #[error("A raw allocation of size {size} as {at} has failed.")]
    RawAllocationError { at: DiskOffset, size: Block<u32> },
}

// To avoid recursive error types here, define a simple translation from
// database to Error.
impl From<crate::database::Error> for Error {
    fn from(value: crate::database::Error) -> Self {
        Error::HandlerError(format!("{value:?}"))
    }
}
