#![allow(missing_docs, unused_doc_comments)]
use crate::{storage_pool::DiskOffset, vdev::Block};
#[cfg(feature = "nvm")]
use pmem_hashmap::PMapError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("The storage pool encountered an error. `{source}`")]
    VdevError {
        #[from]
        source: crate::vdev::Error,
    },
    #[error("The chosen compression type encountered an error.")]
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
    #[error("Input/Output procedure encountered an error.")]
    IoError {
        #[from]
        source: std::io::Error,
    },
    #[error("Could not find fitting space to allocate data.")]
    OutOfSpaceError,
    #[error("A callback function to the cache has errored.")]
    CallbackError,
    #[error("A raw allocation has failed.")]
    RawAllocationError { at: DiskOffset, size: Block<u32> },
    #[cfg(feature = "nvm")]
    #[error("A error occured while accessing the persistent cache. `{source}`")]
    PersistentCacheError {
        #[from]
        source: PMapError,
    },
}

// To avoid recursive error types here, define a simple translation from
// database to Error.
impl From<crate::database::Error> for Error {
    fn from(value: crate::database::Error) -> Self {
        Error::HandlerError(format!("{value:?}"))
    }
}
