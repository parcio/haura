#![allow(missing_docs, unused_doc_comments)]
use thiserror::Error;
use crate::{storage_pool::DiskOffset, vdev::Block};


#[derive(Error, Debug)]
pub enum DmlError {
    #[error("The storage pool encountered an error.")]
    VdevError{
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
    RawAllocationError {
        at: DiskOffset,
        size: Block<u32>,
    }
}

// To avoid recursive error types here, define a simple translation from
// database to DmlError.
impl From<crate::database::Error> for DmlError {
    fn from(value: crate::database::Error) -> Self {
        DmlError::HandlerError(format!("{value:?}"))
    }
}
