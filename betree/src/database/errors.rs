#![allow(missing_docs, unused_doc_comments)]

use crate::vdev::Block;
use thiserror::Error;

pub type Result<R> = std::result::Result<R, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Virtual device encountered an error.")]
    VdevError {
        #[from]
        source: crate::vdev::Error,
    },
    #[error("Storage Pool encountered an error.")]
    StoragePoolError {
        #[from]
        source: crate::storage_pool::Error,
    },
    #[error("A tree operation encountered an error. This is likely an internal error. `{source}`")]
    TreeError {
        #[from]
        source: crate::tree::Error,
    },
    #[error("Serializing into the binary format failed. This is an internal error.")]
    BinarySerializationError {
        #[from]
        source: bincode::Error,
    },
    #[error("String based configuration was not valid.")]
    ConfigurationError {
        #[from]
        source: crate::storage_pool::configuration::Error,
    },
    #[error("IO error occurred.")]
    IoError {
        #[from]
        source: std::io::Error,
    },
    #[error("Dmu encountered an error.")]
    DmlError {
        #[from]
        source: crate::data_management::Error,
    },
    #[error("Cannot operate on a closed dataset.")]
    Closed,
    #[error("Superblock corrupted.")]
    InvalidSuperblock,
    #[error("Key does not exist.")]
    DoesNotExist,
    #[error("Dataset name already occupied. Try to `.open()` the dataset instead.")]
    AlreadyExists,
    // TODO: This should anyway not happen, as there are no problems occuring
    // anymore when two instances are opened. Remove?
    #[error("Given dataset is already in use. Try to close another instance first before opening a new one.")]
    InUse,
    #[error("Message surpasses the maximum length. If you cannot shrink your value, use an object store instead.")]
    MessageTooLarge,
    #[error("Could not serialize the given data. This is an internal error. `{source}`")]
    SerializeFailed {
        #[from]
        source: serde_json::Error,
    },
    #[error("Migration is not possible as {1:?} blocks are not available in tier {0}.")]
    MigrationWouldExceedStorage(u8, Block<u64>),
    #[error("Migration is not possible as the given tier does not exist.")]
    MigrationNotPossible,
    #[error("Null bytes are disallowed in keys.")]
    KeyContainsNullByte,
    #[error("{0}")]
    Generic(String),
}
