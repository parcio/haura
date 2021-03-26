#![allow(missing_docs, unused_doc_comments)]

use std::sync::Arc;

#[derive(thiserror::Error, Debug, Clone)]
pub enum VdevError {
    #[error("Io: {0}")]
    Io(Arc<std::io::Error>),
    #[error("Checksum: {0}")]
    Checksum(Arc<crate::checksum::ChecksumError>),
    #[error("Read: id={0}")]
    Read(String),
    #[error("Write: id={0}")]
    Write(String),
    #[error("Spawn: {0}")]
    Spawn(Arc<futures::task::SpawnError>),
}

impl From<std::io::Error> for VdevError {
    fn from(io_err: std::io::Error) -> Self {
        VdevError::Io(Arc::new(io_err))
    }
}

impl From<crate::checksum::ChecksumError> for VdevError {
    fn from(checksum_err: crate::checksum::ChecksumError) -> Self {
        VdevError::Checksum(Arc::new(checksum_err))
    }
}

impl From<futures::task::SpawnError> for VdevError {
    fn from(spawn_err: futures::task::SpawnError) -> Self {
        VdevError::Spawn(Arc::new(spawn_err))
    }
}
