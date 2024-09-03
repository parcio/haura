//! This module provides a `Checksum` trait and implementors for verifying data
//! integrity.

use crate::size::Size;
use serde::{de::DeserializeOwned, Serialize};
use std::{error::Error, fmt, iter::once};

mod fxhash;
mod gxhash;
mod xxhash;

pub use self::gxhash::{GxHash, GxHashBuilder};
pub use fxhash::{FxHash, FxHashBuilder};
pub use xxhash::{XxHash, XxHashBuilder};

/// A checksum to verify data integrity.
pub trait Checksum:
    Serialize + DeserializeOwned + Size + Clone + Send + Sync + fmt::Debug + 'static
{
    /// Builds a new `Checksum`.
    type Builder: Builder<Self>;

    /// Verifies the contents of the given buffer which consists of multiple
    /// `u8` slices.
    fn verify_buffer<I: IntoIterator<Item = T>, T: AsRef<[u8]>>(
        &self,
        data: I,
    ) -> Result<(), ChecksumError>;

    /// Verifies the contents of the given buffer.
    fn verify(&self, data: &[u8]) -> Result<(), ChecksumError> {
        self.verify_buffer(once(data))
    }

    /// Create a valid empty builder for this checksum type.
    fn builder() -> Self::Builder;
}

/// A checksum builder
pub trait Builder<C: Checksum>:
    Serialize + DeserializeOwned + Clone + Send + Sync + fmt::Debug + 'static
{
    /// The internal state of the checksum.
    type State: State<Checksum = C>;

    /// Create a new state to build a checksum.
    fn build(&self) -> Self::State;

    /// Return an empty Checksum. This variant skips the verificiation steps
    /// when applied to a new buffer.
    fn empty(&self) -> C;
}

/// Holds a state for building a new `Checksum`.
pub trait State {
    /// The resulting `Checksum`.
    type Checksum: Checksum;

    /// Ingests the given data into the state.
    fn ingest(&mut self, data: &[u8]);

    /// Builds the actual `Checksum`.
    fn finish(self) -> Self::Checksum;
}

/// This is the error that will be returned when a `Checksum` does not match.
#[derive(Debug)]
pub struct ChecksumError;

impl fmt::Display for ChecksumError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Failed to verify the integrity")
    }
}

impl Error for ChecksumError {
    fn description(&self) -> &str {
        "a checksum error occurred"
    }
}
