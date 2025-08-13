/// Impl Checksum with GxHash.
use super::{Builder, Checksum, ChecksumError, State};
use crate::size::StaticSize;
use gxhash::GxHasher;
use serde::{Deserialize, Serialize};
use std::hash::Hasher;

/// A checksum created by `GxHash`.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct GxHash(u64);

impl StaticSize for GxHash {
    fn static_size() -> usize {
        8
    }
}

impl Checksum for GxHash {
    type Builder = GxHashBuilder;

    fn verify_buffer<I: IntoIterator<Item = T>, T: AsRef<[u8]>>(
        &self,
        data: I,
    ) -> Result<(), ChecksumError> {
        if self.0 == 0 {
            return Ok(());
        }
        let mut state = GxHashBuilder.build();
        for x in data {
            state.ingest(x.as_ref());
        }
        let other = state.finish();
        if *self == other {
            Ok(())
        } else {
            Err(ChecksumError)
        }
    }

    fn builder() -> Self::Builder {
        GxHashBuilder
    }
}

/// The corresponding `Builder` for `GxHash`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GxHashBuilder;

impl Builder<GxHash> for GxHashBuilder {
    type State = GxHashState;

    fn build(&self) -> Self::State {
        // Due to security concerns the default `GxHasher` is randomized, which
        // does not work for us, therefore, use pinned seed.
        GxHashState(GxHasher::with_seed(0))
    }

    fn empty(&self) -> GxHash {
        GxHash(0)
    }
}

/// The internal state of `GxHash`.
pub struct GxHashState(GxHasher);

impl State for GxHashState {
    type Checksum = GxHash;

    fn ingest(&mut self, data: &[u8]) {
        self.0.write(data);
    }

    fn finish(self) -> Self::Checksum {
        GxHash(self.0.finish())
    }
}
