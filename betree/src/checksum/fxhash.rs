/// Impl Checksum with FxHash.
use super::{Builder, Checksum, ChecksumError, State};
use crate::size::StaticSize;
use rustc_hash::FxHasher;
use serde::{Deserialize, Serialize};
use std::hash::Hasher;

/// The rustc own hash impl originally from Firefox.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct FxHash(u64);

impl StaticSize for FxHash {
    fn static_size() -> usize {
        8
    }
}

impl Checksum for FxHash {
    type Builder = FxHashBuilder;

    fn verify_buffer<I: IntoIterator<Item = T>, T: AsRef<[u8]>>(
        &self,
        data: I,
    ) -> Result<(), ChecksumError> {
        if self.0 == 0 {
            return Ok(());
        }
        let mut state = FxHashBuilder.build();
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
        FxHashBuilder
    }
}

/// The corresponding `Builder` for `FxHash`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FxHashBuilder;

impl Builder<FxHash> for FxHashBuilder {
    type State = FxHashState;

    fn build(&self) -> Self::State {
        FxHashState(FxHasher::default())
    }

    fn empty(&self) -> FxHash {
        FxHash(0)
    }
}

/// The internal state of `FxHash`.
pub struct FxHashState(FxHasher);

impl State for FxHashState {
    type Checksum = FxHash;

    fn ingest(&mut self, data: &[u8]) {
        self.0.write(data);
    }

    fn finish(self) -> Self::Checksum {
        FxHash(self.0.finish())
    }
}
