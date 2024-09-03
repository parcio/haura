/// `XxHash` contains a digest of `xxHash`
/// which is an "extremely fast non-cryptographic hash algorithm"
/// (<https://github.com/Cyan4973/xxHash>)
use super::{Builder, Checksum, ChecksumError, State};
use crate::size::StaticSize;
use serde::{Deserialize, Serialize};
use std::hash::Hasher;

/// A checksum created by `XxHash`.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct XxHash(u64);

impl StaticSize for XxHash {
    fn static_size() -> usize {
        8
    }
}

impl Checksum for XxHash {
    type Builder = XxHashBuilder;

    fn verify_buffer<I: IntoIterator<Item = T>, T: AsRef<[u8]>>(
        &self,
        data: I,
    ) -> Result<(), ChecksumError> {
        if self.0 == 0 {
            return Ok(());
        }
        let mut state = XxHashBuilder.build();
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
        XxHashBuilder
    }
}

/// The corresponding `Builder` for `XxHash`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XxHashBuilder;

impl Builder<XxHash> for XxHashBuilder {
    type State = XxHashState;

    fn build(&self) -> Self::State {
        XxHashState(twox_hash::XxHash::with_seed(0))
    }

    fn empty(&self) -> XxHash {
        XxHash(0)
    }
}

/// The internal state of `XxHash`.
pub struct XxHashState(twox_hash::XxHash);

impl State for XxHashState {
    type Checksum = XxHash;

    fn ingest(&mut self, data: &[u8]) {
        self.0.write(data);
    }

    fn finish(self) -> Self::Checksum {
        XxHash(self.0.finish())
    }
}
