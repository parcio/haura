// This module is necessary, because the `arbitrary` crate removed all public interfaces
// from `Gen` which could interlink with the API stability guarantees of `rand`.
//
// As a result, the exposed random value generation APIs are somewhat limited.
// If they ever find a suitable way of re-exposing them, this module will be obsolete.

use quickcheck::{Arbitrary, Gen};
use rand::RngCore;

pub(crate) struct GenRng<'a>(&'a mut Gen);

pub(crate) trait GenExt {
    fn rng(&mut self) -> GenRng;
}

impl GenExt for Gen {
    fn rng(&mut self) -> GenRng {
        GenRng(self)
    }
}

impl<'a> RngCore for GenRng<'a> {
    fn next_u32(&mut self) -> u32 {
        Arbitrary::arbitrary(self.0)
    }

    fn next_u64(&mut self) -> u64 {
        Arbitrary::arbitrary(self.0)
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        for b in dest {
            *b = Arbitrary::arbitrary(self.0);
        }
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        Ok(self.fill_bytes(dest))
    }
}
