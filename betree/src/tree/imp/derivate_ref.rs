//! Implementation of derivative and original structure container to ensure lifetime
//! guarantees.
use stable_deref_trait::StableDeref;
use std::{
    mem::transmute,
    ops::{Deref, DerefMut},
};

use crate::cache::AddSize;

use super::internal::take_child_buffer::TakeChildBufferWrapper;

/// A reference allowing for a derivative of the original structure to be stored
/// alongside the original. Helpful if a derivative of the original is dependent
/// on its lifetime.
///
/// This structures differs from somthing like an owning reference as that we
/// are not dependent on actual references when considering the reference or
/// derivative of a type. For example when we perform an operation one value o
/// (owner) to get some value d (derivative) which is it's own independent type
/// with references to o we cannot store this with a simple map in owning ref.
///
/// ```rust,ignore
/// // Does not compile 😿
/// let owning_ref = OwningRef::new(o).map(|o| &o.some_transition());
///                                         // ^-- we can't a reference from a temporary value
/// // Does compile 😸
/// let derivate_ref = DerivateRefNVM::try_new(o, |o| o.some_transition())
/// ```
pub struct DerivateRefNVM<T, U> {
    inner: U,
    owner: T,
}

impl<T: StableDeref + DerefMut, U> DerivateRefNVM<T, TakeChildBufferWrapper<'static, U>> {
    /// Unsafe conversions of a limited life-time reference in [TakeChildBuffer]
    /// to a static one. This is only ever safe in the internal context of [DerivateRefNVM].
    pub fn try_new<F>(mut owner: T, f: F) -> Result<Self, T>
    where
        F: for<'a> FnOnce(&'a mut T::Target) -> Option<TakeChildBufferWrapper<'a, U>>,
    {
        match unsafe { transmute(f(&mut owner)) } {
            None => Err(owner),
            Some(inner) => Ok(DerivateRefNVM { owner, inner }),
        }
    }

    pub fn into_owner(self) -> T {
        self.owner
    }
}

impl<T: AddSize, U> AddSize for DerivateRefNVM<T, U> {
    fn add_size(&self, size_delta: isize) {
        self.owner.add_size(size_delta);
    }
}

impl<T, U> Deref for DerivateRefNVM<T, U> {
    type Target = U;
    fn deref(&self) -> &U {
        &self.inner
    }
}

impl<T, U> DerefMut for DerivateRefNVM<T, U> {
    fn deref_mut(&mut self) -> &mut U {
        &mut self.inner
    }
}
