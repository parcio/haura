//! This module provides `Size`, `SizeMut`, and `StaticSize`.
//!
//! These traits are used for serializable objects that knows their serialized
//! size when [`bincode`](../../bincode/index.html) is used.

use parking_lot::RwLock;

/// A trait which represents an serializable object that can quickly calculate
/// the size of it's [`bincode`](../../bincode/index.html) representation and
/// the current size occupied in memory.
pub trait Size {
    /// Returns the size (number of bytes) that this object would have
    /// if serialized using [`bincode`](../../bincode/index.html).
    fn size(&self) -> usize;

    /// Return the, possibly recomputed size, of the current state of the
    /// object.
    fn actual_size(&self) -> Option<usize> {
        None
    }

    /// Return and verify the serialized size of the object based on
    /// [Size::size] and [Size::actual_size].
    fn checked_size(&self) -> Result<usize, (usize, usize)> {
        match (self.size(), self.actual_size()) {
            (predicted, Some(actual)) if predicted == actual => Ok(actual),
            (predicted, Some(actual)) => Err((predicted, actual)),
            (predicted, None) => Ok(predicted),
        }
    }

    /// Size in bytes this
    fn cache_size(&self) -> usize {
        self.size()
    }
}

/// A trait which represents an serializable object
/// that can quickly calculate the size of it's
/// [`bincode`](../../bincode/index.html) representation.
pub trait SizeMut {
    /// Returns the size (number of bytes) that this object would have
    /// if serialized using [`bincode`](../../bincode/index.html).
    fn size(&mut self) -> usize;
}

/// A trait which represents an serializable object
/// that knows the size of it's
/// [`bincode`](../../bincode/index.html) representation.
pub trait StaticSize {
    /// Returns the size (number of bytes) that an object would have
    /// if serialized using [`bincode`](../../bincode/index.html).
    fn static_size() -> usize;
}

impl StaticSize for () {
    fn static_size() -> usize {
        0
    }
}

impl<T: Size> SizeMut for T {
    fn size(&mut self) -> usize {
        Size::size(self)
    }
}

impl<T: StaticSize> Size for T {
    fn size(&self) -> usize {
        T::static_size()
    }
}

impl<T: SizeMut> SizeMut for RwLock<T> {
    fn size(&mut self) -> usize {
        self.get_mut().size()
    }
}
