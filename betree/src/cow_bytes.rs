//! This module provides `CowBytes` which is a Copy-on-Write smart pointer
//! similar to `std::borrow::Cow`.

use crate::size::Size;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use stable_deref_trait::StableDeref;
use std::{
    borrow::Borrow,
    cmp,
    ops::{Deref, DerefMut},
    sync::Arc,
};

/// Copy-on-Write smart pointer which supports cheap cloning as it is
/// reference-counted.
#[derive(Hash, Debug, Clone, Eq, Ord, Default)]
pub struct CowBytes {
    // TODO Replace by own implementation
    pub(super) inner: Arc<Vec<u8>>,
}

use rkyv::{Archived, CheckBytes, Fallible};

impl<C: ?Sized> CheckBytes<C> for ArchivedCowBytes {
    type Error = std::io::Error;

    unsafe fn check_bytes<'a>(
        value: *const Self,
        _context: &mut C,
    ) -> Result<&'a Self, Self::Error> {
        // TODO: Remove the requirement for this trait?
        value.as_ref().ok_or(std::io::Error::other("oops"))
    }
}

/// The zero-copy representation of a [CowBytes].
pub struct ArchivedCowBytes {
    len: Archived<usize>,
    offset: isize,
}

impl ArchivedCowBytes {
    #[inline]
    /// Create a readable slice constrained to the semantic length from the
    /// memory reference. This does not copy the values.
    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self).cast::<u8>().offset(self.offset),
                self.len as usize,
            )
        }
    }

    /// Compatibility wrapper around [Self::as_slice].
    pub fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<D: Fallible + ?Sized> rkyv::Deserialize<CowBytes, D> for ArchivedCowBytes {
    #[inline]
    fn deserialize(&self, _deserializer: &mut D) -> Result<CowBytes, <D as Fallible>::Error> {
        Ok(CowBytes::from(self.as_slice()))
    }
}

/// Resolver to archive [CowBytes].
pub struct CowDigger {
    pos: usize,
}

impl rkyv::Archive for CowBytes {
    type Archived = ArchivedCowBytes;
    type Resolver = CowDigger;

    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        (*out).len = rkyv::to_archived!(self.inner.as_slice().len() as u32);
        let offset = rkyv::rel_ptr::signed_offset(pos, resolver.pos).unwrap();
        (*out).offset = offset;
    }
}

impl<S: rkyv::Fallible + rkyv::ser::Serializer + ?Sized> rkyv::Serialize<S> for CowBytes {
    #[inline]
    fn serialize(
        &self,
        serializer: &mut S,
    ) -> Result<Self::Resolver, <S as rkyv::Fallible>::Error> {
        let pos = serializer.pos();
        serializer.write(self.inner.as_slice())?;

        Ok(CowDigger { pos })
    }
}

impl<T: AsRef<[u8]>> PartialEq<T> for CowBytes {
    fn eq(&self, other: &T) -> bool {
        &**self == other.as_ref()
    }
}

impl<T: AsRef<[u8]>> PartialOrd<T> for CowBytes {
    fn partial_cmp(&self, other: &T) -> Option<cmp::Ordering> {
        (**self).partial_cmp(other.as_ref())
    }
}

impl Serialize for CowBytes {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self)
    }
}

impl<'de> Deserialize<'de> for CowBytes {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<CowBytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Visitor};
        use std::fmt;
        struct CowBytesVisitor;

        impl<'de> Visitor<'de> for CowBytesVisitor {
            type Value = CowBytes;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("byte array")
            }

            #[inline]
            fn visit_bytes<E>(self, v: &[u8]) -> Result<CowBytes, E>
            where
                E: Error,
            {
                Ok(CowBytes::from(v))
            }

            #[inline]
            fn visit_str<E>(self, v: &str) -> Result<CowBytes, E>
            where
                E: Error,
            {
                self.visit_bytes(v.as_ref())
            }
        }
        deserializer.deserialize_bytes(CowBytesVisitor)
    }
}

impl Size for CowBytes {
    fn size(&self) -> usize {
        8 + self.inner.len()
    }
}

impl<'a> From<&'a [u8]> for CowBytes {
    fn from(x: &'a [u8]) -> Self {
        CowBytes {
            inner: Arc::new(x.to_vec()),
        }
    }
}

impl From<Box<[u8]>> for CowBytes {
    fn from(x: Box<[u8]>) -> Self {
        CowBytes {
            inner: Arc::new(x.into_vec()),
        }
    }
}

impl From<Vec<u8>> for CowBytes {
    fn from(x: Vec<u8>) -> Self {
        CowBytes { inner: Arc::new(x) }
    }
}

impl Borrow<[u8]> for CowBytes {
    fn borrow(&self) -> &[u8] {
        self
    }
}

impl AsRef<[u8]> for CowBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

unsafe impl StableDeref for CowBytes {}

impl Deref for CowBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for CowBytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut Arc::make_mut(&mut self.inner)[..]
    }
}

impl<'a> IntoIterator for &'a CowBytes {
    type Item = &'a u8;
    type IntoIter = ::std::slice::Iter<'a, u8>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl CowBytes {
    /// Constructs a new, empty `CowBytes`.
    #[inline]
    pub fn new() -> Self {
        CowBytes::default()
    }
    /// Returns the length of the byte buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns whether this buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Create a new, empty `CowBytes` with the given capacity.
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        CowBytes {
            inner: Arc::new(Vec::with_capacity(cap)),
        }
    }

    /// Pushes a byte slice onto the end of the byte buffer.
    #[inline]
    pub fn push_slice(&mut self, v: &[u8]) {
        Arc::make_mut(&mut self.inner).extend_from_slice(v)
    }

    /// Fills the buffer with zeros up to `size`.
    #[inline]
    pub fn fill_zeros_up_to(&mut self, size: usize) {
        if self.len() < size {
            let fill_up = size - self.len();
            let byte = 0;
            self.extend((0..fill_up).map(|_| &byte));
        }
    }

    /// Returns the size (number of bytes) that this object would have
    /// if serialized using `bincode`.
    pub fn size(&self) -> usize {
        8 + self.inner.len()
    }

    /// Returns the underlying data as `Vec<u8>`.
    /// If this object is the only reference to the data,
    /// this functions avoids copying the underlying data.
    pub fn into_vec(self) -> Vec<u8> {
        match Arc::try_unwrap(self.inner) {
            Ok(v) => v,
            Err(this) => Vec::clone(&this),
        }
    }

    /// Returns a `SlicedCowBytes` which points to `self[pos..pos+len]`.
    pub fn slice(self, pos: u32, len: u32) -> SlicedCowBytes {
        SlicedCowBytes::from(self).subslice(pos, len)
    }

    /// Returns a `SlicedCowBytes` which points to `self[pos..]`.
    pub fn slice_from(self, pos: u32) -> SlicedCowBytes {
        let len = self.len() as u32;
        self.slice(pos, len - pos)
    }
}

impl<'a> Extend<&'a u8> for CowBytes {
    fn extend<T: IntoIterator<Item = &'a u8>>(&mut self, iter: T) {
        Arc::make_mut(&mut self.inner).extend(iter)
    }
}

/// Reference-counted pointer which points to a subslice of the referenced data.
#[derive(Debug, Default, Clone)]
pub struct SlicedCowBytes {
    pub(super) data: ByteSource,
    pos: u32,
    len: u32,
}

#[derive(Debug, Clone)]
pub(super) enum ByteSource {
    Cow(CowBytes),
    Raw { ptr: *const u8, len: usize },
}

impl Deref for ByteSource {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            ByteSource::Cow(data) => &data,
            ByteSource::Raw { ptr, len } => unsafe {
                std::slice::from_raw_parts(ptr.clone(), *len)
            },
        }
    }
}

impl Default for ByteSource {
    fn default() -> Self {
        Self::Cow(CowBytes::default())
    }
}

unsafe impl Send for ByteSource {}
unsafe impl Sync for ByteSource {}

impl PartialEq for SlicedCowBytes {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for SlicedCowBytes {}

impl Serialize for SlicedCowBytes {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self)
    }
}

impl<'de> Deserialize<'de> for SlicedCowBytes {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        CowBytes::deserialize(deserializer).map(Self::from)
    }
}

impl Size for SlicedCowBytes {
    fn size(&self) -> usize {
        8 + self.len as usize
    }
}

impl SlicedCowBytes {
    /// Returns a new subslice which points to `self[pos..pos+len]`.
    pub fn subslice(self, pos: u32, len: u32) -> Self {
        let pos = self.pos + pos;
        assert!(pos + len <= self.len);
        SlicedCowBytes {
            data: self.data,
            pos,
            len,
        }
    }

    /// Returns a new subslice which points to `self[pos..]`.
    pub fn slice_from(self, pos: u32) -> Self {
        assert!(pos <= self.len);
        SlicedCowBytes {
            data: self.data,
            pos: self.pos + pos,
            len: self.len - pos,
        }
    }

    pub(crate) fn into_raw(self) -> *const Vec<u8> {
        match self.data {
            ByteSource::Cow(data) => Arc::into_raw(data.inner),
            ByteSource::Raw { ptr, len } => unsafe {
                // FIXME: This copies data currently when the original buffer
                // is from a raw source ot avoid breaking behavior from
                // outside.
                let mut buf = Vec::with_capacity(len);
                (buf.as_mut_ptr() as *mut u8).copy_from(ptr, len);
                &buf
            },
        }
    }

    pub(crate) unsafe fn from_raw(ptr: *const u8, len: usize) -> Self {
        Self {
            data: ByteSource::Raw { ptr, len },
            pos: 0,
            len: len.try_into().expect("Capacity to large."),
        }
    }
}

impl From<CowBytes> for SlicedCowBytes {
    fn from(data: CowBytes) -> Self {
        SlicedCowBytes {
            pos: 0,
            len: data.len() as u32,
            data: ByteSource::Cow(data),
        }
    }
}

impl Deref for SlicedCowBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        let start = self.pos as usize;
        let end = start + self.len as usize;
        &self.data[start..end]
    }
}

#[cfg(test)]
mod tests {
    use super::{Arc, CowBytes};
    use crate::arbitrary::GenExt;
    use quickcheck::{Arbitrary, Gen};
    use rand::{Rng, RngCore};

    impl Arbitrary for CowBytes {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut rng = g.rng();
            let len = rng.gen_range(0..128);
            let mut bytes = vec![0; len];
            rng.fill_bytes(&mut bytes);
            CowBytes {
                inner: Arc::new(bytes),
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            Box::new(self.inner.shrink().map(|inner| CowBytes { inner }))
        }
    }
}
