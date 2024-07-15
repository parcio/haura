//! This module provides `CowBytes` which is a Copy-on-Write smart pointer
//! similar to `std::borrow::Cow`.

use crate::{compression::DecompressionTag, size::Size};
//use serde::{Deserialize, Deserializer, Serialize, Serializer};
use stable_deref_trait::StableDeref;
use zstd_safe::WriteBuf;
use std::{
    borrow::Borrow,
    cmp,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use crate::{
    compression::CompressionConfiguration,
    compression::CompressionBuilder,
    compression::Zstd,
};

use rkyv::{
    Archive, Deserialize, Serialize,
    ser::{Serializer, serializers::AllocSerializer},
    archived_root,
    ser::{serializers::AlignedSerializer, ScratchSpace},
    vec::{ArchivedVec, VecResolver},
    out_field,
    AlignedVec,
    Archived,
    ArchiveUnsized,
    MetadataResolver,
    RelPtr,
    SerializeUnsized,
    Fallible,
};

use std::marker::PhantomData;

struct OwnedStr2<T> {
    inner: Arc<Vec<u8>>,
    _marker: PhantomData<T>
}

struct ArchivedOwnedStr2 {
    // This will be a relative pointer to our string
    inner: ArchivedVec<u8>,
}

impl ArchivedOwnedStr2 {
    // This will help us get the bytes of our type as a str again.
    // fn as_slice(&self) -> &[u8] {

    //     unsafe {

    //         // The as_ptr() function of RelPtr will get a pointer to the str

    //         &*self.ptr.as_ptr()

    //     }

    // }
}

struct OwnedStr2Resolver {
    // This will be the position that the bytes of our string are stored at.
    // We'll use this to resolve the relative pointer of our
    // ArchivedOwnedStr2.
    pos: usize,
    // The archived metadata for our str may also need a resolver.
    metadata_resolver: VecResolver,
}

// The Archive implementation defines the archived version of our type and
// determines how to turn the resolver into the archived form. The Serialize
// implementations determine how to make a resolver from the original value.
impl<T> Archive for OwnedStr2<T> {
    type Archived = ArchivedVec<u8>;
    // This is the resolver we can create our Archived version from.
    type Resolver = VecResolver;

    // The resolve function consumes the resolver and produces the archived

    // value at the given position.

    unsafe fn resolve(
        &self,
        pos: usize,
        resolver: Self::Resolver,
        out: *mut Self::Archived,
    ) {
        //println!("resolver.pos={}, pos={}", resolver.pos, pos);
        ArchivedVec::resolve_from_len(self.inner.len(), pos, resolver, out);
    }
}

// We restrict our serializer types with Serializer because we need its
// capabilities to archive our type. For other types, we might need more or
// less restrictive bounds on the type of S.
impl<T, S: Serializer + ?Sized + ScratchSpace> Serialize<S> for OwnedStr2<T> {
    fn serialize(
        &self,
        serializer: &mut S
    ) -> Result<Self::Resolver, S::Error> {
        let mut serialized_data: Vec<u8> = Vec::new();

        // bincode::serialize_into(&mut serialized_data, &self.inner)
        // .map_err(|e| {
        //     //debug!("Failed to serialize ObjectPointer.");
        //     std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        // });
        // // This is where we want to write the bytes of our string and return
        // // a resolver that knows where those bytes were written.
        // // We also need to serialize the metadata for our str.
        // println!("-->self.inner.serialize_unsized(serializer)={:?}", self.inner.serialize_unsized(serializer)?);

        // Ok(VecResolver::new())
        // // Ok(VecResolver {
        // //     pos: self.inner.serialize_unsized(serializer)?,
        // //     //pos: serialized_data.len(),
        // //     //metadata_resolver: ArchivedVec::serialize_from_slice(&mut self.inner.serialize_metadata(serializer))?
        // //     //metadata_resolver: ArchivedVec::serialize_from_slice(serialized_data.as_slice(), serializer)?,
        // // })
        ArchivedVec::serialize_from_slice(&self.inner, serializer)
    }
}

// impl<D: Fallible + ?Sized> DeserializeWith<Archived<Vec<u8>>, OwnedStr2, D> for OwnedStr2 {
//     fn deserialize_with(field: &Archived<Vec<u8>>, deserializer: &mut D) -> Result<Self, D::Error> {
//         panic!("Failed to deserialize childbuffer's node_pointer");
//     }
// }
// impl<D: Fallible + ?Sized> Deserialize<Arc<Vec<u8>>, D> for OwnedStr2 {
//     fn deserialize(&self, deserializer: &mut D) -> Result<Arc<Vec<u8>>, D::Error> {
        
//             panic!("Failed to deserialize childbuffer's node_pointer");
//     }
// }

impl<T, D: Fallible + ?Sized> Deserialize<OwnedStr2<T>, D> for Archived<Vec<u8>> {
    fn deserialize(&self, deserializer: &mut D) -> Result<OwnedStr2<T>, D::Error> {
        let vec: Vec<u8> = self.deserialize(deserializer)?;

        // Create an Arc from the Vec<u8>
        let arc_vec = Arc::new(vec);

        // Create the OwnedStr2
        Ok(OwnedStr2 { inner: arc_vec, _marker: PhantomData })
    }
}

/// Copy-on-Write smart pointer which supports cheap cloning as it is
/// reference-counted.
#[derive(Hash, Debug, Clone, Eq, Ord, Default)]//, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
//#[archive(check_bytes)]
pub struct CowBytes2 {
    // TODO Replace by own implementation
    pub(super) inner: Arc<Vec<u8>>,
}

struct ArchivedCowBytes2 {
    // This will be a relative pointer to our string
    inner: ArchivedVec<u8>,
}

impl Archive for CowBytes2 {
    type Archived = ArchivedVec<u8>;
    type Resolver = VecResolver;

    unsafe fn resolve(
        &self,
        pos: usize,
        resolver: Self::Resolver,
        out: *mut Self::Archived,
    ) {
        ArchivedVec::resolve_from_len(self.inner.len(), pos, resolver, out);
    }
}

impl<S: Serializer + ?Sized + ScratchSpace> Serialize<S> for CowBytes2 {
    fn serialize(
        &self,
        serializer: &mut S
    ) -> Result<Self::Resolver, S::Error> {
        //panic!("----------------------");  
        let compression = CompressionConfiguration::None;
        /*let compression = CompressionConfiguration::Zstd(Zstd {
            level: 1,
        });*/
        let default_compression = compression.to_builder();

        let compression = &*default_compression.read().unwrap();
        //let compressed_data = [0u8, 10];
        //panic!("<>");
        let compressed_data = {
            // FIXME: cache this
            let a = compression.new_compression().unwrap();
            let mut state = a.write().unwrap();
            {
                state.write_all(self.inner.as_slice());
            }
            state.finish()
        };

        ArchivedVec::serialize_from_slice(compressed_data.as_slice(), serializer)
        
        //ArchivedVec::serialize_from_slice(&self.inner, serializer)
    }
}

impl<D: Fallible + ?Sized> Deserialize<CowBytes2, D> for ArchivedVec<u8> {
    fn deserialize(&self, deserializer: &mut D) -> Result<CowBytes2, D::Error> {
//panic!("----------------------");
        let vec: Vec<u8> = self.deserialize(deserializer)?;
        
        let d = DecompressionTag::None;
        let mut decompression_state = d.new_decompression();

        let data = decompression_state.unwrap().decompress(vec.as_slice()).unwrap();
        let arc_vec = Arc::new(data.to_vec());

        Ok(CowBytes2 { inner: arc_vec })
        
        
        /*let arc_vec = Arc::new(vec);
        Ok(CowBytes2 { inner: arc_vec })*/
        
    }
}

impl AsRef<[u8]> for ArchivedCowBytes2 {
    fn as_ref(&self) -> &[u8] {
        &self.inner
    }
}

impl<T: AsRef<[u8]>> PartialEq<T> for CowBytes2 {
    fn eq(&self, other: &T) -> bool {
        &**self == other.as_ref()
    }
}

impl<T: AsRef<[u8]>> PartialOrd<T> for CowBytes2 {
    fn partial_cmp(&self, other: &T) -> Option<cmp::Ordering> {
        (**self).partial_cmp(other.as_ref())
    }
}

impl serde::Serialize for CowBytes2 {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(self)
    }
}

impl<'de> serde::Deserialize<'de> for CowBytes2 {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<CowBytes2, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{Error, Visitor};
        use std::fmt;
        struct CowBytes2Visitor;

        impl<'de> Visitor<'de> for CowBytes2Visitor {
            type Value = CowBytes2;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("byte array")
            }

            #[inline]
            fn visit_bytes<E>(self, v: &[u8]) -> Result<CowBytes2, E>
            where
                E: Error,
            {
                Ok(CowBytes2::from(v))
            }

            #[inline]
            fn visit_str<E>(self, v: &str) -> Result<CowBytes2, E>
            where
                E: Error,
            {
                self.visit_bytes(v.as_ref())
            }
        }
        deserializer.deserialize_bytes(CowBytes2Visitor)
    }
}

impl Size for CowBytes2 {
    fn size(&self) -> usize {
        8 + self.inner.len()
    }
}

impl<'a> From<&'a [u8]> for CowBytes2 {
    fn from(x: &'a [u8]) -> Self {
        CowBytes2 {
            inner: Arc::new(x.to_vec()),
        }
    }
}

impl From<Box<[u8]>> for CowBytes2 {
    fn from(x: Box<[u8]>) -> Self {
        CowBytes2 {
            inner: Arc::new(x.into_vec()),
        }
    }
}

impl From<Vec<u8>> for CowBytes2 {
    fn from(x: Vec<u8>) -> Self {
        CowBytes2 { inner: Arc::new(x) }
    }
}

impl Borrow<[u8]> for CowBytes2 {
    fn borrow(&self) -> &[u8] {
        self
    }
}

impl AsRef<[u8]> for CowBytes2 {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

unsafe impl StableDeref for CowBytes2 {}

impl Deref for CowBytes2 {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for CowBytes2 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut Arc::make_mut(&mut self.inner)[..]
    }
}

impl<'a> IntoIterator for &'a CowBytes2 {
    type Item = &'a u8;
    type IntoIter = ::std::slice::Iter<'a, u8>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl CowBytes2 {
    /// Constructs a new, empty `CowBytes2`.
    #[inline]
    pub fn new() -> Self {
        CowBytes2::default()
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

    /// Create a new, empty `CowBytes2` with the given capacity.
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        CowBytes2 {
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

    /// Returns a `SlicedCowBytes2` which points to `self[pos..pos+len]`.
    pub fn slice(self, pos: u32, len: u32) -> SlicedCowBytes2 {
        SlicedCowBytes2::from(self).subslice(pos, len)
    }

    /// Returns a `SlicedCowBytes2` which points to `self[pos..]`.
    pub fn slice_from(self, pos: u32) -> SlicedCowBytes2 {
        let len = self.len() as u32;
        self.slice(pos, len - pos)
    }
}

impl<'a> Extend<&'a u8> for CowBytes2 {
    fn extend<T: IntoIterator<Item = &'a u8>>(&mut self, iter: T) {
        Arc::make_mut(&mut self.inner).extend(iter)
    }
}

/// Reference-counted pointer which points to a subslice of the referenced data.
#[derive(Debug, Default, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[archive(check_bytes)]
pub struct SlicedCowBytes2 {
    pub(super) data: CowBytes2,
    pos: u32,
    len: u32,
}

impl PartialEq for SlicedCowBytes2 {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for SlicedCowBytes2 {}

impl serde::Serialize for SlicedCowBytes2 {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(self)
    }
}

impl<'de> serde::Deserialize<'de> for SlicedCowBytes2 {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <CowBytes2 as serde::Deserialize>::deserialize(deserializer).map(Self::from)
    }
}

impl Size for SlicedCowBytes2 {
    fn size(&self) -> usize {
        8 + self.len as usize
    }
}

impl SlicedCowBytes2 {
    /// Returns a new subslice which points to `self[pos..pos+len]`.
    pub fn subslice(self, pos: u32, len: u32) -> Self {
        let pos = self.pos + pos;
        assert!(pos + len <= self.len);
        SlicedCowBytes2 {
            data: self.data,
            pos,
            len,
        }
    }

    /// Returns a new subslice which points to `self[pos..]`.
    pub fn slice_from(self, pos: u32) -> Self {
        assert!(pos <= self.len);
        SlicedCowBytes2 {
            data: self.data,
            pos: self.pos + pos,
            len: self.len - pos,
        }
    }
}

impl From<CowBytes2> for SlicedCowBytes2 {
    fn from(data: CowBytes2) -> Self {
        SlicedCowBytes2 {
            pos: 0,
            len: data.len() as u32,
            data,
        }
    }
}

impl Deref for SlicedCowBytes2 {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        let start = self.pos as usize;
        let end = start + self.len as usize;
        &self.data[start..end]
    }
}

#[cfg(test)]
mod tests {
    use super::{Arc, CowBytes2};
    use crate::arbitrary::GenExt;
    use quickcheck::{Arbitrary, Gen};
    use rand::{Rng, RngCore};

    impl Arbitrary for CowBytes2 {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut rng = g.rng();
            let len = rng.gen_range(0..128);
            let mut bytes = vec![0; len];
            rng.fill_bytes(&mut bytes);
            CowBytes2 {
                inner: Arc::new(bytes),
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            Box::new(self.inner.shrink().map(|inner| CowBytes2 { inner }))
        }
    }
}
