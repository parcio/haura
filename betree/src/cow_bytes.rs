//! This module provides `CowBytes` which is a Copy-on-Write smart pointer
//! similar to `std::borrow::Cow`.

use crate::{compression::DecompressionTag, size::Size};
//use serde::{Deserialize, Deserializer, Serialize, Serializer};
use stable_deref_trait::StableDeref;
use zstd_safe::WriteBuf;
//use core::slice::SlicePattern;
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

struct OwnedStr<T> {
    inner: Arc<Vec<u8>>,
    _marker: PhantomData<T>
}

struct ArchivedOwnedStr {
    // This will be a relative pointer to our string
    inner: ArchivedVec<u8>,
}

impl ArchivedOwnedStr {
    // This will help us get the bytes of our type as a str again.
    // fn as_slice(&self) -> &[u8] {

    //     unsafe {

    //         // The as_ptr() function of RelPtr will get a pointer to the str

    //         &*self.ptr.as_ptr()

    //     }

    // }
}

struct OwnedStrResolver {
    // This will be the position that the bytes of our string are stored at.
    // We'll use this to resolve the relative pointer of our
    // ArchivedOwnedStr.
    pos: usize,
    // The archived metadata for our str may also need a resolver.
    metadata_resolver: VecResolver,
}

// The Archive implementation defines the archived version of our type and
// determines how to turn the resolver into the archived form. The Serialize
// implementations determine how to make a resolver from the original value.
impl<T> Archive for OwnedStr<T> {
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
impl<T, S: Serializer + ?Sized + ScratchSpace> Serialize<S> for OwnedStr<T> {
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

// impl<D: Fallible + ?Sized> DeserializeWith<Archived<Vec<u8>>, OwnedStr, D> for OwnedStr {
//     fn deserialize_with(field: &Archived<Vec<u8>>, deserializer: &mut D) -> Result<Self, D::Error> {
//         panic!("Failed to deserialize childbuffer's node_pointer");
//     }
// }
// impl<D: Fallible + ?Sized> Deserialize<Arc<Vec<u8>>, D> for OwnedStr {
//     fn deserialize(&self, deserializer: &mut D) -> Result<Arc<Vec<u8>>, D::Error> {
        
//             panic!("Failed to deserialize childbuffer's node_pointer");
//     }
// }

impl<T, D: Fallible + ?Sized> Deserialize<OwnedStr<T>, D> for Archived<Vec<u8>> {
    fn deserialize(&self, deserializer: &mut D) -> Result<OwnedStr<T>, D::Error> {
        let vec: Vec<u8> = self.deserialize(deserializer)?;

        // Create an Arc from the Vec<u8>
        let arc_vec = Arc::new(vec);

        // Create the OwnedStr
        Ok(OwnedStr { inner: arc_vec, _marker: PhantomData })
    }
}

/// Copy-on-Write smart pointer which supports cheap cloning as it is
/// reference-counted.
#[derive(Hash, Debug, Clone, Eq, Ord, Default)]//, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
//#[archive(check_bytes)]
pub struct CowBytes {
    // TODO Replace by own implementation
    pub(super) inner: Arc<Vec<u8>>,
}

struct ArchivedCowBytes {
    // This will be a relative pointer to our string
    inner: ArchivedVec<u8>,
}

pub struct CowBytesResolver {
    len: usize,
    inner: VecResolver,
}

impl Archive for CowBytes {
    type Archived = ArchivedVec<u8>;
    type Resolver = CowBytesResolver;

    unsafe fn resolve(
        &self,
        pos: usize,
        resolver: Self::Resolver,
        out: *mut Self::Archived,
    ) {
        //println!("xxxxxxxxxxxxxxxxxxxxxxx {} {}", self.inner.len(), resolver.len);
        //ArchivedVec::resolve_from_len(self.inner.len(), pos, resolver, out);
        ArchivedVec::resolve_from_len(resolver.len, pos, resolver.inner, out);
    }
}

use speedy::{Readable, Writable};
impl<S: Serializer + ?Sized + ScratchSpace> Serialize<S> for CowBytes {
    fn serialize(
        &self,
        serializer: &mut S
    ) -> Result<Self::Resolver, S::Error> {
        //panic!("----------------------");

        let compression = &*crate::compression::COMPRESSION_VAR.read().unwrap();//default_compression.read().unwrap();
        let compressed_data = {
            let state = compression.new_compression().unwrap();
            // let mut buf = crate::buffer::BufWrite::with_capacity(crate::vdev::Block(1));
            // {
            //     // buf.write(self.inner.as_slice());
            //     // ()
            //     buf.write_all(self.inner.as_slice());
            // }
            // println!("%%% {} {}", self.inner.len(), buf.get_len());
             let mut newstate = state.write().unwrap();
             {
            //     let a = buf.into_buf();

            //     let size: u32 = u32::read_from_buffer(a.as_ref()).unwrap();
            //     let mut buf = crate::buffer::BufWrite::with_capacity(crate::vdev::Block::round_up_from_bytes(size));

                
            //     println!("%%% {} {}", size, a.as_ref().len());
                //newstate.finishext(a.as_ref())
                newstate.finishext(self.inner.as_slice())
            }
        };
        //panic!("%%% {} {}", self.inner.len(), compressed_data.unwrap().len());

        let mut lambda = |data: &Vec<u8>| {

            Ok(CowBytesResolver {
                len: data.len(),
                inner: ArchivedVec::serialize_from_slice(data.as_slice(), serializer)?,
            })
        };
        lambda(&compressed_data.unwrap())

        /*Ok(CowBytesResolver {
            len: self.inner.len(),
            inner: ArchivedVec::serialize_from_slice(self.inner.as_slice(), serializer)?,
        })*/
    }
}


use std::io::Write;

impl<D: Fallible + ?Sized> Deserialize<CowBytes, D> for ArchivedVec<u8> {
    fn deserialize(&self, deserializer: &mut D) -> Result<CowBytes, D::Error> {
        
        let vec: Vec<u8> = self.deserialize(deserializer)?;

         let compression = &*crate::compression::COMPRESSION_VAR.read().unwrap();//crate::compression::COMPRESSION_VAR.read().unwrap()default_compression.read().unwrap();

         let mut decompression_state = compression.decompression_tag().new_decompression().unwrap();//d.new_decompression();

         //let data = decompression_state.decompress(dt/*vec.as_slice()*/).unwrap();
         let data = decompression_state.decompressext(vec.as_slice()).unwrap();
         let arc_vec = Arc::new(data);

         Ok(CowBytes { inner: arc_vec })
         
/*
         let vec: Vec<u8> = self.deserialize(deserializer)?;
        
         let arc_vec = Arc::new(vec);
         Ok(CowBytes { inner: arc_vec })*/
    }
}

impl AsRef<[u8]> for ArchivedCowBytes {
    fn as_ref(&self) -> &[u8] {
        &self.inner
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

impl serde::Serialize for CowBytes {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(self)
    }
}

impl<'de> serde::Deserialize<'de> for CowBytes {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<CowBytes, D::Error>
    where
        D: serde::Deserializer<'de>,
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
#[derive(Debug, Default, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[archive(check_bytes)]
pub struct SlicedCowBytes {
    pub(super) data: CowBytes,
    pos: u32,
    len: u32,
}

impl PartialEq for SlicedCowBytes {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for SlicedCowBytes {}

impl serde::Serialize for SlicedCowBytes {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(self)
    }
}

impl<'de> serde::Deserialize<'de> for SlicedCowBytes {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <CowBytes as serde::Deserialize>::deserialize(deserializer).map(Self::from)
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
}

impl From<CowBytes> for SlicedCowBytes {
    fn from(data: CowBytes) -> Self {
        SlicedCowBytes {
            pos: 0,
            len: data.len() as u32,
            data,
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
