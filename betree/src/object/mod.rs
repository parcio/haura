//! Objects are built on top of the key-value interface, and while there's nothing to prevent
//! simultaneous usage of key-value and object interface in the same dataset, it is discouraged.
//!
//! An object key can consist of any byte sequence that does not contain internal null (0) bytes.
//!
//! An object store consists of two trees, one for the object contents, the other for global and
//! per-object metadata.
//!
//! ## Metadata tree
//!
//! ### Keys
//!
//! ```text
//! [key] -> ObjectInfo, containing object id in data tree, and current object size in bytes
//! ```
//!
//! ## Data tree
//!
//! Each object is mapped onto
//!
//! ```text
//! [0]"oid" -> last allocated object id
//! [64-bit unsigned big-endian object id][32-bit unsigned big-endian chunk ID] -> [chunk value]
//! ```
//!
//! The object id counter must be in the data tree instead of the meta tree,
//! because the value is not an ObjectInfo. Alternatively, a third tree could be created, but it'd
//! be for just one value. In practice, this shouldn't be a problem, as any sync of the oid usually
//! goes along with a sync of data chunks. It could live in the databases root tree in the future.
//!
//! ## Alternative representations
//!
//! - Separate metadata subtree
//!     - Easiest
//!     - Fast object listing
//!     - Less locality
//!
//! - Grouped together
//!     \x01some/path\x00\x00\x01
//!     \x01some/path\x00\x01
//!     - Does this actually have any benefits?
//!     - This is terrible for getting a listing of all objects
//!
//! - Could prefix key with path length, but that would scatter keys unintuitively
//!
//! Could easily have per-object chunk size, by storing block size in meta chunk
//! Con: Necessary read per object open (but not create), possibly not worth it

use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    database::{DatabaseBuilder, Error, Result},
    Database, Dataset,
};

use speedy::Readable;

use std::{
    borrow::Borrow,
    convert::TryInto,
    ops::{Range, RangeBounds},
    result,
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

mod chunk;
mod meta;
use self::{chunk::*, meta::*};

const OBJECT_ID_COUNTER_KEY: &[u8] = b"\0oid";

fn object_chunk_key(object_id: u64, chunk_id: u32) -> [u8; 8 + 4] {
    let mut b = [0; 8 + 4];
    let (object, chunk) = b.split_at_mut(8);
    object.copy_from_slice(&object_id.to_be_bytes());
    chunk.copy_from_slice(&chunk_id.to_be_bytes());
    b
}

/// An object store
pub struct ObjectStore<Config: DatabaseBuilder> {
    data: Dataset<Config>,
    metadata: Dataset<Config, MetaMessageAction>,
    object_id_counter: AtomicU64,
}

impl<Config: DatabaseBuilder> Database<Config> {
    /// Create an object store backed by a single database.
    // TODO: allow for multiple stores, construct names
    pub fn open_object_store(&mut self) -> Result<ObjectStore<Config>> {
        ObjectStore::with_datasets(
            self.open_or_create_dataset(b"data")?,
            self.open_or_create_dataset(b"meta")?,
        )
    }
}

impl<'os, Config: DatabaseBuilder> ObjectStore<Config> {
    /// Provide custom datasets for the object store, allowing to use different pools backed by
    /// different storage classes.
    pub fn with_datasets(
        data: Dataset<Config>,
        metadata: Dataset<Config, MetaMessageAction>,
    ) -> Result<ObjectStore<Config>> {
        Ok(ObjectStore {
            object_id_counter: {
                let last_key = data.get(OBJECT_ID_COUNTER_KEY)?.and_then(
                    |slice: SlicedCowBytes| -> Option<[u8; 8]> { (&slice[..]).try_into().ok() },
                );
                if let Some(bytes) = last_key {
                    AtomicU64::new(u64::from_le_bytes(bytes))
                } else {
                    // no last id saved, start from 0
                    AtomicU64::new(0)
                }
            },
            data,
            metadata,
        })
    }

    /// Create a new object handle.
    pub fn create_object(&'os self, key: &[u8]) -> Result<Object<'os, Config>> {
        assert!(!key.contains(&0));

        let oid = loop {
            let oid = self.object_id_counter.fetch_add(1, Ordering::SeqCst);

            // check for existing object with this oid
            if let Some(_existing_chunk) = self.data.get(object_chunk_key(oid, 0))? {
                warn!("oid collision: {}", oid);
            } else {
                break oid;
            }
        };

        let info = ObjectInfo {
            object_id: oid,
            size: 0,
            mtime: SystemTime::now(),
        };

        self.metadata
            .insert_msg(key, MetaMessage::set_info(&info).pack().into())?;

        // TODO: write back object_id_counter

        Ok(Object {
            store: self,
            key: key.to_vec(),
            info,
        })
    }

    pub fn open_object(&'os self, key: &[u8]) -> Result<Option<Object<'os, Config>>> {
        assert!(!key.contains(&0));

        Ok(self.metadata.get(key)?.map(|info| Object {
            store: self,
            key: key.to_vec(),
            info: ObjectInfo::read_from_buffer_with_ctx(meta::ENDIAN, &info).unwrap(),
        }))
    }

    pub fn open_or_create_object(&'os self, key: &[u8]) -> Result<Object<'os, Config>> {
        if let Some(obj) = self.open_object(key)? {
            Ok(obj)
        } else {
            self.create_object(key)
        }
    }

    pub fn delete_object(&'os self, object: &Object<Config>) -> Result<()> {
        // FIXME: bad error handling, object can end up partially deleted
        // TODO: is this proper deletion, or marked as deleted?
        self.metadata
            .insert_msg(&object.key[..], MetaMessage::delete().pack().into())?;

        self.data.range_delete(
            object_chunk_key(object.info.object_id, 0)
                ..object_chunk_key(object.info.object_id, u32::MAX),
        )?;

        Ok(())
    }

    pub fn close_object(&'os self, object: &Object<Config>) -> Result<()> {
        object.sync_metadata()
    }

    // TODO: return actual objects instead of objectinfos
    pub fn list_objects<R, K>(
        &'os self,
        range: R,
    ) -> Result<Box<dyn Iterator<Item = Object<'os, Config>> + 'os>>
    where
        R: RangeBounds<K>,
        K: Borrow<[u8]> + Into<CowBytes>,
    {
        let raw_iter = self.metadata.range(range)?;
        let iter = raw_iter
            .flat_map(Result::ok) // FIXME
            .map(move |(key, value)| {
                let info = ObjectInfo::read_from_buffer_with_ctx(meta::ENDIAN, &value).unwrap();
                Object {
                    store: self,
                    key: key.to_vec(),
                    info,
                }
            });

        Ok(Box::new(iter))
    }
}

/// A handle to an object which may or may not exist in the [ObjectStore] it was created from.
#[must_use]
pub struct Object<'os, Config: DatabaseBuilder> {
    store: &'os ObjectStore<Config>,
    /// The key for which this object was opened
    pub key: Vec<u8>,
    info: ObjectInfo,
}

impl<'ds, Config: DatabaseBuilder> Object<'ds, Config> {
    /// Close this object, writing out its metadata
    pub fn close(self) -> Result<()> {
        self.store.close_object(&self)
    }

    /// Delete this object
    pub fn delete(self) -> Result<()> {
        self.store.delete_object(&self)
    }

    /// Read object data into `buf`, starting at offset `offset`, and returning the amount of
    /// actually read bytes.
    pub fn read_at(&self, mut buf: &mut [u8], offset: u64) -> result::Result<u64, (u64, Error)> {
        let chunk_range = ChunkRange::from_byte_bounds(offset, buf.len() as u64);
        let oid = self.info.object_id;

        let mut total_read = 0;
        for chunk in chunk_range.split_at_chunk_bounds() {
            let want_len = chunk.single_chunk_len() as usize;
            let key = object_chunk_key(oid, chunk.start.chunk_id);

            let maybe_data = self
                .store
                .data
                .get(&key[..])
                .map_err(|err| (total_read, err))?;
            if let Some(data) = maybe_data {
                let data = &data[chunk.start.offset as usize..];
                let have_len = want_len.min(data.len());
                buf[..have_len].copy_from_slice(&data[..have_len]);

                total_read += have_len as u64;
                buf = &mut buf[have_len..];
            } else {
                break;
            }
        }

        Ok(total_read)
    }

    /// Read this object in chunk-aligned blocks. The iterator will contain any existing chunks in
    /// within `chunk_range`.
    pub fn read_chunk_range(
        &self,
        chunk_range: Range<u32>,
    ) -> Result<Box<dyn Iterator<Item = Result<(CowBytes, SlicedCowBytes)>>>> {
        self.store.data.range(
            &object_chunk_key(self.info.object_id, chunk_range.start)[..]
                ..&object_chunk_key(self.info.object_id, chunk_range.end),
        )
    }

    /// Read all chunks of this object.
    pub fn read_all_chunks(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<(CowBytes, SlicedCowBytes)>>>> {
        self.read_chunk_range(0..CHUNK_MAX)
    }

    /// Write `buf.len()` bytes from `buf` to this objects data, starting at offset `offset`.
    /// This will update the objects size to the largest byte index that has been inserted,
    /// and set the modification time to the current system time when the write was started.
    pub fn write_at(&mut self, mut buf: &[u8], offset: u64) -> result::Result<u64, (u64, Error)> {
        let chunk_range = ChunkRange::from_byte_bounds(offset, buf.len() as u64);
        self.info.mtime = SystemTime::now();

        let mut total_written = 0;
        for chunk in chunk_range.split_at_chunk_bounds() {
            let len = chunk.single_chunk_len() as usize;
            let key = object_chunk_key(self.info.object_id, chunk.start.chunk_id);

            self.store
                .data
                .upsert(&key[..], &buf[..len], chunk.start.offset)
                .map_err(|err| {
                    let _ = self.sync_size_mtime();
                    (total_written, err)
                })?;
            buf = &buf[len..];

            total_written += len as u64;

            self.info.size = self.info.size.max(chunk.end.as_bytes());
        }

        self.sync_size_mtime()
            .map(|()| total_written)
            .map_err(|err| (total_written, err))
    }

    /// Return this objects size in bytes. Size is defined as the largest offset of any byte in
    /// this objects data, and not the total count of bytes, as there could be sparsely allocated
    /// objects.
    pub fn size(&self) -> u64 {
        self.info.size
    }

    /// Return the time at which this object was last modified.
    pub fn modification_time(&self) -> SystemTime {
        self.info.mtime
    }

    fn sync_size_mtime(&self) -> Result<()> {
        self.store.metadata.insert_msg(
            &self.key[..],
            MetaMessage::new(None, Some(self.info.size), Some(self.info.mtime))
                .pack()
                .into(),
        )
    }

    fn sync_metadata(&self) -> Result<()> {
        self.store.metadata.insert_msg(
            &self.key[..],
            MetaMessage::set_info(&self.info).pack().into(),
        )
    }

    /*
    pub fn truncate(&mut self, size: u64) {}

    pub fn cursor(&self) {}
    pub fn mut_cursor(&mut self) {}

    fn key_chunk(&self, chunk_id: u32) -> Vec<u8> {
        assert!(chunk_id <= CHUNK_MAX);
        let mut key = self.key.clone();
        key.push(0);
        key.extend_from_slice(&chunk_id.to_be_bytes());
        key
    }

    fn key_size(&self) -> Vec<u8> {
        // static KEY_SUFFIX: &[u8; 2] = &[0, CHUNK_META_SIZE];
        let mut key = self.key.clone();
        key.push(0);
        key.extend_from_slice(&CHUNK_META_SIZE.to_be_bytes());
        key
    }

    fn key_mtime(&self) -> Vec<u8> {
        // static MTIME_SUFFIX: &[u8; 2] = &[0, CHUNK_META_MODIFICATION_TIME];
        let mut key = self.key.clone();
        key.push(0);
        key.extend_from_slice(&CHUNK_META_MODIFICATION_TIME.to_be_bytes());
        key
    }
    */
}
