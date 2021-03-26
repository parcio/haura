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
    Database, Dataset, StoragePreference,
};

use speedy::{Readable, Writable};

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
pub use meta::ObjectInfo;

const OBJECT_ID_COUNTER_KEY: &[u8] = b"\0oid";

#[derive(Debug, Clone, Copy, Readable, Writable)]
/// The internal id of an object after name resolution, to be treated as an opaque identifier of
/// fixed but unreliable size.
pub struct ObjectId(u64);
impl ObjectId {
    #[cfg(feature = "internal-api")]
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

fn object_chunk_key(ObjectId(object_id): ObjectId, chunk_id: u32) -> [u8; 8 + 4] {
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
    pub fn open_object_store(
        &mut self,
        storage_preference: StoragePreference,
    ) -> Result<ObjectStore<Config>> {
        ObjectStore::with_datasets(
            self.open_or_create_dataset(b"data", storage_preference)?,
            self.open_or_create_dataset(b"meta", storage_preference)?,
        )
    }

    /// Create a namespaced object store, with the datasets "{name}\0data" and "{name}\0meta".
    pub fn open_named_object_store(
        &mut self,
        name: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<ObjectStore<Config>> {
        assert!(!name.contains(&0));
        let mut v = name.to_vec();
        v.push(0);

        let mut data_name = v.clone();
        data_name.extend_from_slice(b"data");
        let mut meta_name = v;
        meta_name.extend_from_slice(b"meta");

        ObjectStore::with_datasets(
            self.open_or_create_dataset(&data_name, storage_preference)?,
            self.open_or_create_dataset(&meta_name, storage_preference)?,
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
    pub fn create_object(
        &'os self,
        key: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<(ObjectHandle<'os, Config>, ObjectInfo)> {
        assert!(!key.contains(&0));

        let oid = loop {
            let oid = ObjectId(self.object_id_counter.fetch_add(1, Ordering::SeqCst));

            // check for existing object with this oid
            if let Some(_existing_chunk) = self.data.get(object_chunk_key(oid, 0))? {
                warn!("oid collision: {:?}", oid);
            } else {
                break oid;
            }
        };

        let info = ObjectInfo {
            object_id: oid,
            size: 0,
            mtime: SystemTime::now(),
        };

        self.update_object_info(key, &MetaMessage::set_info(&info), storage_preference)?;
        self.data.insert_with_pref(
            OBJECT_ID_COUNTER_KEY,
            &oid.0.to_le_bytes(),
            storage_preference,
        )?;

        Ok((
            ObjectHandle {
                store: self,
                object: Object {
                    key: key.to_vec(),
                    id: oid,
                    storage_preference,
                },
            },
            info,
        ))
    }

    pub fn open_object(
        &'os self,
        key: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<Option<(ObjectHandle<'os, Config>, ObjectInfo)>> {
        assert!(!key.contains(&0));

        let info = self.read_object_info(key)?;

        Ok(info.map(|info| {
            (
                ObjectHandle {
                    store: self,
                    object: Object {
                        key: key.to_vec(),
                        id: info.object_id,
                        storage_preference,
                    },
                },
                info,
            )
        }))
    }

    pub fn open_or_create_object(
        &'os self,
        key: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<(ObjectHandle<'os, Config>, ObjectInfo)> {
        if let Some(obj) = self.open_object(key, storage_preference)? {
            Ok(obj)
        } else {
            self.create_object(key, storage_preference)
        }
    }

    pub fn handle_from_object(&'os self, object: Object) -> ObjectHandle<'os, Config> {
        ObjectHandle {
            store: self,
            object,
        }
    }

    pub fn delete_object(&'os self, handle: &ObjectHandle<Config>) -> Result<()> {
        // FIXME: bad error handling, object can end up partially deleted
        // Delete metadata before data, otherwise object could be concurrently reopened,
        // rewritten, and deleted with a live handle.
        self.update_object_info(
            &handle.object.key[..],
            &MetaMessage::delete(),
            StoragePreference::NONE,
        )?;

        self.data.range_delete(
            &object_chunk_key(handle.object.id, 0)[..]
                ..&object_chunk_key(handle.object.id, u32::MAX)[..],
        )?;

        Ok(())
    }

    pub fn close_object(&'os self, object: &ObjectHandle<Config>) -> Result<()> {
        Ok(())
    }

    // TODO: return actual objects instead of objectinfos
    pub fn list_objects<R, K>(
        &'os self,
        range: R,
    ) -> Result<Box<dyn Iterator<Item = (ObjectHandle<'os, Config>, ObjectInfo)> + 'os>>
    where
        R: RangeBounds<K>,
        K: Borrow<[u8]> + Into<CowBytes>,
    {
        let raw_iter = self.metadata.range(range)?;
        let iter = raw_iter
            .flat_map(Result::ok) // FIXME
            .map(move |(key, value)| {
                let info = ObjectInfo::read_from_buffer_with_ctx(meta::ENDIAN, &value).unwrap();
                (
                    ObjectHandle {
                        store: self,
                        object: Object {
                            key: key.to_vec(),
                            id: info.object_id,
                            storage_preference: StoragePreference::NONE, // FIXME: retrieve stored preference
                        },
                    },
                    info,
                )
            });

        Ok(Box::new(iter))
    }

    fn read_object_info(&'os self, key: &[u8]) -> Result<Option<ObjectInfo>> {
        if let Some(meta) = self.metadata.get(key)? {
            Ok(Some(
                ObjectInfo::read_from_buffer_with_ctx(meta::ENDIAN, &meta).unwrap(),
            ))
        } else {
            Ok(None)
        }
    }

    fn update_object_info(
        &'os self,
        key: &[u8],
        info: &MetaMessage,
        storage_preference: StoragePreference,
    ) -> Result<()> {
        self.metadata
            .insert_msg_with_pref(key, info.pack().into(), storage_preference)
    }
}

#[derive(Debug, Clone)]
pub struct Object {
    /// The key for which this object was opened.
    /// Required to interact with its metadata, which is indexed by the full object name.
    pub key: Vec<u8>,
    id: ObjectId,
    storage_preference: StoragePreference,
}

/// A handle to an object which may or may not exist in the [ObjectStore] it was created from.
#[must_use]
pub struct ObjectHandle<'os, Config: DatabaseBuilder> {
    store: &'os ObjectStore<Config>,
    pub object: Object,
}

impl<'ds, Config: DatabaseBuilder> ObjectHandle<'ds, Config> {
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
        let oid = self.object.id;

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
            &object_chunk_key(self.object.id, chunk_range.start)[..]
                ..&object_chunk_key(self.object.id, chunk_range.end),
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
    pub fn write_at(&self, mut buf: &[u8], offset: u64) -> result::Result<u64, (u64, Error)> {
        let chunk_range = ChunkRange::from_byte_bounds(offset, buf.len() as u64);
        let mut meta_change = MetaMessage::default();
        meta_change.mtime = Some(SystemTime::now());

        let mut total_written = 0;
        for chunk in chunk_range.split_at_chunk_bounds() {
            let len = chunk.single_chunk_len() as usize;
            let key = object_chunk_key(self.object.id, chunk.start.chunk_id);

            self.store
                .data
                .upsert_with_pref(
                    &key[..],
                    &buf[..len],
                    chunk.start.offset,
                    self.object.storage_preference,
                )
                .map_err(|err| {
                    self.store.update_object_info(
                        &self.object.key,
                        &meta_change,
                        self.object.storage_preference,
                    );
                    (total_written, err)
                })?;
            buf = &buf[len..];

            total_written += len as u64;

            // Can overwrite without checking previous value, offsets monotically increase during
            // a single write_at invocation, and message merging combines sizes by max.
            meta_change.size = Some(chunk.end.as_bytes());
        }

        self.store
            .update_object_info(
                &self.object.key,
                &meta_change,
                self.object.storage_preference,
            )
            .map(|()| total_written)
            .map_err(|err| (total_written, err))
    }

    /// Fetches this objects metadata.
    /// Return this objects size in bytes. Size is defined as the largest offset of any byte in
    /// this objects data, and not the total count of bytes, as there could be sparsely allocated
    /// objects.
    ///
    /// Ok(None) is only returned if the object was deleted concurrently.
    pub fn info(&self) -> Result<Option<ObjectInfo>> {
        self.store.read_object_info(&self.object.key)
    }
}
