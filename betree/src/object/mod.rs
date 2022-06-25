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
//! [key] -> ObjectInfo, containing object id in data tree, mtime, and current object size in bytes
//! [key][0][custom byte key] -> [custom byte value]
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
    mem,
    ops::{Range, RangeBounds},
    result,
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

mod chunk;
mod meta;
use self::{chunk::*, meta::*};
pub use meta::ObjectInfo;

mod cursor;
pub use cursor::ObjectCursor;

const OBJECT_ID_COUNTER_KEY: &[u8] = b"\0oid";

#[derive(Debug, Clone, Copy, Readable, Writable)]
/// The internal id of an object after name resolution, to be treated as an opaque identifier of
/// fixed but unreliable size.
pub struct ObjectId(u64);
impl ObjectId {
    #[allow(missing_docs)]
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

fn decode_object_chunk_key(key: &[u8; 8 + 4]) -> (ObjectId, u32) {
    let id = u64::from_be_bytes(key[..8].try_into().unwrap());
    let offset = u32::from_be_bytes(key[8..].try_into().unwrap());
    (ObjectId(id), offset)
}

/// An object store
pub struct ObjectStore<Config: DatabaseBuilder> {
    data: Dataset<Config>,
    metadata: Dataset<Config, MetaMessageAction>,
    object_id_counter: AtomicU64,
    default_storage_preference: StoragePreference,
}

impl<Config: DatabaseBuilder> Database<Config> {
    /// Create an object store backed by a single database.
    pub fn open_object_store(&mut self) -> Result<ObjectStore<Config>> {
        ObjectStore::with_datasets(
            self.open_or_create_custom_dataset(b"data", StoragePreference::NONE)?,
            self.open_or_create_custom_dataset(b"meta", StoragePreference::NONE)?,
            StoragePreference::NONE,
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
            self.open_or_create_custom_dataset(&data_name, storage_preference)?,
            self.open_or_create_custom_dataset(&meta_name, storage_preference)?,
            storage_preference,
        )
    }

    pub fn close_object_store(&mut self, store: ObjectStore<Config>) {
        self.close_dataset(store.metadata);
        trace!("Metadata closed.");
        self.close_dataset(store.data);
        trace!("Data closed.");
    }
}

impl<'os, Config: DatabaseBuilder> ObjectStore<Config> {
    /// Provide custom datasets for the object store, allowing to use different pools backed by
    /// different storage classes.
    pub fn with_datasets(
        data: Dataset<Config>,
        metadata: Dataset<Config, MetaMessageAction>,
        default_storage_preference: StoragePreference,
    ) -> Result<ObjectStore<Config>> {
        Ok(ObjectStore {
            object_id_counter: {
                let last_key = data.get(OBJECT_ID_COUNTER_KEY)?.and_then(
                    |slice: SlicedCowBytes| -> Option<[u8; 8]> { (&slice[..]).try_into().ok() },
                );
                if let Some(bytes) = last_key {
                    // the last used id was stored, so start with the next one
                    AtomicU64::new(u64::from_le_bytes(bytes) + 1)
                } else {
                    log::info!("no saved oid counter, resetting to 0");
                    // no last id saved, start from 0
                    AtomicU64::new(0)
                }
            },
            data,
            metadata,
            default_storage_preference,
        })
    }

    /// Create a new object handle.
    pub fn create_object_with_pref(
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

        self.update_object_info(key, &MetaMessage::set_info(&info))?;
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

    /// Create a new object handle.
    pub fn create_object(&'os self, key: &[u8]) -> Result<ObjectHandle<'os, Config>> {
        self.create_object_with_pref(key, self.default_storage_preference)
            .map(|(handle, _info)| handle)
    }

    /// Open an existing object by key, return `None` if it doesn't exist.
    /// As the object metadata needs to be queried anyway, it is also returned.
    pub fn open_object_with_pref(
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

    /// Open an existing object by key, return `None` if it doesn't exist.
    /// As the object metadata needs to be queried anyway, it is also returned.
    pub fn open_object_with_info(
        &'os self,
        key: &[u8],
    ) -> Result<Option<(ObjectHandle<'os, Config>, ObjectInfo)>> {
        self.open_object_with_pref(key, self.default_storage_preference)
    }

    /// Open an existing object by key, return `None` if it doesn't exist.
    pub fn open_object(&'os self, key: &[u8]) -> Result<Option<ObjectHandle<'os, Config>>> {
        self.open_object_with_info(key)
            .map(|option| option.map(|(handle, _info)| handle))
    }

    /// Try to open an object, but create it if it didn't exist.
    pub fn open_or_create_object_with_pref(
        &'os self,
        key: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<(ObjectHandle<'os, Config>, ObjectInfo)> {
        if let Some(obj) = self.open_object_with_pref(key, storage_preference)? {
            Ok(obj)
        } else {
            self.create_object_with_pref(key, storage_preference)
        }
    }

    /// Try to open an object, but create it if it didn't exist.
    pub fn open_or_create_object_with_info(
        &'os self,
        key: &[u8],
    ) -> Result<(ObjectHandle<'os, Config>, ObjectInfo)> {
        self.open_or_create_object_with_pref(key, self.default_storage_preference)
    }

    /// Try to open an object, but create it if it didn't exist.
    pub fn open_or_create_object(&'os self, key: &[u8]) -> Result<ObjectHandle<'os, Config>> {
        self.open_or_create_object_with_info(key)
            .map(|(handle, _info)| handle)
    }

    /// Unsafely construct an [ObjectHandle] from an [ObjectStore] and [Object] descriptor.
    /// This is an escape mechanism means for when storing [ObjectHandle]s become too costly
    /// or difficult, and doesn't protect from using mismatched [ObjectStore]s and [Object]s.
    /// Operations on an [ObjectHandle] created for an [Object] that doesn't exist in the
    /// [ObjectStore] it exists in, will result in undefined behaviour.
    pub fn handle_from_object(&'os self, object: Object) -> ObjectHandle<'os, Config> {
        ObjectHandle {
            store: self,
            object,
        }
    }

    /// Delete an existing object.
    pub(crate) fn delete_object(&'os self, handle: &ObjectHandle<Config>) -> Result<()> {
        // FIXME: bad error handling, object can end up partially deleted
        // Delete metadata before data, otherwise object could be concurrently reopened,
        // rewritten, and deleted with a live handle.
        self.update_object_info(&handle.object.key[..], &MetaMessage::delete())?;
        let (start, end) = handle.object.metadata_bounds();
        let meta_delete = SlicedCowBytes::from(meta::delete_custom());
        for (k, _v) in self.metadata.range(start..end)?.flatten() {
            self.metadata.insert_msg(k, meta_delete.clone());
        }

        self.data.range_delete(
            &object_chunk_key(handle.object.id, 0)[..]
                ..&object_chunk_key(handle.object.id, u32::MAX)[..],
        )?;

        Ok(())
    }

    /// Iterate objects whose keys are contained in the given range.
    ///
    /// The iterated handles are created with [StoragePreference::NONE].
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
            .flat_map(|res| match res {
                Ok(v) => Some(v),
                // FIXME: report this in a way that the caller can react to
                Err(e) => {
                    eprintln!("discarding object due to error: {}", e);
                    None
                }
            })
            .filter(|(key, _value)| meta::is_fixed_key(key))
            .map(move |(key, value)| {
                let info = ObjectInfo::read_from_buffer_with_ctx(meta::ENDIAN, &value).unwrap();
                (
                    ObjectHandle {
                        store: self,
                        object: Object {
                            key: key.to_vec(),
                            id: info.object_id,
                            storage_preference: StoragePreference::NONE,
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

    fn update_object_info(&'os self, key: &[u8], info: &MetaMessage) -> Result<()> {
        self.metadata
            .insert_msg_with_pref(key, info.pack().into(), StoragePreference::NONE)
    }

    #[allow(missing_docs)]
    #[cfg(feature = "internal-api")]
    pub fn data_tree(&self) -> &Dataset<Config> {
        &self.data
    }

    #[allow(missing_docs)]
    #[cfg(feature = "internal-api")]
    pub fn meta_tree(&self) -> &Dataset<Config, meta::MetaMessageAction> {
        &self.metadata
    }
}

/// All the information necessary to quickly describe an object.
///
/// Strictly, either the key or id can unique identify an object, but
/// the lookups/scans can be avoided by storing both of them together.
#[derive(Debug, Clone)]
pub struct Object {
    /// The key for which this object was opened.
    /// Required to interact with its metadata, which is indexed by the full object name.
    key: Vec<u8>,
    id: ObjectId,
    storage_preference: StoragePreference,
}

impl Object {
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    fn metadata_prefix_into(&self, v: &mut Vec<u8>) {
        v.extend_from_slice(&self.key[..]);
        v.push(0);
    }

    fn metadata_end(&self) -> Vec<u8> {
        let prefix_len = self.key.len() + 1;
        let mut v = Vec::with_capacity(prefix_len);
        v.extend_from_slice(&self.key[..]);
        v.push(1);
        v
    }

    fn metadata_bounds(&self) -> (Vec<u8>, Vec<u8>) {
        let prefix_len = self.key.len() + 1;

        // construct the key range of custom metadata: [key]0 .. [key]1
        let mut start = Vec::with_capacity(prefix_len);
        start.extend_from_slice(&self.key[..]);
        start.push(0);
        let mut end = start.clone();
        end.pop();
        end.push(1);

        (start, end)
    }

    fn metadata_key(&self, name: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.key.len() + 1 + name.len());
        self.metadata_prefix_into(&mut v);
        v.extend_from_slice(name);
        v
    }
}

/// A handle to an object which may or may not exist in the [ObjectStore] it was created from.
#[must_use]
pub struct ObjectHandle<'os, Config: DatabaseBuilder> {
    store: &'os ObjectStore<Config>,
    /// The [Object] addressed by this handle
    pub object: Object,
}

impl<'os, Config: DatabaseBuilder> Clone for ObjectHandle<'os, Config> {
    fn clone(&self) -> Self {
        ObjectHandle {
            store: self.store,
            object: self.object.clone(),
        }
    }
}

impl<'ds, Config: DatabaseBuilder> ObjectHandle<'ds, Config> {
    /// Close this object. This function doesn't do anything for now,
    /// but might in the future.
    pub fn close(self) -> Result<()> {
        // no-op for now
        Ok(())
    }

    /// Delete this object
    pub fn delete(self) -> Result<()> {
        self.store.delete_object(&self)
    }

    pub fn rename(&mut self, new_key: &[u8]) -> Result<()> {
        assert!(!new_key.contains(&0));

        let old_key = mem::replace(&mut self.object.key, new_key.to_vec());
        let custom_delete = SlicedCowBytes::from(meta::delete_custom());

        let mut nk = Vec::with_capacity(new_key.len());

        for (k, v) in self
            .store
            .metadata
            .range(old_key..self.object.metadata_end())?
            .flatten()
        {
            if meta::is_fixed_key(&k) {
                self.store
                    .metadata
                    .insert_msg(k, MetaMessage::delete().pack().into())?;
                self.store.metadata.insert_msg(new_key, v)?;
            } else {
                nk.clear();
                nk.extend_from_slice(new_key);
                nk.push(0);
                // unwrap-safe, k must contain 0 as is_fixed_key was false
                let meta_name_start = k.iter().position(|&b| b == 0).unwrap() + 1;
                nk.extend_from_slice(&k[meta_name_start..]);

                self.store.metadata.insert_msg(k, custom_delete.clone())?;
                self.store
                    .metadata
                    .insert_msg(&nk[..], meta::set_custom(&v).into())?;
            }
        }

        Ok(())
    }

    /// Read object data into `buf`, starting at offset `offset`, and returning the amount of
    /// actually read bytes.
    pub fn read_at(&self, mut buf: &mut [u8], offset: u64) -> result::Result<u64, (u64, Error)> {
        let oid = self.object.id;
        let mut total_read = 0;

        // Sparse object data below object size is zero-filled
        let obj_size = self
            .info()
            .map_err(|err| (total_read, err))?
            .map(|info| info.size)
            .unwrap_or(0);

        let remaining_data = obj_size.saturating_sub(offset);
        let to_be_read = (buf.len() as u64).min(remaining_data);
        let chunk_range = ChunkRange::from_byte_bounds(offset, to_be_read);

        for chunk in chunk_range.split_at_chunk_bounds() {
            let want_len = chunk.single_chunk_len() as usize;
            let key = object_chunk_key(oid, chunk.start.chunk_id);

            let maybe_data = self
                .store
                .data
                .get(&key[..])
                .map_err(|err| (total_read, err))?;
            if let Some(data) = maybe_data {
                if let Some(data) = data.get(chunk.start.offset as usize..) {
                    // there was a value, and it has some data in the desired range
                    let have_len = want_len.min(data.len());
                    buf[..have_len].copy_from_slice(&data[..have_len]);

                    // if there was less data available than requested
                    // (and because only data below obj_size is requested at all),
                    // we need to zero-fill the rest of this chunk
                    buf[have_len..want_len].fill(0);
                } else {
                    // there was a value, but it has no data in the desired range
                    buf[..want_len].fill(0);
                }
            } else {
                // there was no data for this key, but there could be additional keys
                // separated by a gap, if this is a sparse object
                buf[..want_len].fill(0);
            }

            total_read += want_len as u64;
            buf = &mut buf[want_len..];
        }

        Ok(total_read)
    }

    /// Read this object in chunk-aligned blocks. The iterator will contain any existing chunks
    /// within `chunk_range`, and specify the address range of each returned chunk in bytes.
    ///
    /// For sparse objects, this will not include unallocated chunks,
    /// and partially written chunks are not zero-filled.
    pub fn read_chunk_range(
        &self,
        chunk_range: Range<u32>,
    ) -> Result<impl Iterator<Item = Result<(Range<u64>, SlicedCowBytes)>>> {
        let iter = self.store.data.range(
            &object_chunk_key(self.object.id, chunk_range.start)[..]
                ..&object_chunk_key(self.object.id, chunk_range.end),
        )?;

        let with_chunks = iter.map(|res| match res {
            Ok((k, v)) => {
                let k: &[u8; 8 + 4] = &k[..].try_into().expect("Invalid key length");
                let (_oid, chunk) = decode_object_chunk_key(k);
                let chunk = ChunkOffset {
                    chunk_id: chunk,
                    offset: 0,
                };
                let byte_offset = chunk.as_bytes();
                let range = byte_offset..byte_offset + k.len() as u64;
                Ok((range, v))
            }
            Err(e) => Err(e),
        });

        Ok(with_chunks)
    }

    /// Read all allocated chunks of this object, along with the address range of
    /// each returned chunk in bytes.
    ///
    /// For sparse objects, this will not include unallocated chunks,
    /// and partially written chunks are not zero-filled.
    pub fn read_all_chunks(
        &self,
    ) -> Result<impl Iterator<Item = Result<(Range<u64>, SlicedCowBytes)>>> {
        self.read_chunk_range(0..CHUNK_MAX)
    }

    /// Write `buf.len()` bytes from `buf` to this objects data, starting at offset `offset`.
    /// This will update the objects size to the largest byte index that has been inserted,
    /// and set the modification time to the current system time when the write was completed.
    ///
    /// `storage_pref` is only used for the data chunks, not for any metadata updates.
    /// If an error is encounted while writing chunks, the operation is aborted and the amount
    /// of bytes written is returned alongside the error.
    pub fn write_at_with_pref(
        &self,
        mut buf: &[u8],
        offset: u64,
        storage_pref: StoragePreference,
    ) -> result::Result<u64, (u64, Error)> {
        let chunk_range = ChunkRange::from_byte_bounds(offset, buf.len() as u64);
        let mut meta_change = MetaMessage::default();
        let mut total_written = 0;
        log::trace!("Entered object::write_at_with_pref");

        for chunk in chunk_range.split_at_chunk_bounds() {
            let len = chunk.single_chunk_len() as usize;
            let key = object_chunk_key(self.object.id, chunk.start.chunk_id);

            self.store
                .data
                .upsert_with_pref(&key[..], &buf[..len], chunk.start.offset, storage_pref)
                .map_err(|err| {
                    // best-effort metadata update
                    // this is called only when the original upsert errored,
                    // there's not much we can do to handle an error during error handling
                    meta_change.mtime = Some(SystemTime::now());
                    let _ = self
                        .store
                        .update_object_info(&self.object.key, &meta_change);
                    (total_written, err)
                })?;
            buf = &buf[len..];

            total_written += len as u64;

            // Can overwrite without checking previous value, offsets monotically increase during
            // a single write_at invocation, and message merging combines sizes by max.
            meta_change.size = Some(chunk.end.as_bytes());
        }

        meta_change.mtime = Some(SystemTime::now());
        self.store
            .update_object_info(&self.object.key, &meta_change)
            .map(|()| total_written)
            .map_err(|err| (total_written, err))
    }

    /// Write `buf.len()` bytes from `buf` to this objects data, starting at offset `offset`.
    /// This will update the objects size to the largest byte index that has been inserted,
    /// and set the modification time to the current system time when the write was started.
    pub fn write_at(&self, buf: &[u8], offset: u64) -> result::Result<u64, (u64, Error)> {
        self.write_at_with_pref(buf, offset, self.object.storage_preference)
    }

    /// Fetches this object's fixed metadata.
    /// Return this objects size in bytes. Size is defined as the largest offset of any byte in
    /// this objects data, and not the total count of bytes, as there could be sparsely allocated
    /// objects.
    ///
    /// Ok(None) is only returned if the object was deleted concurrently.
    pub fn info(&self) -> Result<Option<ObjectInfo>> {
        self.store.read_object_info(&self.object.key)
    }

    pub fn get_metadata(&self, name: &[u8]) -> Result<Option<SlicedCowBytes>> {
        assert!(!name.contains(&0));
        let key = self.object.metadata_key(name);
        self.store.metadata.get(key)
    }

    pub fn set_metadata(&self, name: &[u8], value: &[u8]) -> Result<()> {
        assert!(!name.contains(&0));
        let key = self.object.metadata_key(name);
        let msg = meta::set_custom(value);
        self.store
            .metadata
            .insert_msg(key, SlicedCowBytes::from(msg))
    }

    pub fn delete_metadata(&self, name: &[u8]) -> Result<()> {
        assert!(!name.contains(&0));
        let key = self.object.metadata_key(name);
        let msg = meta::delete_custom();
        self.store
            .metadata
            .insert_msg(key, SlicedCowBytes::from(msg))
    }

    /// Fetches this object's custom metadata.
    pub fn iter_metadata(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<(SlicedCowBytes, SlicedCowBytes)>>>> {
        let (start, end) = self.object.metadata_bounds();
        let prefix_len = start.len();

        let iter = self
            .store
            .metadata
            .range(start..end)?
            // strip key prefix, leave only metadata entry name
            .map(move |res| res.map(|(k, v)| (k.slice_from(prefix_len as u32), v)));

        Ok(Box::new(iter))
    }

    /// Migrate the whole object to a specified storage preference.
    pub fn migrate(&self, pref: StoragePreference) -> Result<()> {
        self.migrate_range(u64::MAX, 0, pref)
    }

    /// Migrate a range of chunks to a new storage preference
    pub fn migrate_range(&self, length: u64, offset: u64, pref: StoragePreference) -> Result<()> {
        let chunk_range = ChunkRange::from_byte_bounds(offset, length);

        self.store.data.migrate_range(
            &object_chunk_key(self.object.id, chunk_range.start.chunk_id)[..]
                ..&object_chunk_key(self.object.id, chunk_range.end.chunk_id),
            pref,
        )
    }
}
