//! Objects are built on top of the key-value interface, and while there's nothing to prevent
//! simultaneous usage of key-value and object interface in the same dataset, it is discouraged.
//!
//! An object key can consist of any byte sequence that does not contain internal null (0) bytes. An object store consists of two trees, one for the object contents, the other for global and per-object metadata.
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

#![allow(missing_docs)]
use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::Dml,
    database::root_tree_msg::{
        OBJECT_STORE_DATA_PREFIX, OBJECT_STORE_ID_COUNTER_PREFIX, OBJECT_STORE_NAME_TO_ID_PREFIX,
    },
    database::{DatasetId, Error, Result},
    migration::{DatabaseMsg, GlobalObjectId},
    size::StaticSize,
    storage_pool::StoragePoolLayer,
    tree::{DefaultMessageAction, TreeLayer},
    vdev::Block,
    Database, Dataset, PreferredAccessType, StoragePreference,
};

use crossbeam_channel::Sender;
use speedy::{Readable, Writable};

use std::{
    borrow::Borrow,
    convert::TryInto,
    fmt::Display,
    mem,
    ops::{Range, RangeBounds},
    result,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Instant, SystemTime},
};

mod chunk;
mod meta;
use self::{chunk::*, meta::*};
pub use meta::ObjectInfo;

mod cursor;
pub use cursor::ObjectCursor;

const OBJECT_ID_COUNTER_KEY: &[u8] = b"\0oid";

use serde::Serialize;

#[derive(Debug, Clone, Copy, Readable, Writable, PartialEq, Eq, Hash, Serialize)]
/// The internal id of an object after name resolution, to be treated as an opaque identifier of
/// fixed but unreliable size.
pub struct ObjectId(u64);
impl ObjectId {
    #[allow(missing_docs)]
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.0))
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

/// An object store which can be shared with multiple threads by cloning.  After
/// the first instance is closed all others are blocked from access. This is
/// useful in situations where independent asynchronously running threads need
/// access to the same resources but do not share some inherent synchronization
/// scheme like scoped threads for example when holding multiple object stores
/// for individual data purposes.
#[derive(Clone)]
pub struct ObjectStore {
    id: ObjectStoreId,
    data: Dataset,
    metadata: Dataset<MetaMessageAction>,
    object_id_counter: Arc<AtomicU64>,
    default_storage_preference: StoragePreference,
    report: Option<Sender<DatabaseMsg>>,
}

// A type alias to represent the on disk identifier for a specific object store.
// Required to find object stores on reinitialization without names.
pub(crate) type ObjectStoreId = DatasetId;

struct ObjectStoreData {
    data: DatasetId,
    meta: DatasetId,
}

use std::io::Write;
impl ObjectStoreData {
    fn pack(&self) -> Result<Vec<u8>> {
        let mut buf = vec![0; 2 * DatasetId::static_size()];
        (&mut buf[0..]).write_all(&self.data.pack())?;
        (&mut buf[DatasetId::static_size()..]).write_all(&self.meta.pack())?;
        Ok(buf)
    }

    fn unpack(data: &[u8]) -> ObjectStoreData {
        Self {
            data: DatasetId::unpack(&data[0..DatasetId::static_size()]),
            meta: DatasetId::unpack(&data[DatasetId::static_size()..]),
        }
    }
}

impl Database {
    fn allocate_os_id(&mut self) -> Result<ObjectStoreId> {
        let key = &[OBJECT_STORE_ID_COUNTER_PREFIX] as &[_];
        let last_os_id = self
            .root_tree
            .get(key)?
            .map(|b| ObjectStoreId::unpack(&b))
            .unwrap_or_default();
        let next_os_id = last_os_id.next();
        let data = &next_os_id.pack() as &[_];
        self.root_tree.insert(
            key,
            DefaultMessageAction::insert_msg(data),
            StoragePreference::NONE,
        )?;
        Ok(next_os_id)
    }

    fn get_or_create_os_id(&mut self, name: &[u8]) -> Result<ObjectStoreId> {
        let mut key = Vec::new();
        key.push(OBJECT_STORE_NAME_TO_ID_PREFIX);
        key.extend_from_slice(name);
        match self
            .root_tree
            .get(key.clone())?
            .map(|b| ObjectStoreId::unpack(&b))
        {
            Some(id) => Ok(id),
            None => {
                let new_id = self.allocate_os_id()?;
                self.root_tree.insert(
                    key,
                    DefaultMessageAction::insert_msg(&new_id.pack()),
                    StoragePreference::NONE,
                )?;
                Ok(new_id)
            }
        }
    }

    fn store_os_data(&mut self, os_id: ObjectStoreId, os_data: ObjectStoreData) -> Result<()> {
        let mut key = Vec::new();
        key.push(OBJECT_STORE_DATA_PREFIX);
        key.extend_from_slice(&os_id.pack());
        let data = os_data.pack()?;
        self.root_tree.insert(
            key,
            DefaultMessageAction::insert_msg(&data),
            StoragePreference::NONE,
        )?;
        Ok(())
    }

    fn fetch_os_data(&mut self, os_id: &ObjectStoreId) -> Result<Option<ObjectStoreData>> {
        let mut key = vec![OBJECT_STORE_DATA_PREFIX];
        key.extend_from_slice(&os_id.pack());
        Ok(self
            .root_tree
            .get(key)?
            .map(|buf| ObjectStoreData::unpack(&buf)))
    }

    /// For tests only: Exposed version of [object_object_store_with_id].
    #[cfg(feature = "internal-api")]
    pub fn internal_open_object_store_with_id(
        &mut self,
        os_id: ObjectStoreId,
    ) -> Result<ObjectStore> {
        self.open_object_store_with_id(os_id)
    }

    /// Open an object store by its internal Id. This method can be used
    /// whenever storing the actual names of object stores is too much expected
    /// effort.
    #[cfg(feature = "internal-api")]
    pub fn open_object_store_with_id_pub(&mut self, os_id: ObjectStoreId) -> Result<ObjectStore> {
        self.open_object_store_with_id(os_id)
    }

    pub(crate) fn open_object_store_with_id(
        &mut self,
        os_id: ObjectStoreId,
    ) -> Result<ObjectStore> {
        let store = self
            .fetch_os_data(&os_id)?
            .map(Ok::<ObjectStoreData, crate::database::errors::Error>)
            .unwrap_or_else(|| bail!(Error::DoesNotExist))?;

        ObjectStore::with_datasets(
            os_id,
            self.open_dataset_with_id(store.data)?,
            self.open_dataset_with_id(store.meta)?,
            StoragePreference::NONE,
            self.db_tx.clone(),
        )
    }

    /// Creates an iterator over all object stores by their internally used Id.
    #[cfg(feature = "internal-api")]
    pub fn iter_object_stores_pub(&self) -> Result<impl Iterator<Item = Result<ObjectStoreId>>> {
        self.iter_object_stores()
    }

    /// Iterates over all object stores in the database.
    pub(crate) fn iter_object_stores(&self) -> Result<impl Iterator<Item = Result<ObjectStoreId>>> {
        let low = &[OBJECT_STORE_DATA_PREFIX] as &[_];
        let high = &[OBJECT_STORE_DATA_PREFIX + 1] as &[_];
        Ok(self.root_tree.range(low..high)?.map(move |result| {
            let (b, _) = result?;
            Ok(ObjectStoreId::unpack(&b[1..]))
        }))
    }

    /// Iterates over all object stores by their given names.
    pub fn iter_object_store_names(&self) -> Result<impl Iterator<Item = CowBytes>> {
        let low = &[OBJECT_STORE_NAME_TO_ID_PREFIX] as &[_];
        let high = &[OBJECT_STORE_NAME_TO_ID_PREFIX + 1] as &[_];
        Ok(self.root_tree.range(low..high)?.filter_map(move |result| {
            result.ok().and_then(|(b, _)| match b.contains(&0) {
                true => None,
                false => Some(b),
            })
        }))
    }

    /// Create an object store backed by a single database.
    pub fn open_object_store(&mut self) -> Result<ObjectStore> {
        let id = self.get_or_create_os_id(&[0])?;
        let data = self.open_or_create_custom_dataset(b"data", StoragePreference::NONE, false)?;
        let meta = self.open_or_create_custom_dataset(b"meta", StoragePreference::NONE, false)?;
        self.store_os_data(
            id,
            ObjectStoreData {
                data: data.id(),
                meta: meta.id(),
            },
        )?;
        ObjectStore::with_datasets(id, data, meta, StoragePreference::NONE, self.db_tx.clone())
    }

    /// Create a namespaced object store, with the datasets "{name}\0data" and "{name}\0meta".
    pub fn open_named_object_store(
        &mut self,
        name: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<ObjectStore> {
        if name.contains(&0) {
            return Err(Error::KeyContainsNullByte);
        }
        let mut v = name.to_vec();
        v.push(0);

        let id = self.get_or_create_os_id(name)?;

        let mut data_name = v.clone();
        data_name.extend_from_slice(b"data");
        let mut meta_name = v;
        meta_name.extend_from_slice(b"meta");
        let data = self.open_or_create_custom_dataset(&data_name, storage_preference, false)?;
        let meta = self.open_or_create_custom_dataset(&meta_name, storage_preference, false)?;
        self.store_os_data(
            id,
            ObjectStoreData {
                data: data.id(),
                meta: meta.id(),
            },
        )?;

        ObjectStore::with_datasets(id, data, meta, storage_preference, self.db_tx.clone())
    }

    pub fn close_object_store(&mut self, store: ObjectStore) {
        if let Some(tx) = &self.db_tx {
            let _ = tx
                .send(DatabaseMsg::ObjectstoreClose(store.id))
                .map_err(|_| warn!("Channel Receiver has been dropped."));
        }
        let _ = self.close_dataset(store.metadata);
        trace!("Metadata closed.");
        let _ = self.close_dataset(store.data);
        trace!("Data closed.");
    }
}

impl<'os> ObjectStore {
    /// Provide custom datasets for the object store, allowing to use different pools backed by
    /// different storage classes.
    pub fn with_datasets(
        id: ObjectStoreId,
        data: Dataset,
        metadata: Dataset<MetaMessageAction>,
        default_storage_preference: StoragePreference,
        report: Option<Sender<DatabaseMsg>>,
    ) -> Result<ObjectStore> {
        let _d_id = data.id();
        let _m_id = metadata.id();
        let store = ObjectStore {
            id,
            object_id_counter: {
                let last_key = data.get(OBJECT_ID_COUNTER_KEY)?.and_then(
                    |slice: SlicedCowBytes| -> Option<[u8; 8]> { (&slice[..]).try_into().ok() },
                );
                if let Some(bytes) = last_key {
                    // the last used id was stored, so start with the next one
                    Arc::new(AtomicU64::new(u64::from_le_bytes(bytes) + 1))
                } else {
                    log::info!("no saved oid counter, resetting to 0");
                    // no last id saved, start from 0
                    Arc::new(AtomicU64::new(0))
                }
            },
            data,
            metadata,
            default_storage_preference,
            report: report.clone(),
        };
        if let Some(tx) = report {
            let _ = tx
                .send(DatabaseMsg::ObjectstoreOpen(store.id, store.clone()))
                .map_err(|_| warn!("Channel receiver was dropped."));
        }
        Ok(store)
    }

    /// Return an iterator overall object names and metadata in this object store.
    pub fn iter_objects(&self) -> Result<impl Iterator<Item = (CowBytes, ObjectInfo)>> {
        // Iterate over the metadata and create tuples of object keys and ids.
        Ok(self
            .metadata
            .range(&[0u8] as &[_]..=&[u8::MAX] as &[_])?
            .map(|res| {
                let (k, v) = res.unwrap();
                (
                    k,
                    ObjectInfo::read_from_buffer_with_ctx(meta::ENDIAN, &v).unwrap(),
                )
            }))
    }

    /// Create a new object handle and fit storage location to expected access
    /// pattern.
    pub fn create_object_with_access_type(
        &'os self,
        key: &[u8],
        access_type: PreferredAccessType,
    ) -> Result<(ObjectHandle<'os>, ObjectInfo)> {
        let pref = self
            .data
            .call_tree(|t| t.dmu().spl().access_type_preference(access_type));
        self.init_object_with_pref_and_access_type(key, pref, access_type)
    }

    /// Create a new object handle.
    pub fn create_object_with_pref(
        &'os self,
        key: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<(ObjectHandle<'os>, ObjectInfo)> {
        self.init_object_with_pref_and_access_type(
            key,
            storage_preference,
            PreferredAccessType::Unknown,
        )
    }

    /// Create a new object handle.
    fn init_object_with_pref_and_access_type(
        &'os self,
        key: &[u8],
        storage_preference: StoragePreference,
        access_type: PreferredAccessType,
    ) -> Result<(ObjectHandle<'os>, ObjectInfo)> {
        if key.contains(&0) {
            return Err(Error::KeyContainsNullByte);
        }

        let oid = loop {
            let oid = ObjectId(self.object_id_counter.fetch_add(1, Ordering::SeqCst));

            // FIXME: Ensure that OID is valid without disk sanity checks.
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
            pref: storage_preference,
            access_pattern: access_type,
        };

        self.update_object_info(key, &MetaMessage::set_info(&info))?;
        self.data.insert_with_pref(
            OBJECT_ID_COUNTER_KEY,
            &oid.0.to_le_bytes(),
            storage_preference,
        )?;

        if let Some(tx) = self.report.as_ref() {
            let _ = tx
                .send(DatabaseMsg::ObjectOpen(
                    GlobalObjectId::build(self.id, info.object_id),
                    info.clone(),
                    CowBytes::from(key),
                ))
                .map_err(|_| warn!("Channel Receiver has been dropped."));
        }

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
    pub fn create_object(&'os self, key: &[u8]) -> Result<ObjectHandle<'os>> {
        self.create_object_with_pref(key, self.default_storage_preference)
            .map(|(handle, _info)| handle)
    }

    /// Open an existing object by key, return `None` if it doesn't exist.
    /// As the object metadata needs to be queried anyway, it is also returned.
    pub fn open_object_with_pref(
        &'os self,
        key: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<Option<(ObjectHandle<'os>, ObjectInfo)>> {
        if key.contains(&0) {
            return Err(Error::KeyContainsNullByte);
        }

        let info = self.read_object_info(key)?;

        if let (Some(info), Some(tx)) = (info.clone(), self.report.as_ref()) {
            let _ = tx
                .send(DatabaseMsg::ObjectOpen(
                    GlobalObjectId::build(self.id, info.object_id),
                    info,
                    CowBytes::from(key),
                ))
                .map_err(|_| warn!("Channel Receiver has been dropped."));
        }

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
    ) -> Result<Option<(ObjectHandle<'os>, ObjectInfo)>> {
        self.open_object_with_pref(key, self.default_storage_preference)
    }

    /// Open an existing object by key, return `None` if it doesn't exist.
    pub fn open_object(&'os self, key: &[u8]) -> Result<Option<ObjectHandle<'os>>> {
        self.open_object_with_info(key)
            .map(|option| option.map(|(handle, _info)| handle))
    }

    /// Try to open an object, but create it if it didn't exist.
    pub fn open_or_create_object_with_pref(
        &'os self,
        key: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<(ObjectHandle<'os>, ObjectInfo)> {
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
    ) -> Result<(ObjectHandle<'os>, ObjectInfo)> {
        self.open_or_create_object_with_pref(key, self.default_storage_preference)
    }

    /// Try to open an object, but create it if it didn't exist.
    pub fn open_or_create_object(&'os self, key: &[u8]) -> Result<ObjectHandle<'os>> {
        self.open_or_create_object_with_info(key)
            .map(|(handle, _info)| handle)
    }

    /// Unsafely construct an [ObjectHandle] from an [ObjectStore] and [Object] descriptor.
    /// This is an escape mechanism means for when storing [ObjectHandle]s become too costly
    /// or difficult, and doesn't protect from using mismatched [ObjectStore]s and [Object]s.
    /// Operations on an [ObjectHandle] created for an [Object] that doesn't exist in the
    /// [ObjectStore] it exists in, will result in undefined behaviour.
    pub fn handle_from_object(&'os self, object: Object) -> ObjectHandle<'os> {
        ObjectHandle {
            store: self,
            object,
        }
    }

    /// Delete an existing object.
    pub(crate) fn delete_object(&'os self, handle: &ObjectHandle) -> Result<()> {
        // FIXME: bad error handling, object can end up partially deleted
        // Delete metadata before data, otherwise object could be concurrently reopened,
        // rewritten, and deleted with a live handle.
        self.update_object_info(&handle.object.key[..], &MetaMessage::delete())?;
        let (start, end) = handle.object.metadata_bounds();
        let meta_delete = SlicedCowBytes::from(meta::delete_custom());
        for (k, _v) in self.metadata.range(start..end)?.flatten() {
            let _ = self.metadata.insert_msg(k, meta_delete.clone());
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
    ) -> Result<Box<dyn Iterator<Item = (ObjectHandle<'os>, ObjectInfo)> + 'os>>
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
                    eprintln!("discarding object due to error: {e}");
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
    pub fn data_tree(&self) -> &Dataset {
        &self.data
    }

    #[allow(missing_docs)]
    #[cfg(feature = "internal-api")]
    pub fn meta_tree(&self) -> &Dataset<meta::MetaMessageAction> {
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
pub struct ObjectHandle<'os> {
    store: &'os ObjectStore,
    /// The [Object] addressed by this handle
    pub object: Object,
}

impl<'os> Clone for ObjectHandle<'os> {
    fn clone(&self) -> Self {
        ObjectHandle {
            store: self.store,
            object: self.object.clone(),
        }
    }
}

impl<'ds> ObjectHandle<'ds> {
    /// Close this object. This function doesn't do anything for now,
    /// but might in the future.
    pub fn close(self) -> Result<()> {
        // report for semantics
        if let (Ok(Some(info)), Some(tx)) = (self.info(), self.store.report.as_ref()) {
            let _ = tx
                .send(DatabaseMsg::ObjectClose(
                    GlobalObjectId::build(self.store.id, self.object.id),
                    info,
                ))
                .map_err(|_| warn!("Channel Receiver has been dropped."));
        }

        // no-op for now
        Ok(())
    }

    /// Delete this object
    pub fn delete(self) -> Result<()> {
        self.store.delete_object(&self)
    }

    pub fn rename(&mut self, new_key: &[u8]) -> Result<()> {
        if new_key.contains(&0) {
            return Err(Error::KeyContainsNullByte);
        }

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

        let start = Instant::now();

        let mut last_offset = offset;

        let chunks = self
            .read_chunk_range(chunk_range.start.chunk_id..chunk_range.end.chunk_id)
            .map_err(|e| (total_read, e))?;

        for chunk in chunks {
            let chunk = chunk.map_err(|e| (total_read, e))?;
            let chunk_start = chunk.0.start.max(offset);

            // There was a gap in the stored data, fill with zero
            let gap = (chunk_start.checked_sub(last_offset).unwrap_or(0)).min(to_be_read) as usize;
            buf[..gap].fill(0);
            total_read += gap as u64;
            buf = &mut buf[gap..];

            let chunk_end = chunk.0.end.min(offset + to_be_read);
            let want_len = chunk_end.saturating_sub(chunk_start) as usize;
            // Cut-off data if start of the range is within the chunk
            if let Some(data) = chunk.1.get((chunk_start - chunk.0.start) as usize..) {
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
            last_offset = chunk.0.end;
            total_read += want_len as u64;
            buf = &mut buf[want_len..];
        }
        if total_read != buf.len() as u64 {
            // No data or tailing data could not be found, simply fill the
            // buffer to the end with `0` in this case.
            buf.fill(0);
            return Ok(to_be_read);
        }

        if let Some(tx) = &self.store.report {
            let _ = tx
                .send(DatabaseMsg::ObjectRead(
                    GlobalObjectId::build(self.store.id, self.object.id),
                    start.elapsed(),
                ))
                .map_err(|_| warn!("Channel Receiver has been dropped."));
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
        // FIXME: This is incorrect, correctly we shoud measure how long each individual fetch takes
        let start = Instant::now();
        let iter = self.store.data.range(
            &object_chunk_key(self.object.id, chunk_range.start)[..]
                ..=&object_chunk_key(self.object.id, chunk_range.end),
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
                let range = byte_offset..byte_offset + v.len() as u64;
                Ok((range, v))
            }
            Err(e) => Err(e),
        });
        if let Some(tx) = &self.store.report {
            let _ = tx
                .send(DatabaseMsg::ObjectRead(
                    GlobalObjectId::build(self.store.id, self.object.id),
                    start.elapsed(),
                ))
                .map_err(|_| warn!("Channel Receiver has been dropped."));
        }

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

        let start = Instant::now();
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

        if let (Some(tx), Some(size)) = (&self.store.report, meta_change.size) {
            let _ = tx
                .send(DatabaseMsg::ObjectWrite(
                    GlobalObjectId::build(self.store.id, self.object.id),
                    size,
                    storage_pref,
                    start.elapsed(),
                ))
                .map_err(|_| warn!("Channel Receiver has been dropped."));
        }
        meta_change.mtime = Some(SystemTime::now());
        meta_change.pref = Some(storage_pref);
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
        if name.contains(&0) {
            return Err(Error::KeyContainsNullByte);
        }
        let key = self.object.metadata_key(name);
        self.store.metadata.get(key)
    }

    pub fn set_metadata(&self, name: &[u8], value: &[u8]) -> Result<()> {
        if name.contains(&0) {
            return Err(Error::KeyContainsNullByte);
        }
        let key = self.object.metadata_key(name);
        let msg = meta::set_custom(value);
        self.store
            .metadata
            .insert_msg(key, SlicedCowBytes::from(msg))
    }

    pub fn delete_metadata(&self, name: &[u8]) -> Result<()> {
        if name.contains(&0) {
            return Err(Error::KeyContainsNullByte);
        }
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

    /// Migrate the whole object to a specified storage preference and write all future accesses to the same storage
    /// tier.
    pub fn migrate(&mut self, pref: StoragePreference) -> Result<()> {
        // Future writes should adhere to the same preference
        self.migrate_once(pref)?;
        self.object.storage_preference = pref;
        Ok(())
    }

    /// Migrate the whole object to a specified storage preference. This includes all present chunks, future chunks may
    /// be written to different storage tiers specified in the [Object].
    pub fn migrate_once(&self, pref: StoragePreference) -> Result<()> {
        // Has to exist with handle
        let info = self
            .info()
            .expect("An object key which should exist was not present");
        if let Some(info) = info {
            // will be atleast this large
            let blocks = Block::round_up_from_bytes(info.size);
            let tier_info = self.store.data.free_space_tier(pref)?;
            if blocks > tier_info.free {
                return Err(Error::MigrationWouldExceedStorage(pref.as_u8(), blocks));
            }
        }
        self.migrate_range(u64::MAX, 0, pref)
    }

    /// Migrate the whole object to the next faster storage tier.
    /// The object will continue to use this tier for future writes.
    /// Returns error if no higher tier is available or no storage tier is specified.
    pub fn migrate_up(&mut self) -> Result<()> {
        if let Some(pref) = self.object.storage_preference.lift() {
            self.migrate(pref)
        } else {
            Err(Error::MigrationNotPossible)
        }
    }

    /// Migrate the whole object to the next faster storage tier.
    /// Returns error if no higher tier is available or no storage tier is specified.
    pub fn migrate_up_once(&mut self) -> Result<()> {
        if let Some(pref) = self.object.storage_preference.lift() {
            self.migrate_once(pref)
        } else {
            Err(Error::MigrationNotPossible)
        }
    }

    /// Migrate the whole object to the next slower storage tier.
    /// The object will continue to use this tier for future writes.
    /// Returns error if no lower tier is available or no storage tier is specified.
    pub fn migrate_down(&mut self) -> Result<()> {
        if let Some(pref) = self.object.storage_preference.lower() {
            self.migrate(pref)
        } else {
            Err(Error::MigrationNotPossible)
        }
    }

    /// Migrate the whole object to the next slower storage tier.
    /// Returns error if no lower tier is available or no storage tier is specified.
    pub fn migrate_down_once(&self) -> Result<()> {
        if let Some(pref) = self.object.storage_preference.lower() {
            self.migrate_once(pref)
        } else {
            Err(Error::MigrationNotPossible)
        }
    }

    /// Migrate a range of chunks to a new storage preference
    pub fn migrate_range(&self, length: u64, offset: u64, pref: StoragePreference) -> Result<()> {
        let chunk_range = ChunkRange::from_byte_bounds(offset, length);

        self.store.data.migrate_range(
            &object_chunk_key(self.object.id, chunk_range.start.chunk_id)[..]
                ..&object_chunk_key(self.object.id, chunk_range.end.chunk_id),
            pref,
        )?;
        if let Some(tx) = &self.store.report {
            let _ = tx
                .send(DatabaseMsg::ObjectMigrate(
                    GlobalObjectId::build(self.store.id, self.object.id),
                    pref,
                ))
                .map_err(|_| warn!("Channel Receiver has been dropped."));
        }
        let meta_change = MetaMessage {
            pref: Some(pref),
            ..MetaMessage::default()
        };
        self.store
            .update_object_info(&self.object.key, &meta_change)
    }
}
