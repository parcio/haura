use super::root_tree_msg::dataset;
use super::{
    errors::*, fetch_ds_data, Database, DatasetData, DatasetId, DatasetTree, Generation,
    MessageTree, RootDmu, StorageInfo,
};
use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::Dml,
    migration::DatabaseMsg,
    tree::{self, DefaultMessageAction, MessageAction, PivotKey, Tree, TreeLayer},
    StoragePreference,
};

#[cfg(feature = "internal-api")]
use crate::tree::NodeInfo;

use parking_lot::RwLock;
use std::{borrow::Borrow, collections::HashSet, ops::RangeBounds, sync::Arc};

/// The internal data set type.  This is the non-user facing variant which is
/// then wrapped in the [Dataset] type.
pub struct DatasetInner<Message = DefaultMessageAction> {
    pub(super) tree: MessageTree<RootDmu, Message>,
    pub(crate) id: DatasetId,
    name: Box<[u8]>,
    pub(super) open_snapshots: HashSet<Generation>,
    storage_preference: StoragePreference,
}

/// The data set type.
pub struct Dataset<Message = DefaultMessageAction> {
    inner: Arc<RwLock<DatasetInner<Message>>>,
}

impl<Message> Clone for Dataset<Message> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<Message> From<DatasetInner<Message>> for Dataset<Message> {
    fn from(inner: DatasetInner<Message>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }
}

impl Database {
    fn lookup_dataset_id(&self, name: &[u8]) -> Result<DatasetId> {
        let key = dataset::name_to_id(name);
        let data = self.root_tree.get(key)?.ok_or(Error::DoesNotExist)?;
        Ok(DatasetId::unpack(&data))
    }

    /// A convenience instantiation of [Database::open_custom_dataset] with the default message set.
    pub fn open_dataset(&mut self, name: &[u8]) -> Result<Dataset> {
        self.open_custom_dataset::<DefaultMessageAction>(name, StoragePreference::NONE)
    }

    /// A convenience instantiation of [Database::create_custom_dataset] with the default message set.
    pub fn create_dataset(&mut self, name: &[u8]) -> Result<()> {
        self.create_custom_dataset::<DefaultMessageAction>(name, StoragePreference::NONE)
    }

    /// A convenience instantiation of [Database::open_or_create_custom_dataset] with the default message set.
    pub fn open_or_create_dataset(&mut self, name: &[u8]) -> Result<Dataset> {
        self.open_or_create_custom_dataset::<DefaultMessageAction>(name, StoragePreference::NONE)
    }

    /// Opens a data set identified by the given name.
    ///
    /// Fails if the data set does not exist.
    pub fn open_custom_dataset<M: MessageAction + Default + 'static>(
        &mut self,
        name: &[u8],
        _storage_preference: StoragePreference,
    ) -> Result<Dataset<M>> {
        let id = self.lookup_dataset_id(name)?;
        self.open_dataset_with_id_and_name(id, name)
    }

    /// Internal function to open a dataset based on it's internal id, saves knowing the actual name.
    /// THE NAME IS NOT KNOWN IN THIS CASE AND THE NAME BOX EMPTY.
    pub(crate) fn open_dataset_with_id<M: MessageAction + Default + 'static>(
        &mut self,
        id: DatasetId,
    ) -> Result<Dataset<M>> {
        self.open_dataset_with_id_and_name(id, &[])
    }

    fn open_dataset_with_id_and_name<M: MessageAction + Default + 'static>(
        &mut self,
        id: DatasetId,
        name: &[u8],
    ) -> Result<Dataset<M>> {
        let ds_data = fetch_ds_data(&self.root_tree, id)?;
        if self.open_datasets.contains_key(&id) {
            return Err(Error::InUse);
        }
        let storage_preference = StoragePreference::NONE;
        let ds_tree = Tree::open(
            id,
            ds_data.ptr,
            M::default(),
            Arc::clone(self.root_tree.dmu()),
            storage_preference,
        );

        if let Some(ss_id) = ds_data.previous_snapshot {
            self.root_tree
                .dmu()
                .handler()
                .last_snapshot_generation
                .write()
                .insert(id, ss_id);
        }
        let erased_tree = Box::new(ds_tree.clone());
        self.open_datasets.insert(id, erased_tree);

        let ds: Dataset<M> = DatasetInner {
            tree: ds_tree,
            id,
            name: Box::from(name),
            open_snapshots: Default::default(),
            storage_preference,
        }
        .into();

        if let Some(tx) = &self.db_tx {
            let _ = tx
                .send(DatabaseMsg::DatasetOpen(id))
                .map_err(|_| warn!("Channel Receiver has been dropped."));
        }

        Ok(ds)
    }

    /// Creates a new data set identified by the given name.
    ///
    /// Fails if a data set with the same name exists already.
    pub fn create_custom_dataset<M: MessageAction + Clone>(
        &mut self,
        name: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<()> {
        match self.lookup_dataset_id(name) {
            Ok(_) => return Err(Error::AlreadyExists),
            Err(Error::DoesNotExist) => {}
            Err(e) => return Err(e),
        };
        let ds_id = self.allocate_ds_id()?;

        let tree = DatasetTree::empty_tree(
            ds_id,
            DefaultMessageAction,
            Arc::clone(self.root_tree.dmu()),
            storage_preference,
        );
        let ptr = tree.sync()?;

        let key = &dataset::data_key(ds_id) as &[_];
        let data = DatasetData {
            ptr,
            previous_snapshot: None,
        }
        .pack()?;
        self.root_tree.insert(
            key,
            DefaultMessageAction::insert_msg(&data),
            StoragePreference::NONE,
        )?;
        let mut key = vec![1];
        key.extend(name);
        self.root_tree.insert(
            key,
            DefaultMessageAction::insert_msg(&ds_id.pack()),
            StoragePreference::NONE,
        )?;
        Ok(())
    }

    /// Opens a dataset, creating a new one if none exists by the given name.
    pub fn open_or_create_custom_dataset<M: MessageAction + Default + Clone + 'static>(
        &mut self,
        name: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<Dataset<M>> {
        match self.lookup_dataset_id(name) {
            Ok(_) => self.open_custom_dataset(name, storage_preference),
            Err(Error::DoesNotExist) => self
                .create_custom_dataset::<M>(name, storage_preference)
                .and_then(|()| self.open_custom_dataset(name, storage_preference)),
            Err(e) => Err(e),
        }
    }

    fn allocate_ds_id(&mut self) -> Result<DatasetId> {
        let key = &dataset::id_counter() as &[_];
        let last_ds_id = self
            .root_tree
            .get(key)?
            .map(|b| DatasetId::unpack(&b))
            .unwrap_or_default();
        let next_ds_id = last_ds_id.next();
        let data = &next_ds_id.pack() as &[_];
        self.root_tree.insert(
            key,
            DefaultMessageAction::insert_msg(data),
            StoragePreference::NONE,
        )?;
        Ok(next_ds_id)
    }

    /// Iterates over all data sets in the database.
    pub fn iter_datasets(&self) -> Result<impl Iterator<Item = Result<SlicedCowBytes>>> {
        let low = &dataset::data_key(DatasetId::default()) as &[_];
        let high = &dataset::data_key_max() as &[_];
        Ok(self.root_tree.range(low..high)?.map(move |result| {
            let (b, _) = result?;
            let len = b.len() as u32;
            Ok(b.slice(1, len - 1))
        }))
    }

    /// Closes the given data set.
    pub fn close_dataset<Message: MessageAction + 'static>(
        &mut self,
        ds: Dataset<Message>,
    ) -> Result<()> {
        if let Some(tx) = &self.db_tx {
            let _ = tx
                .send(DatabaseMsg::DatasetClose(ds.id()))
                .map_err(|_| warn!("Channel Receiver has been dropped."));
        }
        // Check if the dataset is still opened from other positions in the stack.
        if Arc::strong_count(&ds.inner) > 1 {
            if let Some(ds) = ds.inner.try_read() {
                self.sync_ds(ds.id, &ds.tree)?;
            }
            return Ok(());
        }
        // Deactivate the dataset for further modifications
        let ds = ds.inner.write();
        log::trace!("close_dataset: Enter");
        self.sync_ds(ds.id, &ds.tree)?;
        log::trace!("synced dataset");
        self.open_datasets.remove(&ds.id);
        self.root_tree
            .dmu()
            .handler()
            .last_snapshot_generation
            .write()
            .remove(&ds.id);
        drop(ds);
        Ok(())
    }
}

impl<Message: MessageAction + 'static> DatasetInner<Message> {
    /// Inserts a message for the given key.
    pub fn insert_msg<K: Borrow<[u8]> + Into<CowBytes>>(
        &self,
        key: K,
        msg: SlicedCowBytes,
    ) -> Result<()> {
        self.insert_msg_with_pref(key, msg, StoragePreference::NONE)
    }

    /// Inserts a message for the given key, allowing to override storage preference
    /// for this operation.
    pub fn insert_msg_with_pref<K: Borrow<[u8]> + Into<CowBytes>>(
        &self,
        key: K,
        msg: SlicedCowBytes,
        storage_preference: StoragePreference,
    ) -> Result<()> {
        Ok(self
            .tree
            .insert(key, msg, storage_preference.or(self.storage_preference))?)
    }

    /// Returns the value for the given key if existing.
    pub fn get<K: Borrow<[u8]>>(&self, key: K) -> Result<Option<SlicedCowBytes>> {
        Ok(self.tree.get(key)?)
    }

    /// Immutably fetch a given node by its pivot key.
    pub(crate) fn get_node_pivot(
        &self,
        pk: &PivotKey,
    ) -> Result<Option<<RootDmu as Dml>::CacheValueRef>> {
        debug_assert!(self.id == pk.d_id());
        Ok(self.tree.get_node_pivot(pk)?)
    }

    /// Mutably fetch a given node by its pivot key.
    pub(crate) fn get_node_pivot_mut(
        &self,
        pk: &PivotKey,
    ) -> Result<Option<<RootDmu as Dml>::CacheValueRefMut>> {
        debug_assert!(self.id == pk.d_id());
        Ok(self.tree.get_mut_node_pivot(pk)?)
    }

    #[cfg(feature = "internal-api")]
    pub fn test_get_node_pivot(
        &self,
        pk: &PivotKey,
    ) -> Result<Option<<RootDmu as Dml>::CacheValueRef>> {
        debug_assert!(self.id == pk.d_id());
        Ok(self.tree.get_node_pivot(pk)?)
    }

    /// Iterates over all key-value pairs in the given key range.
    pub fn range<R, K>(
        &self,
        range: R,
    ) -> Result<Box<dyn Iterator<Item = Result<(CowBytes, SlicedCowBytes)>>>>
    where
        R: RangeBounds<K>,
        K: Borrow<[u8]> + Into<CowBytes>,
    {
        Ok(Box::new(self.tree.range(range)?.map(|r| Ok(r?))))
    }

    /// Returns the name of the data set.
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    #[allow(missing_docs)]
    #[cfg(feature = "internal-api")]
    pub fn tree_dump(&self) -> Result<NodeInfo> {
        Ok(self.tree.tree_dump()?)
    }
}

// Member access on internal type
impl<Message> Dataset<Message> {
    pub(crate) fn id(&self) -> DatasetId {
        self.inner.read().id
    }

    pub(super) fn call_open_snapshots<F, R>(&self, call: F) -> R
    where
        F: FnOnce(&HashSet<Generation>) -> R,
    {
        call(&self.inner.read().open_snapshots)
    }

    pub(super) fn call_mut_open_snapshots<F, R>(&self, call: F) -> R
    where
        F: FnOnce(&mut HashSet<Generation>) -> R,
    {
        call(&mut self.inner.write().open_snapshots)
    }

    pub(crate) fn call_tree<F, R>(&self, call: F) -> R
    where
        F: FnOnce(&MessageTree<RootDmu, Message>) -> R,
    {
        call(&self.inner.read().tree)
    }
}

// Mirroring of the [DatasetInner] API
impl<Message: MessageAction + 'static> Dataset<Message> {
    /// Inserts a message for the given key.
    pub fn insert_msg<K: Borrow<[u8]> + Into<CowBytes>>(
        &self,
        key: K,
        msg: SlicedCowBytes,
    ) -> Result<()> {
        self.inner.read().insert_msg(key, msg)
    }

    /// Inserts a message for the given key, allowing to override storage preference
    /// for this operation.
    pub fn insert_msg_with_pref<K: Borrow<[u8]> + Into<CowBytes>>(
        &self,
        key: K,
        msg: SlicedCowBytes,
        storage_preference: StoragePreference,
    ) -> Result<()> {
        self.inner
            .read()
            .insert_msg_with_pref(key, msg, storage_preference)
    }

    /// Returns the value for the given key if existing.
    pub fn get<K: Borrow<[u8]>>(&self, key: K) -> Result<Option<SlicedCowBytes>> {
        self.inner.read().get(key)
    }

    /// Iterates over all key-value pairs in the given key range.
    pub fn range<R, K>(
        &self,
        range: R,
    ) -> Result<Box<dyn Iterator<Item = Result<(CowBytes, SlicedCowBytes)>>>>
    where
        R: RangeBounds<K>,
        K: Borrow<[u8]> + Into<CowBytes>,
    {
        self.inner.read().range(range)
    }

    /// Returns the name of the data set.
    pub fn name(&self) -> Box<[u8]> {
        self.inner.read().name.clone()
    }

    #[allow(missing_docs)]
    #[cfg(feature = "internal-api")]
    pub fn tree_dump(&self) -> Result<NodeInfo> {
        self.inner.read().tree_dump()
    }
}

impl DatasetInner<DefaultMessageAction> {
    /// Inserts the given key-value pair.
    ///
    /// Note that any existing value will be overwritten.
    pub fn insert_with_pref<K: Borrow<[u8]> + Into<CowBytes>>(
        &self,
        key: K,
        data: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<()> {
        if data.len() > tree::MAX_MESSAGE_SIZE {
            return Err(Error::MessageTooLarge);
        }
        self.insert_msg_with_pref(
            key,
            DefaultMessageAction::insert_msg(data),
            storage_preference,
        )
    }

    /// Inserts the given key-value pair.
    ///
    /// Note that any existing value will be overwritten.
    pub fn insert<K: Borrow<[u8]> + Into<CowBytes>>(&self, key: K, data: &[u8]) -> Result<()> {
        self.insert_with_pref(key, data, StoragePreference::NONE)
    }

    /// Upserts the value for the given key at the given offset.
    ///
    /// Note that the value will be zeropadded as needed.
    pub fn upsert_with_pref<K: Borrow<[u8]> + Into<CowBytes>>(
        &self,
        key: K,
        data: &[u8],
        offset: u32,
        storage_preference: StoragePreference,
    ) -> Result<()> {
        if offset as usize + data.len() > tree::MAX_MESSAGE_SIZE {
            return Err(Error::MessageTooLarge);
        }
        // TODO: In case of overfilling the underlying storage we should notify in _any_ case that the writing is not successfull, for this
        // we need to know wether the space to write out has been expanded. For this we need further information which we ideally do not want
        // to read out from the disk here.
        self.insert_msg_with_pref(
            key,
            DefaultMessageAction::upsert_msg(offset, data),
            storage_preference,
        )
    }

    /// Upserts the value for the given key at the given offset.
    ///
    /// Note that the value will be zeropadded as needed.
    pub fn upsert<K: Borrow<[u8]> + Into<CowBytes>>(
        &self,
        key: K,
        data: &[u8],
        offset: u32,
    ) -> Result<()> {
        self.upsert_with_pref(key, data, offset, StoragePreference::NONE)
    }

    /// Given a key and storage preference notify for this entry to be moved to a new storage level.
    /// If the key is already located on this layer no operation is performed and success is returned.
    ///
    /// As the migration is for a singular there is no guarantee that when selectiong migrate for a key
    /// that the value is actually moved to the specified storage tier.
    /// Internally: The most high required tier will be chosen for one leaf node.
    pub fn migrate<K: Borrow<[u8]> + Into<CowBytes>>(
        &self,
        key: K,
        pref: StoragePreference,
    ) -> Result<Option<()>> {
        use crate::storage_pool::StoragePoolLayer;
        if self.tree.dmu().spl().disk_count(pref.as_u8()) == 0 {
            return Err(Error::MigrationNotPossible);
        }
        Ok(self.tree.apply_with_info(key, pref)?.map(|_| ()))
    }

    /// Deletes the key-value pair if existing.
    pub fn delete<K: Borrow<[u8]> + Into<CowBytes>>(&self, key: K) -> Result<()> {
        self.insert_msg_with_pref(
            key,
            DefaultMessageAction::delete_msg(),
            StoragePreference::NONE,
        )
    }

    pub(crate) fn free_space_tier(&self, pref: StoragePreference) -> Result<StorageInfo> {
        if let Some(info) = self.tree.dmu().handler().free_space_tier(pref.as_u8()) {
            Ok(info)
        } else {
            Err(Error::DoesNotExist)
        }
    }

    /// Removes all key-value pairs in the given key range.
    pub fn range_delete<R, K>(&self, range: R) -> Result<()>
    where
        R: RangeBounds<K>,
        K: Borrow<[u8]> + Into<CowBytes>,
    {
        let mut res = Ok(());

        for (k, _v) in self.tree.range(range)?.flatten() {
            // keep going even on errors, return earliest Err
            let del_res = self.delete(k);
            if del_res.is_err() && res.is_ok() {
                res = del_res;
            }
        }

        res
    }

    /// Migrate a complete range of keys to another storage preference.
    /// If an entry is already located on this layer no operation is performed and success is returned.
    pub fn migrate_range<R, K>(&self, range: R, pref: StoragePreference) -> Result<()>
    where
        K: Borrow<[u8]> + Into<CowBytes>,
        R: RangeBounds<K>,
    {
        for (k, _v) in self.tree.range(range)?.flatten() {
            // abort on errors, they will likely be that one layer is full
            self.migrate(k, pref)?;
        }
        Ok(())
    }
}

// Mirroring the [DatasetInner] API
impl Dataset<DefaultMessageAction> {
    /// Inserts the given key-value pair.
    ///
    /// Note that any existing value will be overwritten.
    pub fn insert_with_pref<K: Borrow<[u8]> + Into<CowBytes>>(
        &self,
        key: K,
        data: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<()> {
        self.inner
            .read()
            .insert_with_pref(key, data, storage_preference)
    }

    /// Inserts the given key-value pair.
    ///
    /// Note that any existing value will be overwritten.
    pub fn insert<K: Borrow<[u8]> + Into<CowBytes>>(&self, key: K, data: &[u8]) -> Result<()> {
        self.inner.read().insert(key, data)
    }

    /// Upserts the value for the given key at the given offset.
    ///
    /// Note that the value will be zeropadded as needed.
    pub fn upsert_with_pref<K: Borrow<[u8]> + Into<CowBytes>>(
        &self,
        key: K,
        data: &[u8],
        offset: u32,
        storage_preference: StoragePreference,
    ) -> Result<()> {
        self.inner
            .read()
            .upsert_with_pref(key, data, offset, storage_preference)
    }

    /// Upserts the value for the given key at the given offset.
    ///
    /// Note that the value will be zeropadded as needed.
    pub fn upsert<K: Borrow<[u8]> + Into<CowBytes>>(
        &self,
        key: K,
        data: &[u8],
        offset: u32,
    ) -> Result<()> {
        self.inner.read().upsert(key, data, offset)
    }

    /// Immutably fetch a given node by its pivot key.
    pub(crate) fn get_node_pivot(
        &self,
        pk: &PivotKey,
    ) -> Result<Option<<RootDmu as Dml>::CacheValueRef>> {
        self.inner.read().get_node_pivot(pk)
    }

    /// Mutably fetch a given node by its pivot key.
    pub(crate) fn get_node_pivot_mut(
        &self,
        pk: &PivotKey,
    ) -> Result<Option<<RootDmu as Dml>::CacheValueRefMut>> {
        self.inner.read().get_node_pivot_mut(pk)
    }

    #[cfg(feature = "internal-api")]
    /// Fetch a node by it's pivot key. For testing purposes.
    pub fn test_get_node_pivot(
        &self,
        pk: &PivotKey,
    ) -> Result<Option<<RootDmu as Dml>::CacheValueRef>> {
        self.inner.read().test_get_node_pivot(pk)
    }

    /// Given a key and storage preference notify for this entry to be moved to a new storage level.
    /// If the key is already located on this layer no operation is performed and success is returned.
    ///
    /// As the migration is for a singular there is no guarantee that when selectiong migrate for a key
    /// that the value is actually moved to the specified storage tier.
    /// Internally: The most high required tier will be chosen for one leaf node.
    pub fn migrate<K: Borrow<[u8]> + Into<CowBytes>>(
        &self,
        key: K,
        pref: StoragePreference,
    ) -> Result<Option<()>> {
        self.inner.read().migrate(key, pref)
    }

    /// Deletes the key-value pair if existing.
    pub fn delete<K: Borrow<[u8]> + Into<CowBytes>>(&self, key: K) -> Result<()> {
        self.inner.read().delete(key)
    }

    pub(crate) fn free_space_tier(&self, pref: StoragePreference) -> Result<StorageInfo> {
        self.inner.read().free_space_tier(pref)
    }

    /// Removes all key-value pairs in the given key range.
    pub fn range_delete<R, K>(&self, range: R) -> Result<()>
    where
        R: RangeBounds<K>,
        K: Borrow<[u8]> + Into<CowBytes>,
    {
        self.inner.read().range_delete(range)
    }

    /// Migrate a complete range of keys to another storage preference.
    /// If an entry is already located on this layer no operation is performed and success is returned.
    pub fn migrate_range<R, K>(&self, range: R, pref: StoragePreference) -> Result<()>
    where
        K: Borrow<[u8]> + Into<CowBytes>,
        R: RangeBounds<K>,
    {
        self.inner.read().migrate_range(range, pref)
    }
}
