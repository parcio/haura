use super::{
    ds_data_key, errors::*, fetch_ds_data, Database, DatasetData, DatasetId, DatasetTree,
    Generation, MessageTree,
};
use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::DmlWithHandler,
    database::DatabaseBuilder,
    tree::{self, DefaultMessageAction, MessageAction, Tree, TreeBaseLayer, TreeLayer},
    StoragePreference,
};
use std::{borrow::Borrow, collections::HashSet, ops::RangeBounds, sync::Arc};

/// The data set type.
pub struct Dataset<Config, Message = DefaultMessageAction>
where
    Config: DatabaseBuilder,
{
    pub(super) tree: MessageTree<Config::Dmu, Message>,
    pub(super) id: DatasetId,
    name: Box<[u8]>,
    pub(super) open_snapshots: HashSet<Generation>,
    storage_preference: StoragePreference,
}

impl<Config: DatabaseBuilder> Database<Config> {
    fn lookup_dataset_id(&self, name: &[u8]) -> Result<DatasetId> {
        let mut key = Vec::with_capacity(1 + name.len());
        key.push(1);
        key.extend_from_slice(name);
        let data = self.root_tree.get(key)?.ok_or(ErrorKind::DoesNotExist)?;
        Ok(DatasetId::unpack(&data))
    }

    /// A convenience instantiation of [Database::open_custom_dataset] with [DefaultMessageAction].
    pub fn open_dataset(&mut self, name: &[u8]) -> Result<Dataset<Config>> {
        self.open_custom_dataset::<DefaultMessageAction>(name, StoragePreference::NONE)
    }

    /// A convenience instantiation of [Database::create_custom_dataset] with [DefaultMessageAction].
    pub fn create_dataset(&mut self, name: &[u8]) -> Result<()> {
        self.create_custom_dataset::<DefaultMessageAction>(name, StoragePreference::NONE)
    }

    /// A convenience instantiation of [Database::open_or_create_custom_dataset] with [DefaultMessageAction].
    pub fn open_or_create_dataset(&mut self, name: &[u8]) -> Result<Dataset<Config>> {
        self.open_or_create_custom_dataset::<DefaultMessageAction>(name, StoragePreference::NONE)
    }

    /// Opens a data set identified by the given name.
    ///
    /// Fails if the data set does not exist.
    pub fn open_custom_dataset<M: MessageAction + Default + 'static>(
        &mut self,
        name: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<Dataset<Config, M>> {
        let id = self.lookup_dataset_id(name)?;
        let ds_data = fetch_ds_data(&self.root_tree, id)?;
        if self.open_datasets.contains_key(&id) {
            bail!(ErrorKind::InUse)
        }
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

        Ok(Dataset {
            tree: ds_tree.clone(),
            id,
            name: Box::from(name),
            open_snapshots: Default::default(),
            storage_preference,
        })
    }

    /// Creates a new data set identified by the given name.
    ///
    /// Fails if a data set with the same name exists already.
    pub fn create_custom_dataset<M: MessageAction>(
        &mut self,
        name: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<()> {
        match self.lookup_dataset_id(name) {
            Ok(_) => bail!(ErrorKind::AlreadyExists),
            Err(Error(ErrorKind::DoesNotExist, _)) => {}
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

        let key = &ds_data_key(ds_id) as &[_];
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
    pub fn open_or_create_custom_dataset<M: MessageAction + Default + 'static>(
        &mut self,
        name: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<Dataset<Config, M>> {
        match self.lookup_dataset_id(name) {
            Ok(_) => self.open_custom_dataset(name, storage_preference),
            Err(Error(ErrorKind::DoesNotExist, _)) => self
                .create_custom_dataset::<M>(name, storage_preference)
                .and_then(|()| self.open_custom_dataset(name, storage_preference)),
            Err(e) => Err(e),
        }
    }

    fn allocate_ds_id(&mut self) -> Result<DatasetId> {
        let key = &[0u8] as &[_];
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
        let low = &ds_data_key(DatasetId::default()) as &[_];
        let high = &[3u8] as &[_];
        Ok(self.root_tree.range(low..high)?.map(move |result| {
            let (b, _) = result?;
            let len = b.len() as u32;
            Ok(b.slice(1, len - 1))
        }))
    }

    /// Closes the given data set.
    pub fn close_dataset<Message: MessageAction + 'static>(
        &mut self,
        ds: Dataset<Config, Message>,
    ) -> Result<()> {
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

impl<Message: MessageAction + 'static, Config: DatabaseBuilder> Dataset<Config, Message> {
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
    pub fn tree_dump(&self) -> Result<impl serde::Serialize> {
        Ok(self.tree.tree_dump()?)
    }
}

impl<Config: DatabaseBuilder> Dataset<Config, DefaultMessageAction> {
    /// Inserts the given key-value pair.
    ///
    /// Note that any existing value will be overwritten.
    pub fn insert_with_pref<K: Borrow<[u8]> + Into<CowBytes>>(
        &self,
        key: K,
        data: &[u8],
        storage_preference: StoragePreference,
    ) -> Result<()> {
        ensure!(
            data.len() <= tree::MAX_MESSAGE_SIZE,
            ErrorKind::MessageTooLarge
        );
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
        ensure!(
            offset as usize + data.len() <= tree::MAX_MESSAGE_SIZE,
            ErrorKind::MessageTooLarge
        );
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
    ) -> Result<()> {
        use crate::{data_management::DmlWithSpl, storage_pool::StoragePoolLayer};
        if self.tree.dmu().spl().disk_count(pref.as_u8()) == 0 {
            bail!(ErrorKind::DoesNotExist)
        }
        // TODO: What happens on none existent keys? They should not be inserted in this case. Check!
        self.insert_msg_with_pref(key, DefaultMessageAction::noop_msg(), pref)
    }

    /// Deletes the key-value pair if existing.
    pub fn delete<K: Borrow<[u8]> + Into<CowBytes>>(&self, key: K) -> Result<()> {
        self.insert_msg_with_pref(
            key,
            DefaultMessageAction::delete_msg(),
            StoragePreference::NONE,
        )
    }

    /// Removes all key-value pairs in the given key range.
    pub fn range_delete<R, K>(&self, range: R) -> Result<()>
    where
        R: RangeBounds<K>,
        K: Borrow<[u8]> + Into<CowBytes>,
    {
        let mut res = Ok(());

        for entry in self.tree.range(range)? {
            if let Ok((k, _v)) = entry {
                // keep going even on errors, return earliest Err
                let del_res = self.delete(k);
                if del_res.is_err() && res.is_ok() {
                    res = del_res;
                }
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
        for entry in self.tree.range(range)? {
            if let Ok((k, _v)) = entry {
                // abort on errors, they will likely be that one layer is full
                self.migrate(k, pref)?;
            }
        }
        Ok(())
    }
}
