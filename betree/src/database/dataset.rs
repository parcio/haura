use super::{
    ds_data_key, errors::*, fetch_ds_data, Database, DatasetData, DatasetId, DatasetTree,
    Generation, MessageTree,
};
use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::DmlWithHandler,
    database::DatabaseBuilder,
    tree::{self, DefaultMessageAction, MessageAction, Tree, TreeBaseLayer, TreeLayer},
};
use std::{borrow::Borrow, collections::HashSet, ops::RangeBounds, sync::Arc};

/// The data set type.
pub struct Dataset<Config, Message = DefaultMessageAction>
where
    Config: DatabaseBuilder,
{
    tree: MessageTree<Config::Dmu, Message>,
    pub(super) id: DatasetId,
    name: Box<[u8]>,
    pub(super) open_snapshots: HashSet<Generation>,
}

impl<Config: DatabaseBuilder> Database<Config> {
    fn lookup_dataset_id(&self, name: &[u8]) -> Result<DatasetId> {
        let mut key = Vec::with_capacity(1 + name.len());
        key.push(1);
        key.extend_from_slice(name);
        let data = self.root_tree.get(key)?.ok_or(ErrorKind::DoesNotExist)?;
        Ok(DatasetId::unpack(&data))
    }

    pub fn open_dataset(&mut self, name: &[u8]) -> Result<Dataset<Config>> {
        self.open_custom_dataset::<DefaultMessageAction>(name)
    }

    pub fn create_dataset(&mut self, name: &[u8]) -> Result<()> {
        self.create_custom_dataset::<DefaultMessageAction>(name)
    }

    /// Opens a data set identified by the given name.
    ///
    /// Fails if the data set does not exist.
    pub fn open_custom_dataset<M: MessageAction + Default + 'static>(
        &mut self,
        name: &[u8],
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
        );

        if let Some(ss_id) = ds_data.previous_snapshot {
            self.root_tree
                .dmu()
                .handler()
                .last_snapshot_generation
                .write()
                .insert(id, ss_id);
        }
        // FIXME: important!
        let erased_tree = Box::new(ds_tree.clone());
        self.open_datasets.insert(id, erased_tree);

        Ok(Dataset {
            tree: ds_tree.clone(),
            id,
            name: Box::from(name),
            open_snapshots: Default::default(),
        })
    }

    /// Creates a new data set identified by the given name.
    ///
    /// Fails if a data set with the same name exists already.
    pub fn create_custom_dataset<M: MessageAction>(&mut self, name: &[u8]) -> Result<()> {
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
        );
        let ptr = tree.sync()?;

        let key = &ds_data_key(ds_id) as &[_];
        let data = DatasetData {
            ptr,
            previous_snapshot: None,
        }
        .pack()?;
        self.root_tree
            .insert(key, DefaultMessageAction::insert_msg(&data))?;
        let mut key = vec![1];
        key.extend(name);
        self.root_tree
            .insert(key, DefaultMessageAction::insert_msg(&ds_id.pack()))?;
        Ok(())
    }

    /// Opens a dataset, creating a new one if none exists by the given name.
    pub fn open_or_create_dataset<M: MessageAction + Default + 'static>(
        &mut self,
        name: &[u8],
    ) -> Result<Dataset<Config, M>> {
        match self.lookup_dataset_id(name) {
            Ok(_) => self.open_custom_dataset(name),
            Err(Error(ErrorKind::DoesNotExist, _)) => self
                .create_custom_dataset::<M>(name)
                .and_then(|()| self.open_custom_dataset(name)),
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
        self.root_tree
            .insert(key, DefaultMessageAction::insert_msg(data))?;
        Ok(next_ds_id)
    }

    /// Iterates over all data sets in the database.
    pub fn iter_datasets(&self) -> Result<impl Iterator<Item = Result<SlicedCowBytes>>> {
        let low = &ds_data_key(DatasetId::default()) as &[_];
        let high = &[2u8] as &[_];
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
        self.sync_ds(ds.id, &ds.tree)?;
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
        Ok(self.tree.insert(key, msg)?)
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

    /// Removes all key-value pairs in the given key range.
    pub fn range_delete<R, K>(&self, range: R) -> Result<()>
    where
        R: RangeBounds<K>,
        K: Borrow<[u8]> + Into<Box<[u8]>>,
    {
        Ok(self.tree.range_delete(range)?)
    }

    /// Returns the name of the data set.
    pub fn name(&self) -> &[u8] {
        &self.name
    }
}

impl<Config: DatabaseBuilder> Dataset<Config, DefaultMessageAction> {
    /// Inserts the given key-value pair.
    ///
    /// Note that any existing value will be overwritten.
    pub fn insert<K: Borrow<[u8]> + Into<CowBytes>>(&self, key: K, data: &[u8]) -> Result<()> {
        ensure!(
            data.len() <= tree::MAX_MESSAGE_SIZE,
            ErrorKind::MessageTooLarge
        );
        self.insert_msg(key, DefaultMessageAction::insert_msg(data))
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
        ensure!(
            offset as usize + data.len() <= tree::MAX_MESSAGE_SIZE,
            ErrorKind::MessageTooLarge
        );
        self.insert_msg(key, DefaultMessageAction::upsert_msg(offset, data))
    }

    /// Deletes the key-value pair if existing.
    pub fn delete<K: Borrow<[u8]> + Into<CowBytes>>(&self, key: K) -> Result<()> {
        self.insert_msg(key, DefaultMessageAction::delete_msg())
    }
}
