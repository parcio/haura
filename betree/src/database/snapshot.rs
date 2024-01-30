use super::{
    dataset::Dataset, errors::*, fetch_ds_data, fetch_ss_data, root_tree_msg::dataset,
    root_tree_msg::deadlist, root_tree_msg::snapshot, Database, DatasetData, DatasetId,
    DatasetTree, DeadListData, Generation, ObjectPointer, RootDmu,
};
use crate::{
    allocator::Action,
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::DmlWithHandler,
    tree::{DefaultMessageAction, Tree, TreeLayer},
    StoragePreference,
};
use byteorder::{BigEndian, ByteOrder};
use std::{borrow::Borrow, ops::RangeBounds, sync::Arc};

/// The snapshot type.
pub struct Snapshot {
    tree: DatasetTree<RootDmu>,
    #[allow(dead_code)]
    name: Box<[u8]>,
}

impl Database {
    /// Open a snapshot for the given data set identified by the given name.
    pub fn open_snapshot<M>(&self, ds: &mut Dataset<M>, name: &[u8]) -> Result<Snapshot> {
        let id = self.lookup_snapshot_id(ds.id(), name)?;
        if !ds.call_mut_open_snapshots(|set| set.insert(id)) {
            return Err(Error::InUse);
        }
        let ptr = fetch_ss_data(&self.root_tree, ds.id(), id)?.ptr;
        Ok(Snapshot {
            tree: Tree::open(
                ds.id(),
                ptr,
                DefaultMessageAction,
                Arc::clone(self.root_tree.dmu()),
                StoragePreference::NONE,
            ),
            name: Box::from(name),
        })
    }

    fn lookup_snapshot_id(&self, ds_id: DatasetId, name: &[u8]) -> Result<Generation> {
        let key = snapshot::key(ds_id, name);
        let data = self.root_tree.get(key)?.ok_or(Error::DoesNotExist)?;
        Ok(Generation::unpack(&data))
    }

    /// Creates a new snapshot for the given data set identified by the given
    /// name.
    ///
    /// Note that the creation fails if a snapshot with the same name exists
    /// already for the given data set.
    pub fn create_snapshot<M>(&mut self, ds: &mut Dataset<M>, name: &[u8]) -> Result<()> {
        match self.lookup_snapshot_id(ds.id(), name).err() {
            None => return Err(Error::AlreadyExists),
            Some(Error::DoesNotExist) => {}
            Some(e) => return Err(e),
        };

        let data = fetch_ds_data(&self.root_tree, ds.id())?;
        let ss_id = data.ptr.generation();
        let key = &snapshot::data_key(ds.id(), ss_id) as &[_];
        let data = data.pack()?;
        self.root_tree.insert(
            key,
            DefaultMessageAction::insert_msg(&data),
            StoragePreference::NONE,
        )?;
        let key = &dataset::data_key(ds.id()) as &[_];
        self.root_tree.insert(
            key,
            DatasetData::<ObjectPointer>::update_previous_snapshot(Some(ss_id)),
            StoragePreference::NONE,
        )?;
        self.sync()
    }

    /// Iterate over all snapshots for the given data set.
    pub fn iter_snapshots<M>(
        &self,
        ds: &Dataset<M>,
    ) -> Result<impl Iterator<Item = Result<SlicedCowBytes>>> {
        let mut low = [0; 9];
        low[0] = 3;
        BigEndian::write_u64(&mut low[1..], ds.id().0);
        let high = &[4u8] as &[_];
        Ok(self.root_tree.range(&low[..]..high)?.map(|result| {
            let (b, _) = result?;
            let len = b.len() as u32;
            Ok(b.slice(9, len - 9))
        }))
    }

    /// Deletes the snapshot identified by the given name.
    ///
    /// Note that the deletion fails if a snapshot with the given name does not
    /// exist for this data set.
    pub fn delete_snapshot<M>(&self, ds: &mut Dataset<M>, name: &[u8]) -> Result<()> {
        let ss_id = self.lookup_snapshot_id(ds.id(), name)?;
        if ds.call_open_snapshots(|set| set.contains(&ss_id)) {
            return Err(Error::InUse);
        }

        self.root_tree.insert(
            snapshot::key(ds.id(), name),
            DefaultMessageAction::delete_msg(),
            StoragePreference::NONE,
        )?;

        let previous_ss_id = fetch_ss_data(&self.root_tree, ds.id(), ss_id)?.previous_snapshot;
        let update_previous_ss_msg =
            DatasetData::<ObjectPointer>::update_previous_snapshot(previous_ss_id);

        let max_key_snapshot;
        let max_key_dataset;

        let max_key = if let Some(next_ss_id) = self.next_snapshot_id(ds.id(), ss_id)? {
            self.root_tree.insert(
                &snapshot::data_key(ds.id(), next_ss_id) as &[_],
                update_previous_ss_msg,
                StoragePreference::NONE,
            )?;
            max_key_snapshot = deadlist::max_key(ds.id(), next_ss_id);
            &max_key_snapshot as &[_]
        } else {
            self.root_tree.insert(
                &dataset::data_key(ds.id()) as &[_],
                update_previous_ss_msg,
                StoragePreference::NONE,
            )?;
            max_key_dataset = deadlist::max_key_ds(ds.id());
            &max_key_dataset as &[_]
        };
        let min_key = &deadlist::min_key(ds.id(), ss_id.next()) as &[_];

        for result in self.root_tree.range(min_key..max_key)? {
            let (key, value) = result?;
            let entry = DeadListData::unpack(&value)?;
            if previous_ss_id < Some(entry.birth) {
                let offset = deadlist::offset_from_key(&key);
                self.root_tree.dmu().handler().update_allocation_bitmap(
                    offset,
                    entry.size,
                    Action::Deallocate,
                    self.root_tree.dmu(),
                )?;
                self.root_tree.insert(
                    key,
                    DefaultMessageAction::delete_msg(),
                    StoragePreference::NONE,
                )?;
            }
        }

        Ok(())
    }

    fn next_snapshot_id(&self, ds_id: DatasetId, ss_id: Generation) -> Result<Option<Generation>> {
        let low = &snapshot::data_key(ds_id, ss_id.next()) as &[_];
        let high = &snapshot::data_key_max(ds_id) as &[_];
        Ok(
            if let Some(result) = self.root_tree.range(low..high)?.next() {
                let (key, _) = result?;
                Some(Generation::unpack(&key[9..]))
            } else {
                None
            },
        )
    }
}

impl Snapshot {
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
}
