//! Implementation of the [NVMLeafNode] node type.
use crate::{
    buffer::Buf,
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::HasStoragePreference,
    database::RootSpu,
    size::{Size, StaticSize},
    storage_pool::{AtomicSystemStoragePreference, DiskOffset, StoragePoolLayer},
    tree::{imp::packed, pivot_key::LocalPivotKey, KeyInfo, MessageAction},
    vdev::{Block, BLOCK_SIZE},
    AtomicStoragePreference, StoragePreference,
};
use std::{
    borrow::Borrow,
    cell::OnceCell,
    collections::BTreeMap,
    iter::FromIterator,
    mem::size_of,
    sync::{Arc, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use itertools::Itertools;
use rkyv::{
    archived_root,
    ser::{
        serializers::{AllocSerializer, CoreSerializer},
        ScratchSpace, Serializer,
    },
    vec::{ArchivedVec, VecResolver},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
    Archive, Archived, Deserialize, Fallible, Infallible, Serialize,
};

use super::node::NODE_PREFIX_LEN;

pub(crate) const NVMLEAF_METADATA_LEN_OFFSET: usize = 0;
pub(crate) const NVMLEAF_DATA_LEN_OFFSET: usize = size_of::<u32>();
pub(crate) const NVMLEAF_METADATA_OFFSET: usize = NVMLEAF_DATA_LEN_OFFSET + size_of::<u32>();
pub(crate) const NVMLEAF_HEADER_FIXED_LEN: usize = NVMLEAF_METADATA_OFFSET;
const NVMLEAF_PER_KEY_META_LEN: usize = 3 * size_of::<u32>();

pub(super) struct NVMLeafNodeLoadDetails {
    pub need_to_load_data_from_nvm: bool,
    pub time_for_nvm_last_fetch: SystemTime,
    pub nvm_fetch_counter: usize,
}

// Enable actual zero-copy at all? All data is copied twice at the moment, we
// could hold a variant which holds the original buffer and simply returns
// slices to this buffer.
#[derive(Clone)]
pub(super) struct NVMLeafNode {
    pub pool: Option<RootSpu>,
    pub disk_offset: Option<DiskOffset>,
    // NOTE: Use for now, non-blocking would be nicer.
    pub state: NVMLeafNodeState,
    pub meta_data: NVMLeafNodeMetaData,
    //pub data: NVMLeafNodeData,
    pub meta_data_size: usize,
    pub data_size: usize,
    pub data_start: usize,
    pub data_end: usize,
    pub node_size: crate::vdev::Block<u32>,
    pub checksum: Option<crate::checksum::XxHash>,
    pub nvm_load_details: std::sync::Arc<std::sync::RwLock<NVMLeafNodeLoadDetails>>,
}

#[derive(Clone, Debug)]
/// A NVMLeaf can have different states depending on how much data has actually
/// been loaded from disk. Or if this data is already deserialized and copied
/// again to another memory buffer. The latter is most important for NVM.
pub enum NVMLeafNodeState {
    /// State in which a node is allowed to access the memory range independly
    /// but does not guarantee that all keys are present in the memory
    /// structure. Zero-copy possible. This state does _not_ support insertions.
    ///
    /// After one or more accesses the data is mirrored to memory.
    ///
    /// This state may hold k keys with { k | 0 <= k < n } if k == n the state
    /// _must_ transition to the Deserialized state. This is essentially lazy
    /// deserialization.
    PartiallyLoaded {
        buf: &'static [u8],
        // Construct with empty cells while reading metadata? Saves locking of
        // nodes when multiple keys are fetched from the same node, for example
        // when prefetching keys in an object. We should test if this in-node
        // parallelism brings some advantages.
        //
        // TODO: Fetch keys initially in serial manner.
        data: BTreeMap<CowBytes, (usize, OnceLock<(KeyInfo, SlicedCowBytes)>)>,
    },
    /// Only from this state a node may be serialized again.
    Deserialized { data: NVMLeafNodeData },
}

use thiserror::Error;

#[derive(Error, Debug)]
pub enum NVMLeafError {
    #[error(
        "NVMLeafNode attempted an invalid transition to fully deserialized while some keys are not present in memory."
    )]
    AttemptedInvalidTransition,
    #[error("NVMLeafNode attempted to transition from deserialized to deserialized.")]
    AlreadyDeserialized,
}

impl NVMLeafNodeState {
    /// Transition a node from "partially in memory" to "deserialized".
    pub fn upgrade(&mut self) -> Result<(), NVMLeafError> {
        match self {
            NVMLeafNodeState::PartiallyLoaded { buf, data } => {
                if data.iter().filter(|x| x.1 .1.get().is_some()).count() < data.len() {
                    return Err(NVMLeafError::AttemptedInvalidTransition);
                }
                // NOTE: Empty BTreeMaps don't induce any allocations so that is cheap.
                let data = std::mem::replace(data, BTreeMap::new());
                std::mem::replace(
                    self,
                    NVMLeafNodeState::Deserialized {
                        data: NVMLeafNodeData {
                            entries: BTreeMap::from_iter(
                                data.into_iter().map(|mut e| (e.0, e.1 .1.take().unwrap())),
                            ),
                        },
                    },
                );
                Ok(())
            }
            NVMLeafNodeState::Deserialized { data } => Err(NVMLeafError::AlreadyDeserialized),
        }
    }

    /// Transition a node from "partially in memory" to "deserialized" fetching
    /// not present entries if necessary.
    pub fn force_upgrade(&mut self) {
        self.fetch();
        let err = if let Err(e) = self.upgrade() {
            match e {
                NVMLeafError::AttemptedInvalidTransition => Err(e),
                NVMLeafError::AlreadyDeserialized => Ok(()),
            }
        } else {
            Ok(())
        };
        err.unwrap()
    }

    /// Deserialize all entries from the underlying storage. This can bring
    /// advantages when fetching entries multiple times.
    ///
    /// Note: This does not perform the transition to the "deserialized" state.
    pub fn fetch(&self) {
        match self {
            NVMLeafNodeState::PartiallyLoaded { data, .. } => {
                for (k, _) in data.iter() {
                    let _ = self.get(k);
                }
            }
            NVMLeafNodeState::Deserialized { .. } => {
                return;
            }
        }
    }

    /// Returns an entry if it is present. This includes memory *and* disk
    /// storage. Memory is always preferred.
    pub fn get(&self, key: &[u8]) -> Option<&(KeyInfo, SlicedCowBytes)> {
        match self {
            NVMLeafNodeState::PartiallyLoaded { buf, data } => {
                data.get(key).and_then(|e| {
                    Some(e.1.get_or_init(|| {
                        // FIXME: Replace this entire part with simple offsets?
                        let archivedleafnodedata: &ArchivedNVMLeafNodeData =
                            unsafe { rkyv::archived_root::<NVMLeafNodeData>(buf) };
                        archivedleafnodedata
                            .entries
                            .get(e.0)
                            .map(|d| {
                                // FIXME: At best we avoid this copy too, but due to
                                // the return types in the block tree this copy is
                                // necessary. It's also two copies due to rkyv when
                                // not relying on internal device caching of
                                // adjacent chunks.
                                d.value.deserialize(&mut Infallible).unwrap()
                            })
                            .unwrap()
                    }))
                })
            }
            NVMLeafNodeState::Deserialized { data } => data.entries.get(key),
        }
    }

    /// Returns an entry if it is located in memory.
    pub fn get_from_cache(&self, key: &[u8]) -> Option<&(KeyInfo, SlicedCowBytes)> {
        match self {
            NVMLeafNodeState::PartiallyLoaded { data, .. } => data.get(key).and_then(|e| e.1.get()),
            NVMLeafNodeState::Deserialized { data } => data.entries.get(key),
        }
    }

    pub fn insert(
        &mut self,
        key: CowBytes,
        val: (KeyInfo, SlicedCowBytes),
    ) -> Option<(KeyInfo, SlicedCowBytes)> {
        match self {
            NVMLeafNodeState::PartiallyLoaded { .. } => unimplemented!(),
            NVMLeafNodeState::Deserialized { data } => data.entries.insert(key, val),
        }
    }

    /// Iterate over all key value pairs.
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&CowBytes, &(KeyInfo, SlicedCowBytes))> + DoubleEndedIterator {
        match self {
            NVMLeafNodeState::PartiallyLoaded { .. } => todo!(),
            NVMLeafNodeState::Deserialized { data } => data.entries.iter(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            NVMLeafNodeState::PartiallyLoaded { data, .. } => data.len(),
            NVMLeafNodeState::Deserialized { data } => data.entries.len(),
        }
    }

    /// Access the underlying the BTree, only valid in the context of deserialized state.
    pub fn force_entries(&mut self) -> &mut BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)> {
        match self {
            NVMLeafNodeState::PartiallyLoaded { .. } => unimplemented!(),
            NVMLeafNodeState::Deserialized { data } => &mut data.entries,
        }
    }

    /// Access the internal data representation. Panics if node not entirely deserialized.
    pub fn force_data(&self) -> &NVMLeafNodeData {
        match self {
            NVMLeafNodeState::PartiallyLoaded { .. } => unreachable!(),
            NVMLeafNodeState::Deserialized { data } => data,
        }
    }

    /// Create a new deserialized empty state.
    pub fn new() -> Self {
        Self::Deserialized {
            data: NVMLeafNodeData {
                entries: BTreeMap::new(),
            },
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[cfg_attr(test, derive(PartialEq))]
pub(super) struct NVMLeafNodeMetaData {
    pub storage_preference: AtomicStoragePreference,
    /// A storage preference assigned by the Migration Policy
    pub system_storage_preference: AtomicSystemStoragePreference,
    pub entries_size: usize,
}

impl StaticSize for NVMLeafNodeMetaData {
    fn static_size() -> usize {
        // pref             sys pref            entries size   FIXME  ARCHIVE OVERHEAD
        size_of::<u8>() + size_of::<u8>() + size_of::<u32>() + 2
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[cfg_attr(test, derive(PartialEq))]

pub struct NVMLeafNodeData {
    #[with(rkyv::with::AsVec)]
    pub entries: BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>,
}

impl std::fmt::Debug for NVMLeafNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self.state)
    }
}

/// Case-dependent outcome of a rebalance operation.
#[derive(Debug)]
pub(super) enum NVMFillUpResult {
    Rebalanced {
        pivot_key: CowBytes,
        size_delta: isize,
    },
    Merged {
        size_delta: isize,
    },
}

impl Size for NVMLeafNode {
    fn size(&self) -> usize {
        NVMLEAF_HEADER_FIXED_LEN + NVMLeafNodeMetaData::static_size() + self.meta_data.entries_size
    }

    fn actual_size(&self) -> Option<usize> {
        let data_size: usize = self
            .state
            .iter()
            .map(|(_k, (info, v))| v.len() + info.size())
            .sum();

        let key_size: usize = self
            .state
            .iter()
            .map(|(k, _)| NVMLEAF_PER_KEY_META_LEN + k.len())
            .sum();

        let size =
            NVMLEAF_HEADER_FIXED_LEN + NVMLeafNodeMetaData::static_size() + data_size + key_size;
        Some(size)
    }
}

impl HasStoragePreference for NVMLeafNode {
    fn current_preference(&self) -> Option<StoragePreference> {
        self.meta_data
            .storage_preference
            .as_option()
            .map(|pref| self.meta_data.system_storage_preference.weak_bound(&pref))
    }

    fn recalculate(&self) -> StoragePreference {
        let mut pref = StoragePreference::NONE;

        for (keyinfo, _v) in self.state.iter().map(|e| e.1) {
            pref.upgrade(keyinfo.storage_preference);
        }

        self.meta_data.storage_preference.set(pref);
        self.meta_data.system_storage_preference.weak_bound(&pref)
    }

    fn system_storage_preference(&self) -> StoragePreference {
        self.meta_data.system_storage_preference.borrow().into()
    }

    fn set_system_storage_preference(&mut self, pref: StoragePreference) {
        self.meta_data.system_storage_preference.set(pref)
    }
}

impl<'a> FromIterator<(&'a [u8], (KeyInfo, SlicedCowBytes))> for NVMLeafNode {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (&'a [u8], (KeyInfo, SlicedCowBytes))>,
    {
        let mut storage_pref = StoragePreference::NONE;
        let mut entries_size = 0;

        let mut entries = BTreeMap::new();
        let mut needs_second_pass = false;

        for (key, (keyinfo, value)) in iter.into_iter() {
            // pref of overall node is highest pref from keys.
            // We're already looking at every entry here, so finding the overall pref here
            // avoids a full scan later.
            storage_pref.upgrade(keyinfo.storage_preference);
            entries_size += key.len() + NVMLEAF_PER_KEY_META_LEN + value.len() + keyinfo.size();

            let curr_storage_pref = keyinfo.storage_preference;
            if let Some((ckeyinfo, cvalue)) = entries.insert(CowBytes::from(key), (keyinfo, value))
            {
                // iterator has collisions, try to compensate
                //
                // this entry will no longer be part of the final map, subtract its size
                entries_size -=
                    key.len() + NVMLEAF_PER_KEY_META_LEN + cvalue.len() + ckeyinfo.size();

                // In case the old value increased the overall storage priority (faster), and the new
                // value wouldn't have increased it as much, we might need to recalculate the
                // proper preference in a second pass.
                if ckeyinfo.storage_preference != curr_storage_pref {
                    needs_second_pass = true;
                }
            }
        }

        if needs_second_pass {
            storage_pref = StoragePreference::NONE;
            for (keyinfo, _value) in entries.values() {
                storage_pref.upgrade(keyinfo.storage_preference);
            }
        }

        NVMLeafNode {
            pool: None,
            disk_offset: None,
            meta_data: NVMLeafNodeMetaData {
                storage_preference: AtomicStoragePreference::known(storage_pref),
                system_storage_preference: AtomicSystemStoragePreference::from(
                    StoragePreference::NONE,
                ),
                entries_size,
            },
            state: NVMLeafNodeState::Deserialized {
                data: NVMLeafNodeData { entries },
            },
            meta_data_size: 0,
            data_size: 0,
            data_start: 0,
            data_end: 0,
            node_size: crate::vdev::Block(0),
            checksum: None,
            nvm_load_details: std::sync::Arc::new(std::sync::RwLock::new(NVMLeafNodeLoadDetails {
                need_to_load_data_from_nvm: false,
                time_for_nvm_last_fetch: SystemTime::UNIX_EPOCH,
                nvm_fetch_counter: 0,
            })),
        }
    }
}

impl NVMLeafNode {
    /// Constructs a new, empty `NVMLeafNode`.
    pub fn new() -> Self {
        NVMLeafNode {
            pool: None,
            disk_offset: None,
            meta_data: NVMLeafNodeMetaData {
                storage_preference: AtomicStoragePreference::known(StoragePreference::NONE),
                system_storage_preference: AtomicSystemStoragePreference::from(
                    StoragePreference::NONE,
                ),
                entries_size: 0,
            },
            state: NVMLeafNodeState::new(),
            meta_data_size: 0,
            data_size: 0,
            data_start: 0,
            data_end: 0,
            node_size: crate::vdev::Block(0),
            checksum: None,
            nvm_load_details: std::sync::Arc::new(std::sync::RwLock::new(NVMLeafNodeLoadDetails {
                need_to_load_data_from_nvm: false,
                time_for_nvm_last_fetch: SystemTime::UNIX_EPOCH,
                nvm_fetch_counter: 0,
            })),
        }
    }

    pub fn pack<W: std::io::Write>(
        &self,
        mut writer: W,
        metadata_size: &mut usize,
    ) -> Result<(), std::io::Error> {
        let mut serializer_meta_data = rkyv::ser::serializers::AllocSerializer::<0>::default();
        serializer_meta_data
            .serialize_value(&self.meta_data)
            .unwrap();
        let bytes_meta_data = serializer_meta_data.into_serializer().into_inner();

        let mut bytes_pivots: Vec<u8> = vec![];
        let mut data_entry_offset = 0;
        // TODO: Inefficient wire format these are 12 bytes extra for each and every entry
        // Also avoid redundant copies... directly to writer
        for (key, (_, val)) in self.state.force_data().entries.iter() {
            bytes_pivots.extend_from_slice(&(key.len() as u32).to_le_bytes());
            bytes_pivots.extend_from_slice(&(data_entry_offset as u32).to_le_bytes());
            bytes_pivots.extend_from_slice(&(val.len() as u32).to_le_bytes());
            bytes_pivots.extend_from_slice(key);
            data_entry_offset += KeyInfo::static_size() + val.len();
        }

        let meta_len = (bytes_meta_data.len() as u32 + bytes_pivots.len() as u32).to_le_bytes();
        let data_len: usize = self
            .state
            .iter()
            .map(|(_, (info, val))| info.size() + val.len())
            .sum();
        writer.write_all(meta_len.as_ref())?;
        writer.write_all(&(data_len as u32).to_le_bytes())?;
        writer.write_all(&bytes_meta_data.as_ref())?;
        writer.write_all(&bytes_pivots)?;

        for (_, (info, val)) in self.state.force_data().entries.iter() {
            writer.write_all(&info.storage_preference.as_u8().to_le_bytes())?;
            writer.write_all(&val)?;
        }

        *metadata_size = NVMLEAF_METADATA_OFFSET + bytes_meta_data.len();

        debug!("NVMLeaf node packed successfully");
        Ok(())
    }

    pub fn unpack(
        data: &[u8],
        pool: RootSpu,
        offset: DiskOffset,
        checksum: crate::checksum::XxHash,
        size: Block<u32>,
    ) -> Result<Self, std::io::Error> {
        let meta_data_len: usize = u32::from_le_bytes(
            data[NVMLEAF_METADATA_LEN_OFFSET..NVMLEAF_DATA_LEN_OFFSET]
                .try_into()
                .unwrap(),
        ) as usize;
        let data_len: usize = u32::from_le_bytes(
            data[NVMLEAF_DATA_LEN_OFFSET..NVMLEAF_METADATA_OFFSET]
                .try_into()
                .unwrap(),
        ) as usize;
        let meta_data_end = NVMLEAF_METADATA_OFFSET + meta_data_len;
        let data_start = meta_data_end;
        let data_end = data_start + data_len;

        let archivedleafnodemetadata = rkyv::check_archived_root::<NVMLeafNodeMetaData>(
            &data[NVMLEAF_METADATA_OFFSET
                ..NVMLEAF_METADATA_OFFSET + NVMLeafNodeMetaData::static_size()],
        )
        .unwrap();
        let meta_data: NVMLeafNodeMetaData = archivedleafnodemetadata
            .deserialize(&mut rkyv::de::deserializers::SharedDeserializeMap::new())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Read in keys, format: len key len key ...
        let keys = {
            let mut ks = vec![];
            let mut off = NVMLEAF_METADATA_OFFSET + NVMLeafNodeMetaData::static_size();
            let mut total = 0;
            while off < meta_data_end {
                let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                let entry_offset =
                    u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                let val_len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                ks.push((
                    total,
                    entry_offset,
                    val_len,
                    CowBytes::from(&data[off..off + len]),
                ));
                off += len;
                total += 1;
            }
            ks
        };

        #[cfg(not(test))]
        // Fetch the slice location where data is located.
        let compressed_data = pool
            .slice(
                offset,
                data_start + NODE_PREFIX_LEN,
                data_end + NODE_PREFIX_LEN,
            )
            .unwrap();

        #[cfg(test)]
        let compressed_data = &[];

        Ok(NVMLeafNode {
            pool: Some(pool),
            disk_offset: Some(offset),
            meta_data,
            state: NVMLeafNodeState::PartiallyLoaded {
                buf: compressed_data,
                data: keys
                    .into_iter()
                    .map(|(idx, entry_offset, val_len, key)| (key, (idx, OnceLock::new())))
                    .collect(),
            },
            meta_data_size: meta_data_len,
            data_size: data_len,
            data_start,
            data_end,
            node_size: size,
            checksum: Some(checksum),
            nvm_load_details: std::sync::Arc::new(std::sync::RwLock::new(NVMLeafNodeLoadDetails {
                need_to_load_data_from_nvm: true,
                time_for_nvm_last_fetch: SystemTime::now(),
                nvm_fetch_counter: 0,
            })),
        })
    }

    /// Returns the value for the given key.
    pub fn get(&self, key: &[u8]) -> Option<SlicedCowBytes> {
        self.state.get(key).and_then(|o| Some(o.1.clone()))
    }

    pub(in crate::tree) fn get_with_info(&self, key: &[u8]) -> Option<(KeyInfo, SlicedCowBytes)> {
        // FIXME: This is not so nice, maybe adjust get type.
        self.state
            .get(key)
            .and_then(|o| Some((o.0.clone(), o.1.clone())))
    }

    pub fn len(&self) -> usize {
        self.state.len()
    }

    pub(in crate::tree) fn entry_info(&mut self, key: &[u8]) -> Option<&mut KeyInfo> {
        unimplemented!("seems to be an orpahn method!")
        //self.data.write().as_mut().unwrap().as_mut().unwrap().entries.get_mut(key).map(|e| &mut e.0)
    }

    /// Split the node and transfer entries to a given other node `right_sibling`.
    /// Use entries which are, when summed up in-order, above the `min_size` limit.
    /// Returns new pivot key and size delta to the left sibling.
    fn do_split_off(
        &mut self,
        right_sibling: &mut Self,
        min_size: usize,
        max_size: usize,
    ) -> (CowBytes, isize) {
        self.state.force_upgrade();

        debug_assert!(self.size() > max_size);
        debug_assert!(right_sibling.meta_data.entries_size == 0);

        let mut sibling_size = 0;
        let mut sibling_pref = StoragePreference::NONE;
        let mut split_key = None;
        for (k, (keyinfo, v)) in self.state.iter().rev() {
            let size_delta = k.len() + NVMLEAF_PER_KEY_META_LEN + v.len() + KeyInfo::static_size();
            sibling_size += size_delta;
            sibling_pref.upgrade(keyinfo.storage_preference);

            if sibling_size >= min_size {
                split_key = Some(k.clone());
                break;
            }
        }
        let split_key = split_key.unwrap();

        *right_sibling.state.force_entries() = self.state.force_entries().split_off(&split_key);
        right_sibling.meta_data.entries_size = sibling_size;
        self.meta_data.entries_size -= sibling_size;
        right_sibling.meta_data.storage_preference.set(sibling_pref);

        // have removed many keys from self, no longer certain about own pref, mark invalid
        self.meta_data.storage_preference.invalidate();

        let size_delta = -(sibling_size as isize);

        let pivot_key = self
            .state
            .force_entries()
            .keys()
            .next_back()
            .cloned()
            .unwrap();
        (pivot_key, size_delta)
    }

    pub fn apply<K>(&mut self, key: K, pref: StoragePreference) -> Option<KeyInfo>
    where
        K: Borrow<[u8]>,
    {
        // FIXME: Make the KeyInfo atomic so that query speed is not afflicted.
        unimplemented!();
        // self.meta_data.storage_preference.invalidate();
    }

    /// Inserts a new message as leaf entry.
    pub fn insert<Q, M>(
        &mut self,
        key: Q,
        keyinfo: KeyInfo,
        msg: SlicedCowBytes,
        msg_action: M,
    ) -> isize
    where
        Q: Borrow<[u8]> + Into<CowBytes>,
        M: MessageAction,
    {
        self.state.force_upgrade();

        let size_before = self.meta_data.entries_size as isize;
        let key_size = key.borrow().len();
        let mut data = self.get(key.borrow());
        msg_action.apply_to_leaf(key.borrow(), msg, &mut data);

        if let Some(data) = data {
            // Value was added or preserved by msg
            self.meta_data.entries_size += data.len();
            self.meta_data
                .storage_preference
                .upgrade(keyinfo.storage_preference);

            if let Some((old_info, old_data)) =
                self.state.insert(key.into(), (keyinfo.clone(), data))
            {
                // There was a previous value in entries, which was now replaced
                self.meta_data.entries_size -= old_data.len();

                // if previous entry was stricter than new entry, invalidate
                if old_info.storage_preference < keyinfo.storage_preference {
                    self.meta_data.storage_preference.invalidate();
                }
            } else {
                // There was no previous value in entries
                self.meta_data.entries_size +=
                    key_size + NVMLEAF_PER_KEY_META_LEN + KeyInfo::static_size();
            }
        } else if let Some((old_info, old_data)) = self.state.force_entries().remove(key.borrow()) {
            // The value was removed by msg, this may be a downgrade opportunity.
            // The preference of the removed entry can't be stricter than the current node
            // preference, by invariant. That leaves "less strict" and "as strict" as the
            // node preference:
            //
            // - less strict:
            //     If the preference of the removed entry is less strict than the current
            //     node preference, there must be another entry which is preventing a downgrade.
            // - as strict:
            //     The removed entry _may_ have caused the original upgrade to this preference,
            //     we'll have to trigger a scan to find out.
            if self.meta_data.storage_preference.as_option() == Some(old_info.storage_preference) {
                self.meta_data.storage_preference.invalidate();
            }

            self.meta_data.entries_size -= key_size + NVMLEAF_PER_KEY_META_LEN;
            self.meta_data.entries_size -= old_data.len() + KeyInfo::static_size();
        }
        self.meta_data.entries_size as isize - size_before
    }

    /// Inserts messages as leaf entries.
    pub fn insert_msg_buffer<M, I>(&mut self, msg_buffer: I, msg_action: M) -> isize
    where
        M: MessageAction,
        I: IntoIterator<Item = (CowBytes, (KeyInfo, SlicedCowBytes))>,
    {
        let mut size_delta = 0;
        for (key, (keyinfo, msg)) in msg_buffer {
            size_delta += self.insert(key, keyinfo, msg, &msg_action);
        }
        size_delta
    }

    /// Splits this `NVMLeafNode` into to two leaf nodes.
    /// Returns a new right sibling, the corresponding pivot key, and the size
    /// delta of this node.
    pub fn split(
        &mut self,
        min_size: usize,
        max_size: usize,
    ) -> (Self, CowBytes, isize, LocalPivotKey) {
        // assert!(self.size() > S::MAX);
        let mut right_sibling = NVMLeafNode {
            pool: None,
            disk_offset: None,
            // During a split, preference can't be inherited because the new subset of entries
            // might be a subset with a lower maximal preference.
            meta_data: NVMLeafNodeMetaData {
                storage_preference: AtomicStoragePreference::known(StoragePreference::NONE),
                system_storage_preference: AtomicSystemStoragePreference::from(
                    StoragePreference::NONE,
                ),
                entries_size: 0,
            },
            state: NVMLeafNodeState::new(),
            meta_data_size: 0,
            data_size: 0,
            data_start: 0,
            data_end: 0,
            node_size: crate::vdev::Block(0),
            checksum: None,
            nvm_load_details: std::sync::Arc::new(std::sync::RwLock::new(NVMLeafNodeLoadDetails {
                need_to_load_data_from_nvm: false,
                time_for_nvm_last_fetch: SystemTime::UNIX_EPOCH,
                nvm_fetch_counter: 0,
            })),
        };

        // This adjusts sibling's size and pref according to its new entries
        let (pivot_key, size_delta) = self.do_split_off(&mut right_sibling, min_size, max_size);

        (
            right_sibling,
            pivot_key.clone(),
            size_delta,
            LocalPivotKey::Right(pivot_key),
        )
    }

    /// Create an iterator over all entries.
    /// FIXME: This also fetches entries which are not required, maybe implement special iterator for that.
    pub fn range(&self) -> impl Iterator<Item = (&CowBytes, &(KeyInfo, SlicedCowBytes))> {
        self.state.fetch();
        self.state.iter()
    }

    /// Merge all entries from the *right* node into the *left* node.  Returns
    /// the size change, positive for the left node, negative for the right
    /// node.
    pub fn merge(&mut self, right_sibling: &mut Self) -> isize {
        self.state.force_upgrade();
        right_sibling.state.force_upgrade();
        self.state
            .force_entries()
            .append(&mut right_sibling.state.force_entries());
        let size_delta = right_sibling.meta_data.entries_size;
        self.meta_data.entries_size += right_sibling.meta_data.entries_size;

        self.meta_data
            .storage_preference
            .upgrade_atomic(&right_sibling.meta_data.storage_preference);

        // right_sibling is now empty, reset to defaults
        right_sibling.meta_data.entries_size = 0;
        right_sibling
            .meta_data
            .storage_preference
            .set(StoragePreference::NONE);

        size_delta as isize
    }

    /// Rebalances `self` and `right_sibling`. Returns `Merged`
    /// if all entries of `right_sibling` have been merged into this node.
    /// Otherwise, returns a new pivot key.
    pub fn rebalance(
        &mut self,
        right_sibling: &mut Self,
        min_size: usize,
        max_size: usize,
    ) -> NVMFillUpResult {
        let size_delta = self.merge(right_sibling);
        if self.size() <= max_size {
            NVMFillUpResult::Merged { size_delta }
        } else {
            // First size_delta is from the merge operation where we split
            let (pivot_key, split_size_delta) =
                self.do_split_off(right_sibling, min_size, max_size);
            NVMFillUpResult::Rebalanced {
                pivot_key,
                size_delta: size_delta + split_size_delta,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CowBytes, NVMLeafNode, Size};
    use crate::{
        arbitrary::GenExt,
        checksum::{Builder, State, XxHashBuilder},
        data_management::HasStoragePreference,
        storage_pool::{DiskOffset, StoragePoolLayer},
        tree::{
            default_message_action::{DefaultMessageAction, DefaultMessageActionMsg},
            KeyInfo,
        },
        StoragePoolConfiguration,
    };

    use quickcheck::{Arbitrary, Gen, TestResult};
    use rand::Rng;
    /*
    impl Arbitrary for KeyInfo {
        fn arbitrary(g: &mut Gen) -> Self {
            let sp = g.rng().gen_range(0..=3);
            KeyInfo {
                storage_preference: StoragePreference::from_u8(sp),
            }
        }
    }
    */
    impl Arbitrary for NVMLeafNode {
        fn arbitrary(g: &mut Gen) -> Self {
            let len = g.rng().gen_range(0..20);
            let entries: Vec<_> = (0..len)
                .map(|_| {
                    (
                        CowBytes::arbitrary(g),
                        DefaultMessageActionMsg::arbitrary(g),
                    )
                })
                .map(|(k, v)| (k, v.0))
                .collect();

            let node: NVMLeafNode = entries
                .iter()
                .map(|(k, v)| (&k[..], (KeyInfo::arbitrary(g), v.clone())))
                .collect();
            node.recalculate();
            node
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            let v: Vec<_> = self
                .state
                .force_data()
                .entries
                .iter()
                .map(|(k, (info, v))| (k.clone(), (info.clone(), CowBytes::from(v.to_vec()))))
                .collect();
            Box::new(v.shrink().map(|entries| {
                entries
                    .iter()
                    .map(|(k, (info, v))| (&k[..], (info.clone(), v.clone().into())))
                    .collect()
            }))
        }
    }

    fn serialized_size(leaf: &NVMLeafNode) -> usize {
        let mut w = vec![];
        let mut m_size = 0;
        leaf.pack(&mut w, &mut m_size).unwrap();
        w.len()
    }

    #[quickcheck]
    fn check_actual_size(leaf_node: NVMLeafNode) {
        assert_eq!(leaf_node.actual_size(), Some(serialized_size(&leaf_node)));
    }

    #[quickcheck]
    fn check_size(leaf_node: NVMLeafNode) {
        let size = leaf_node.size();
        let serialized = serialized_size(&leaf_node);
        if size != serialized {
            eprintln!(
                "leaf {:?}, size {}, actual_size {:?}, serialized_size {}",
                leaf_node,
                size,
                leaf_node.actual_size(),
                serialized
            );
            assert_eq!(size, serialized);
        }
    }

    #[quickcheck]
    fn check_ser_deser(leaf_node: NVMLeafNode) {
        let mut bytes = vec![];
        let mut metadata_size = 0;
        leaf_node.pack(&mut bytes, &mut metadata_size).unwrap();

        let config = StoragePoolConfiguration::default();
        let pool = crate::database::RootSpu::new(&config).unwrap();
        let csum = XxHashBuilder.build().finish();

        let _node = NVMLeafNode::unpack(
            &bytes,
            pool,
            DiskOffset::from_u64(0),
            csum,
            crate::vdev::Block(4),
        )
        .unwrap();
    }

    #[quickcheck]
    fn check_size_insert(
        mut leaf_node: NVMLeafNode,
        key: CowBytes,
        key_info: KeyInfo,
        msg: DefaultMessageActionMsg,
    ) {
        let size_before = leaf_node.size();
        let size_delta = leaf_node.insert(key, key_info, msg.0, DefaultMessageAction);
        let size_after = leaf_node.size();
        assert_eq!((size_before as isize + size_delta) as usize, size_after);
        assert_eq!(
            serialized_size(&leaf_node),
            leaf_node.actual_size().unwrap()
        );
        assert_eq!(serialized_size(&leaf_node), size_after);
    }

    const MIN_LEAF_SIZE: usize = 512;
    const MAX_LEAF_SIZE: usize = 2048;

    #[quickcheck]
    fn check_size_split(mut leaf_node: NVMLeafNode) -> TestResult {
        let size_before = leaf_node.size();

        if size_before <= MAX_LEAF_SIZE || size_before > MAX_LEAF_SIZE + MIN_LEAF_SIZE {
            return TestResult::discard();
        }

        assert_eq!(serialized_size(&leaf_node), leaf_node.size());
        assert_eq!(
            serialized_size(&leaf_node),
            leaf_node.actual_size().unwrap()
        );
        let (sibling, _split_key, _size_delta, _pivot_key) =
            leaf_node.split(MIN_LEAF_SIZE, MAX_LEAF_SIZE);
        assert_eq!(serialized_size(&leaf_node), leaf_node.size());
        assert_eq!(
            serialized_size(&leaf_node),
            leaf_node.actual_size().unwrap()
        );
        assert_eq!(serialized_size(&sibling), sibling.size());
        assert_eq!(serialized_size(&sibling), sibling.actual_size().unwrap());
        assert!(sibling.size() <= MAX_LEAF_SIZE);
        assert!(sibling.size() >= MIN_LEAF_SIZE);
        assert!(leaf_node.size() >= MIN_LEAF_SIZE);
        assert!(leaf_node.size() <= MAX_LEAF_SIZE);
        TestResult::passed()
    }

    #[quickcheck]
    fn check_split_merge_idempotent(mut leaf_node: NVMLeafNode) -> TestResult {
        if leaf_node.size() <= MAX_LEAF_SIZE {
            return TestResult::discard();
        }
        let this = leaf_node.clone();
        let (mut sibling, ..) = leaf_node.split(MIN_LEAF_SIZE, MAX_LEAF_SIZE);
        leaf_node.recalculate();
        leaf_node.merge(&mut sibling);
        assert_eq!(this.meta_data, leaf_node.meta_data);
        assert_eq!(this.state.force_data(), leaf_node.state.force_data());
        TestResult::passed()
    }
}
