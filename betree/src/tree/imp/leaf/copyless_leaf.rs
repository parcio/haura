//! Implementation of the [NVMLeafNode] node type.
//!
//! FIXME: This node is freely allowed to occupy memory at the moment. This can
//! be bad. At the moment we always assume in the DMU the worst-case (entire
//! node) and are somewhat fine due to that. But a more efficient way would be
//! the propagating size changes to the cache. Although size increases are more
//! difficult to handle than because nodes cannot evict other entries.
use crate::{
    buffer::Buf,
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::{HasStoragePreference, IntegrityMode},
    size::{Size, StaticSize},
    storage_pool::AtomicSystemStoragePreference,
    tree::{pivot_key::LocalPivotKey, KeyInfo, MessageAction},
    AtomicStoragePreference, StoragePreference,
};
use std::{
    borrow::Borrow, collections::BTreeMap, io::Write, iter::FromIterator, mem::size_of, ops::Range,
};

pub(crate) const NVMLEAF_METADATA_LEN_OFFSET: usize = 0;
pub(crate) const NVMLEAF_DATA_LEN_OFFSET: usize = size_of::<u32>();
pub(crate) const NVMLEAF_METADATA_OFFSET: usize = NVMLEAF_DATA_LEN_OFFSET + size_of::<u32>();
pub(crate) const NVMLEAF_HEADER_FIXED_LEN: usize = NVMLEAF_METADATA_OFFSET;
const NVMLEAF_PER_KEY_META_LEN: usize = 3 * size_of::<u32>();

// Enable actual zero-copy at all? All data is copied twice at the moment, we
// could hold a variant which holds the original buffer and simply returns
// slices to this buffer.
#[derive(Clone)]
pub struct CopylessLeaf {
    state: LeafNodeState,
    meta: Meta,
}

#[derive(Clone, Debug)]
/// A Leaf can have different states depending on how much data has actually
/// been loaded from disk. Or if this data is already deserialized and copied
/// again to another memory buffer. The latter is most important for NVM.
enum LeafNodeState {
    /// State in which a node is allowed to access the memory range independly
    /// but does not guarantee that all keys are present in the memory
    /// structure. Zero-copy possible. This state does _not_ support insertions.
    ///
    /// This state may hold k keys with { k | 0 <= k < n } if k == n the state
    /// _must_ transition to the Deserialized state. This is essentially lazy
    /// deserialization.
    PartiallyLoaded {
        buf: SlicedCowBytes,
        // Construct with empty cells while reading metadata? Saves locking of
        // nodes when multiple keys are fetched from the same node, for example
        // when prefetching keys in an object. We should test if this in-node
        // parallelism brings some advantages.
        // data: BTreeMap<CowBytes, (Location, OnceLock<(KeyInfo, SlicedCowBytes)>)>,
        keys: Vec<(CowBytes, Location)>,
    },
    /// Only from this state a node may be serialized again.
    Deserialized {
        data: BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>,
    },
}

#[derive(Clone, Debug)]
struct Location {
    off: u32,
    len: u32,
}

impl Location {
    fn pack<W: Write>(&self, mut w: W) -> Result<(), std::io::Error> {
        w.write_all(&self.off.to_le_bytes())?;
        w.write_all(&self.len.to_le_bytes())
    }

    fn unpack(data: &[u8]) -> Self {
        debug_assert!(data.len() >= 8);
        Location {
            off: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            len: u32::from_le_bytes(data[4..8].try_into().unwrap()),
        }
    }

    fn range(&self) -> Range<usize> {
        self.off as usize..self.off as usize + self.len as usize
    }
}

impl StaticSize for Location {
    fn static_size() -> usize {
        2 * size_of::<u32>()
    }
}

fn unpack_entry(data: &[u8]) -> (KeyInfo, SlicedCowBytes) {
    (KeyInfo::unpack(&data[0..1]), unsafe {
        SlicedCowBytes::from_raw(data[1..].as_ptr(), data[1..].len())
    })
}

fn pack_entry<W: Write>(
    mut w: W,
    info: KeyInfo,
    val: SlicedCowBytes,
) -> Result<(), std::io::Error> {
    info.pack(&mut w)?;
    w.write_all(&val)
}

impl KeyInfo {
    pub fn pack<W: Write>(&self, mut w: W) -> Result<(), std::io::Error> {
        w.write_all(&self.storage_preference.as_u8().to_le_bytes())
    }

    pub fn unpack(data: &[u8]) -> Self {
        KeyInfo {
            storage_preference: StoragePreference::from_u8(u8::from_le_bytes(
                data[0..1].try_into().unwrap(),
            )),
        }
    }
}

use thiserror::Error;

use super::FillUpResult;

#[derive(Error, Debug)]
pub enum CopylessLeafError {
    #[error(
        "CopylessLeaf attempted an invalid transition to fully deserialized while some keys are not present in memory."
    )]
    AttemptedInvalidTransition,
    #[error("CopylessLeaf attempted to transition from deserialized to deserialized.")]
    AlreadyDeserialized,
}

impl LeafNodeState {
    /// Transition a node from "partially in memory" to "deserialized".
    pub fn upgrade(&mut self) -> Result<(), CopylessLeafError> {
        match self {
            LeafNodeState::PartiallyLoaded { keys, buf } => {
                let it = keys
                    .into_iter()
                    .map(|(key, loc)| (key.clone(), unpack_entry(&buf[loc.range()])));

                let other = LeafNodeState::Deserialized {
                    data: BTreeMap::from_iter(it),
                };
                let _ = std::mem::replace(self, other);
                Ok(())
            }
            LeafNodeState::Deserialized { .. } => Err(CopylessLeafError::AlreadyDeserialized),
        }
    }

    /// Transition a node from "partially in memory" to "deserialized" fetching
    /// not present entries if necessary.
    pub fn force_upgrade(&mut self) {
        let err = if let Err(e) = self.upgrade() {
            match e {
                CopylessLeafError::AttemptedInvalidTransition => Err(e),
                CopylessLeafError::AlreadyDeserialized => Ok(()),
            }
        } else {
            Ok(())
        };
        err.unwrap()
    }

    /// Returns an entry if it is present. This includes memory *and* disk
    /// storage. Memory is always preferred.
    pub fn get(&self, key: &[u8]) -> Option<(KeyInfo, SlicedCowBytes)> {
        match self {
            LeafNodeState::PartiallyLoaded { buf, keys } => keys
                .binary_search_by(|e| e.0.as_ref().cmp(key))
                .ok()
                .and_then(|idx| Some(unpack_entry(&buf[keys[idx].1.range()]))),
            LeafNodeState::Deserialized { data } => data.get(key).cloned(),
        }
    }

    /// Insert an new entry into the state. Only valid when executed with a fully deserialized map.
    pub fn insert(
        &mut self,
        key: CowBytes,
        val: (KeyInfo, SlicedCowBytes),
    ) -> Option<(KeyInfo, SlicedCowBytes)> {
        match self {
            LeafNodeState::PartiallyLoaded { .. } => unimplemented!(),
            LeafNodeState::Deserialized { data } => data.insert(key, val),
        }
    }

    /// Iterate over all key value pairs.
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&CowBytes, (KeyInfo, SlicedCowBytes))> + DoubleEndedIterator {
        CopylessIter {
            state: self,
            start: 0,
            end: match self {
                LeafNodeState::PartiallyLoaded { keys, .. } => keys.len(),
                LeafNodeState::Deserialized { data } => data.len(),
            }
            .saturating_sub(1),
        }
    }

    /// Returns the number of entries present in the node.
    pub fn len(&self) -> usize {
        match self {
            LeafNodeState::PartiallyLoaded { keys, .. } => keys.len(),
            LeafNodeState::Deserialized { data } => data.len(),
        }
    }

    /// Access the underlying the BTree, only valid in the context of deserialized state.
    pub fn force_data_mut(&mut self) -> &mut BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)> {
        match self {
            LeafNodeState::PartiallyLoaded { .. } => unimplemented!(),
            LeafNodeState::Deserialized { ref mut data } => data,
        }
    }

    /// Access the internal data representation. Panics if node not entirely deserialized.
    pub fn force_data(&self) -> &BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)> {
        match self {
            LeafNodeState::PartiallyLoaded { .. } => unreachable!(),
            LeafNodeState::Deserialized { data } => data,
        }
    }

    /// Create a new deserialized empty state.
    pub fn new() -> Self {
        Self::Deserialized {
            data: BTreeMap::new(),
        }
    }

    #[cfg(test)]
    pub fn set_data(&mut self, data: SlicedCowBytes) {
        match self {
            LeafNodeState::PartiallyLoaded { ref mut buf, .. } => *buf = data,
            LeafNodeState::Deserialized { .. } => {
                panic!("Set data on deserialized copyless leaf state.")
            }
        }
    }
}

pub struct CopylessIter<'a> {
    state: &'a LeafNodeState,
    start: usize,
    end: usize,
}

impl<'a> Iterator for CopylessIter<'a> {
    type Item = (&'a CowBytes, (KeyInfo, SlicedCowBytes));

    fn next(&mut self) -> Option<Self::Item> {
        if self.start >= self.end {
            return None;
        }
        let res = match self.state {
            LeafNodeState::PartiallyLoaded { buf, keys } => keys
                .get(self.start)
                .map(|(key, loc)| (key, unpack_entry(&buf[loc.range()]))),
            LeafNodeState::Deserialized { data } => data
                .iter()
                .nth(self.start)
                .map(|(key, (info, val))| (key, (info.clone(), val.clone()))),
        };
        self.start += 1;
        res
    }
}

impl<'a> DoubleEndedIterator for CopylessIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.end <= self.start {
            return None;
        }
        let res = match self.state {
            LeafNodeState::PartiallyLoaded { buf, keys } => keys
                .get(self.end)
                .map(|(key, loc)| (key, unpack_entry(&buf[loc.range()]))),
            LeafNodeState::Deserialized { data } => data
                .iter()
                .nth(self.end)
                .map(|(key, (info, val))| (key, (info.clone(), val.clone()))),
        };
        self.end -= 1;
        res
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub(super) struct Meta {
    pub storage_preference: AtomicStoragePreference,
    /// A storage preference assigned by the Migration Policy
    pub system_storage_preference: AtomicSystemStoragePreference,
    pub entries_size: usize,
}

impl Meta {
    pub fn pack<W: Write>(&self, mut w: W) -> Result<(), std::io::Error> {
        w.write_all(
            &self
                .storage_preference
                .as_option()
                .unwrap_or(StoragePreference::NONE)
                .as_u8()
                .to_le_bytes(),
        )?;
        w.write_all(
            &self
                .system_storage_preference
                .strong_bound(&StoragePreference::NONE)
                .as_u8()
                .to_le_bytes(),
        )?;
        w.write_all(&(self.entries_size as u32).to_le_bytes())
    }

    pub fn unpack(data: &[u8]) -> Self {
        let pref: StoragePreference =
            StoragePreference::from_u8(u8::from_le_bytes(data[0..1].try_into().unwrap()));
        let sys_pref: StoragePreference =
            StoragePreference::from_u8(u8::from_le_bytes(data[1..2].try_into().unwrap()));
        Self {
            storage_preference: AtomicStoragePreference::known(pref),
            system_storage_preference: sys_pref.into(),
            entries_size: u32::from_le_bytes(data[2..2 + 4].try_into().unwrap()) as usize,
        }
    }
}

impl StaticSize for Meta {
    fn static_size() -> usize {
        // pref             sys pref            entries size
        size_of::<u8>() + size_of::<u8>() + size_of::<u32>()
    }
}

impl std::fmt::Debug for CopylessLeaf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self.state)
    }
}

impl Size for CopylessLeaf {
    fn size(&self) -> usize {
        NVMLEAF_HEADER_FIXED_LEN + Meta::static_size() + self.meta.entries_size
    }

    fn actual_size(&self) -> Option<usize> {
        // let (data_size, key_size) = self.state.iter().fold((0, 0), |acc, (k, (info, v))| {
        //     (
        //         acc.0 + v.len() + info.size(),
        //         acc.1 + NVMLEAF_PER_KEY_META_LEN + k.len(),
        //     )
        // });
        // return Some(NVMLEAF_HEADER_FIXED_LEN + Meta::static_size() + data_size + key_size);
        Some(self.size())
    }

    fn cache_size(&self) -> usize {
        match &self.state {
            LeafNodeState::PartiallyLoaded { keys, .. } => {
                Meta::static_size()
                    + std::mem::size_of::<usize>()
                    + keys.len() * Location::static_size()
                    + keys.iter().map(|b| b.0.len()).sum::<usize>()
            }
            LeafNodeState::Deserialized { .. } => self.size(),
        }
    }
}

impl HasStoragePreference for CopylessLeaf {
    fn current_preference(&self) -> Option<StoragePreference> {
        self.meta
            .storage_preference
            .as_option()
            .map(|pref| self.meta.system_storage_preference.weak_bound(&pref))
    }

    fn recalculate(&self) -> StoragePreference {
        let mut pref = StoragePreference::NONE;

        for (keyinfo, _v) in self.state.iter().map(|e| e.1) {
            pref.upgrade(keyinfo.storage_preference);
        }

        self.meta.storage_preference.set(pref);
        self.meta.system_storage_preference.weak_bound(&pref)
    }

    fn system_storage_preference(&self) -> StoragePreference {
        self.meta.system_storage_preference.borrow().into()
    }

    fn set_system_storage_preference(&mut self, pref: StoragePreference) {
        self.meta.system_storage_preference.set(pref)
    }
}

impl<'a> FromIterator<(CowBytes, (KeyInfo, SlicedCowBytes))> for CopylessLeaf {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (CowBytes, (KeyInfo, SlicedCowBytes))>,
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
            if let Some((ckeyinfo, cvalue)) = entries.insert(key.clone(), (keyinfo, value)) {
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

        CopylessLeaf {
            meta: Meta {
                storage_preference: AtomicStoragePreference::known(storage_pref),
                system_storage_preference: AtomicSystemStoragePreference::from(
                    StoragePreference::NONE,
                ),
                entries_size,
            },
            state: LeafNodeState::Deserialized { data: entries },
        }
    }
}

impl CopylessLeaf {
    /// Constructs a new, empty `NVMLeafNode`.
    pub fn new() -> Self {
        CopylessLeaf {
            meta: Meta {
                storage_preference: AtomicStoragePreference::known(StoragePreference::NONE),
                system_storage_preference: AtomicSystemStoragePreference::from(
                    StoragePreference::NONE,
                ),
                entries_size: 0,
            },
            state: LeafNodeState::new(),
        }
    }

    pub fn pack<W: std::io::Write>(&self, mut writer: W) -> Result<IntegrityMode, std::io::Error> {
        let pivots_size: usize = self
            .state
            .force_data()
            .iter()
            .map(|(k, _)| k.len() + NVMLEAF_PER_KEY_META_LEN)
            .sum();
        let meta_len = Meta::static_size() + pivots_size;
        let data_len: usize = self
            .state
            .force_data()
            .iter()
            .map(|(_, (info, val))| info.size() + val.len())
            .sum();
        writer.write_all(&(meta_len as u32).to_le_bytes())?;
        writer.write_all(&(data_len as u32).to_le_bytes())?;
        self.meta.pack(&mut writer)?;

        // Offset after metadata
        let mut data_entry_offset = 0;
        // TODO: Inefficient wire format these are 12 bytes extra for each and every entry
        for (key, (_, val)) in self.state.force_data().iter() {
            writer.write_all(&(key.len() as u32).to_le_bytes())?;
            let val_len = KeyInfo::static_size() + val.len();
            let loc = Location {
                off: data_entry_offset as u32,
                len: val_len as u32,
            };
            loc.pack(&mut writer)?;
            writer.write_all(key)?;
            data_entry_offset += val_len;
        }

        for (_, (info, val)) in self.state.force_data().iter() {
            info.pack(&mut writer)?;
            writer.write_all(&val)?;
        }

        Ok(IntegrityMode::Internal)
    }

    pub fn unpack(data: Buf) -> Result<Self, std::io::Error> {
        // Skip the node
        let data = data
            .into_sliced_cow_bytes()
            .slice_from(crate::tree::imp::node::NODE_PREFIX_LEN as u32);
        let meta_data_len: usize = u32::from_le_bytes(
            data[NVMLEAF_METADATA_LEN_OFFSET..NVMLEAF_DATA_LEN_OFFSET]
                .try_into()
                .unwrap(),
        ) as usize;
        // let data_len: usize = u32::from_le_bytes(
        //     data[NVMLEAF_DATA_LEN_OFFSET..NVMLEAF_METADATA_OFFSET]
        //         .try_into()
        //         .unwrap(),
        // ) as usize;
        let meta_data_end = NVMLEAF_METADATA_OFFSET + meta_data_len;
        let data_start = meta_data_end;

        let meta_data = Meta::unpack(
            &data[NVMLEAF_METADATA_OFFSET..NVMLEAF_METADATA_OFFSET + Meta::static_size()],
        );

        // Read in keys, format: len key len key ...
        let keys = {
            let mut ks = vec![];
            let mut off = NVMLEAF_METADATA_OFFSET + Meta::static_size();
            while off < meta_data_end {
                let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                let location = Location::unpack(&data[off..off + Location::static_size()]);
                off += Location::static_size();
                ks.push((CowBytes::from(&data[off..off + len]), location));
                off += len;
            }
            ks
        };

        // Fetch the slice where data is located.
        let raw_data = data.slice_from(data_start as u32);
        Ok(CopylessLeaf {
            meta: meta_data,
            state: LeafNodeState::PartiallyLoaded {
                buf: raw_data,
                keys,
            },
        })
    }

    /// Returns the value for the given key.
    pub fn get(&self, key: &[u8]) -> Option<SlicedCowBytes> {
        self.state.get(key).and_then(|o| Some(o.1.clone()))
    }

    pub(in crate::tree) fn get_with_info(&self, key: &[u8]) -> Option<(KeyInfo, SlicedCowBytes)> {
        self.state
            .get(key)
            .and_then(|o| Some((o.0.clone(), o.1.clone())))
    }

    pub fn len(&self) -> usize {
        self.state.len()
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
        debug_assert!(right_sibling.meta.entries_size == 0);

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

        *right_sibling.state.force_data_mut() = self.state.force_data_mut().split_off(&split_key);
        right_sibling.meta.entries_size = sibling_size;
        self.meta.entries_size -= sibling_size;
        right_sibling.meta.storage_preference.set(sibling_pref);

        // have removed many keys from self, no longer certain about own pref, mark invalid
        self.meta.storage_preference.invalidate();

        let size_delta = -(sibling_size as isize);

        let pivot_key = self
            .state
            .force_data_mut()
            .keys()
            .next_back()
            .cloned()
            .unwrap();
        (pivot_key, size_delta)
    }

    pub fn apply<K>(&mut self, _key: K, _pref: StoragePreference) -> Option<KeyInfo>
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

        let size_before = self.meta.entries_size as isize;
        let key_size = key.borrow().len();
        let mut data = self.get(key.borrow());
        msg_action.apply_to_leaf(key.borrow(), msg, &mut data);

        if let Some(data) = data {
            // Value was added or preserved by msg
            self.meta.entries_size += data.len();
            self.meta
                .storage_preference
                .upgrade(keyinfo.storage_preference);

            if let Some((old_info, old_data)) =
                self.state.insert(key.into(), (keyinfo.clone(), data))
            {
                // There was a previous value in entries, which was now replaced
                self.meta.entries_size -= old_data.len();

                // if previous entry was stricter than new entry, invalidate
                if old_info.storage_preference < keyinfo.storage_preference {
                    self.meta.storage_preference.invalidate();
                }
            } else {
                // There was no previous value in entries
                self.meta.entries_size +=
                    key_size + NVMLEAF_PER_KEY_META_LEN + KeyInfo::static_size();
            }
        } else if let Some((old_info, old_data)) = self.state.force_data_mut().remove(key.borrow())
        {
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
            if self.meta.storage_preference.as_option() == Some(old_info.storage_preference) {
                self.meta.storage_preference.invalidate();
            }

            self.meta.entries_size -= key_size + NVMLEAF_PER_KEY_META_LEN;
            self.meta.entries_size -= old_data.len() + KeyInfo::static_size();
        }
        self.meta.entries_size as isize - size_before
    }

    /// Inserts messages as leaf entries.
    pub fn insert_msg_buffer<M, I>(&mut self, msg_buffer: I, msg_action: M) -> isize
    where
        M: MessageAction,
        I: IntoIterator<Item = (CowBytes, (KeyInfo, SlicedCowBytes))>,
    {
        self.state.force_upgrade();
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
        self.state.force_upgrade();
        // assert!(self.size() > S::MAX);
        let mut right_sibling = CopylessLeaf {
            // During a split, preference can't be inherited because the new subset of entries
            // might be a subset with a lower maximal preference.
            meta: Meta {
                storage_preference: AtomicStoragePreference::known(StoragePreference::NONE),
                system_storage_preference: AtomicSystemStoragePreference::from(
                    StoragePreference::NONE,
                ),
                entries_size: 0,
            },
            state: LeafNodeState::new(),
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
    pub fn range(&self) -> Box<dyn Iterator<Item = (&CowBytes, (KeyInfo, SlicedCowBytes))> + '_> {
        Box::new(self.state.iter())
    }

    /// Merge all entries from the *right* node into the *left* node.  Returns
    /// the size change, positive for the left node, negative for the right
    /// node.
    pub fn merge(&mut self, right_sibling: &mut Self) -> isize {
        self.state.force_upgrade();
        right_sibling.state.force_upgrade();
        self.state
            .force_data_mut()
            .append(&mut right_sibling.state.force_data_mut());
        let size_delta = right_sibling.meta.entries_size;
        self.meta.entries_size += right_sibling.meta.entries_size;

        self.meta
            .storage_preference
            .upgrade_atomic(&right_sibling.meta.storage_preference);

        // right_sibling is now empty, reset to defaults
        right_sibling.meta.entries_size = 0;
        right_sibling
            .meta
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
    ) -> FillUpResult {
        self.state.force_upgrade();
        right_sibling.state.force_upgrade();
        let size_delta = self.merge(right_sibling);
        if self.size() <= max_size {
            FillUpResult::Merged { size_delta }
        } else {
            // First size_delta is from the merge operation where we split
            let (pivot_key, split_size_delta) =
                self.do_split_off(right_sibling, min_size, max_size);
            FillUpResult::Rebalanced {
                pivot_key,
                size_delta: size_delta + split_size_delta,
            }
        }
    }

    pub fn to_block_leaf(mut self) -> super::leaf::LeafNode {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::{CopylessLeaf, CowBytes, Size};
    use crate::{
        arbitrary::GenExt,
        buffer::BufWrite,
        cow_bytes::SlicedCowBytes,
        data_management::HasStoragePreference,
        tree::{
            default_message_action::{DefaultMessageAction, DefaultMessageActionMsg},
            imp::leaf::copyless_leaf::{
                NVMLEAF_DATA_LEN_OFFSET, NVMLEAF_METADATA_LEN_OFFSET, NVMLEAF_METADATA_OFFSET,
            },
            KeyInfo,
        },
        vdev::Block,
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
    impl Arbitrary for CopylessLeaf {
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

            let node: CopylessLeaf = entries
                .iter()
                .map(|(k, v)| (k.clone(), (KeyInfo::arbitrary(g), v.clone())))
                .collect();
            node.recalculate();
            node
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            let v: Vec<_> = self
                .state
                .force_data()
                .iter()
                .map(|(k, (info, v))| (k.clone(), (info.clone(), CowBytes::from(v.to_vec()))))
                .collect();
            Box::new(v.shrink().map(|entries| {
                entries
                    .iter()
                    .map(|(k, (info, v))| (k.clone(), (info.clone(), v.clone().into())))
                    .collect()
            }))
        }
    }

    fn serialized_size(leaf: &CopylessLeaf) -> usize {
        let mut w = vec![];
        let _m_size = leaf.pack(&mut w);
        w.len()
    }

    #[quickcheck]
    fn actual_size(leaf_node: CopylessLeaf) {
        assert_eq!(leaf_node.actual_size(), Some(serialized_size(&leaf_node)));
    }

    #[quickcheck]
    fn size(leaf_node: CopylessLeaf) {
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
    fn ser_deser(leaf_node: CopylessLeaf) {
        let mut bytes = BufWrite::with_capacity(Block(1));
        bytes
            .write(&[0; crate::tree::imp::node::NODE_PREFIX_LEN])
            .unwrap();
        let _metadata_size = leaf_node.pack(&mut bytes).unwrap();
        let _node = CopylessLeaf::unpack(bytes.into_buf()).unwrap();
    }

    #[quickcheck]
    fn insert(
        mut leaf_node: CopylessLeaf,
        key: CowBytes,
        key_info: KeyInfo,
        msg: DefaultMessageActionMsg,
    ) {
        let size_before = leaf_node.size();
        let size_delta = leaf_node.insert(key, key_info, msg.0, DefaultMessageAction);
        let size_after = leaf_node.size();
        assert_eq!((size_before as isize + size_delta) as usize, size_after);
        assert_eq!(leaf_node.size(), serialized_size(&leaf_node));
        assert_eq!(
            serialized_size(&leaf_node),
            leaf_node.actual_size().unwrap()
        );
        assert_eq!(serialized_size(&leaf_node), size_after);
    }

    const MIN_LEAF_SIZE: usize = 512;
    const MAX_LEAF_SIZE: usize = 4096;

    #[quickcheck]
    fn split(mut leaf_node: CopylessLeaf) -> TestResult {
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
        assert!(leaf_node.size() + sibling.size() <= 2 * MAX_LEAF_SIZE);
        TestResult::passed()
    }

    #[quickcheck]
    fn split_merge_idempotent(mut leaf_node: CopylessLeaf) -> TestResult {
        if leaf_node.size() <= MAX_LEAF_SIZE {
            return TestResult::discard();
        }
        let this = leaf_node.clone();
        let (mut sibling, ..) = leaf_node.split(MIN_LEAF_SIZE, MAX_LEAF_SIZE);
        leaf_node.recalculate();
        leaf_node.merge(&mut sibling);
        leaf_node.recalculate();
        assert_eq!(this.meta, leaf_node.meta);
        assert_eq!(this.state.force_data(), leaf_node.state.force_data());
        TestResult::passed()
    }

    #[quickcheck]
    fn access_serialized(leaf_node: CopylessLeaf) -> TestResult {
        if leaf_node.size() < MIN_LEAF_SIZE && leaf_node.state.force_data().len() < 3 {
            return TestResult::discard();
        }

        let kvs: Vec<(CowBytes, (KeyInfo, SlicedCowBytes))> = leaf_node
            .state
            .force_data()
            .iter()
            .map(|(k, v)| (k.clone(), (v.0.clone(), v.1.clone())))
            .collect();

        let mut buf = BufWrite::with_capacity(Block(1));
        buf.write(&[0; crate::tree::imp::node::NODE_PREFIX_LEN])
            .unwrap();
        let _ = leaf_node.pack(&mut buf).unwrap();
        let buf = buf.into_buf().into_boxed_slice();
        let mut wire_node = CopylessLeaf::unpack(buf.clone().into()).unwrap();

        let meta_data_len: usize = u32::from_le_bytes(
            buf[NVMLEAF_METADATA_LEN_OFFSET + crate::tree::imp::node::NODE_PREFIX_LEN
                ..NVMLEAF_DATA_LEN_OFFSET + crate::tree::imp::node::NODE_PREFIX_LEN]
                .try_into()
                .unwrap(),
        ) as usize;
        let meta_data_end = NVMLEAF_METADATA_OFFSET + meta_data_len;

        wire_node.state.set_data(
            CowBytes::from(buf)
                .slice_from(meta_data_end as u32 + crate::tree::imp::node::NODE_PREFIX_LEN as u32),
        );

        for (key, v) in kvs.into_iter() {
            assert_eq!(Some(v), wire_node.get_with_info(&key));
        }

        TestResult::passed()
    }

    #[quickcheck]
    fn serialize_deser_partial(leaf_node: CopylessLeaf) -> TestResult {
        if leaf_node.size() < MAX_LEAF_SIZE / 2 && leaf_node.state.force_data().len() < 3 {
            return TestResult::discard();
        }

        assert!(leaf_node.range().count() > 0);
        let mut buf = crate::buffer::BufWrite::with_capacity(Block(1));
        buf.write(&[0; crate::tree::imp::node::NODE_PREFIX_LEN])
            .unwrap();
        let _ = leaf_node.pack(&mut buf).unwrap();
        let buf = buf.into_buf();
        let wire_node = CopylessLeaf::unpack(buf.into_boxed_slice().into()).unwrap();
        for (key, (info, val)) in leaf_node.range() {
            assert_eq!(wire_node.get_with_info(&key), Some((info, val)));
        }

        TestResult::passed()
    }
}
