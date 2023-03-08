//! This file contains creation definition for messages in the root tree of a
//! database. Several prefix are used, which are defined here as constants.
//!
//! To add new messages define an additional prefix and describe the purpose of
//! it here.

use crate::storage_pool::DiskOffset;
use byteorder::{BigEndian, ByteOrder};

use super::{DatasetId, Generation};

pub(super) const DATASET_ID_COUNTER: u8 = 0;
pub(super) const DATASET_NAME_TO_ID: u8 = 1;
pub(super) const DATASET_DATA: u8 = 2;
pub(super) const SNAPSHOT_DS_ID_AND_NAME_TO_ID: u8 = 3;
pub(super) const SNAPSHOT_DATA: u8 = 4;
pub(super) const DEADLIST: u8 = 5;

pub(super) fn ds_id_counter() -> [u8; 1] {
    [DATASET_ID_COUNTER]
}

pub(super) fn ds_name_to_id(name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + name.len());
    key.push(DATASET_NAME_TO_ID);
    key.extend_from_slice(name);
    key
}

pub(super) fn ds_data_key(id: DatasetId) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = DATASET_DATA;
    key[1..].copy_from_slice(&id.pack());
    key
}

// SNAPSHOTS

pub(super) fn ss_key(ds_id: DatasetId, name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 8 + name.len());
    key.push(SNAPSHOT_DS_ID_AND_NAME_TO_ID);
    key.extend_from_slice(&ds_id.pack());
    key.extend_from_slice(name);
    key
}

pub(super) fn ss_data_key(ds_id: DatasetId, ss_id: Generation) -> [u8; 17] {
    let mut key = [0; 17];
    key[0] = SNAPSHOT_DATA;
    key[1..9].copy_from_slice(&ds_id.pack());
    key[9..].copy_from_slice(&ss_id.pack());
    key
}

pub(super) fn ss_data_key_max(mut ds_id: DatasetId) -> [u8; 9] {
    ds_id.0 += 1;
    let mut key = [0; 9];
    key[0] = SNAPSHOT_DATA;
    key[1..9].copy_from_slice(&ds_id.pack());
    key
}

// DEADLIST - snapshot only objects

pub(super) mod deadlist {
    //! The required definitions and helpers to handle slices representing a
    //! deadlist object key.  Safe handling is only guarantee when using these
    //! provided functions, byte-wise handling is discouraged.

    use byteorder::{BigEndian, ByteOrder};

    use crate::{
        database::{DatasetId, Generation},
        storage_pool::DiskOffset,
    };

    use super::DEADLIST;
    const DS_ID_OFFSET: usize = 1;
    const SS_ID_OFFSET: usize = 9;
    const DO_OFFSET: usize = 17;
    const FULL: usize = 25;

    // Partially Key filled up to the snapshot id
    pub fn min_key(ds_id: DatasetId, ss_id: Generation) -> [u8; DO_OFFSET] {
        let mut key = [0; DO_OFFSET];
        key[0] = DEADLIST;
        key[DS_ID_OFFSET..SS_ID_OFFSET].copy_from_slice(&ds_id.pack());
        key[SS_ID_OFFSET..].copy_from_slice(&ss_id.pack());
        key
    }

    // Partially Key filled up to the snapshot id
    pub fn max_key(ds_id: DatasetId, ss_id: Generation) -> [u8; DO_OFFSET] {
        min_key(ds_id, ss_id.next())
    }

    // Partially Key filled up to the dataset id
    pub fn max_key_ds(mut ds_id: DatasetId) -> [u8; SS_ID_OFFSET] {
        ds_id.0 += 1;
        let mut key = [0; SS_ID_OFFSET];
        key[0] = DEADLIST;
        key[DS_ID_OFFSET..SS_ID_OFFSET].copy_from_slice(&ds_id.pack());
        key
    }

    pub fn key(ds_id: DatasetId, cur_gen: Generation, offset: DiskOffset) -> [u8; FULL] {
        let mut key = [0; FULL];
        key[0] = DEADLIST;
        key[DS_ID_OFFSET..SS_ID_OFFSET].copy_from_slice(&ds_id.pack());
        key[SS_ID_OFFSET..DO_OFFSET].copy_from_slice(&cur_gen.pack());
        BigEndian::write_u64(&mut key[DO_OFFSET..], offset.as_u64());
        key
    }

    pub fn offset_from_key(key: &[u8]) -> DiskOffset {
        DiskOffset::from_u64(BigEndian::read_u64(&key[DO_OFFSET..]))
    }
}
