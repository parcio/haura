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

pub(super) fn dead_list_min_key(ds_id: DatasetId, ss_id: Generation) -> [u8; 17] {
    let mut key = [0; 17];
    key[0] = 5;
    key[1..9].copy_from_slice(&ds_id.pack());
    key[9..].copy_from_slice(&ss_id.pack());
    key
}

pub(super) fn dead_list_max_key(ds_id: DatasetId, ss_id: Generation) -> [u8; 17] {
    dead_list_min_key(ds_id, ss_id.next())
}

pub(super) fn dead_list_max_key_ds(mut ds_id: DatasetId) -> [u8; 9] {
    ds_id.0 += 1;
    let mut key = [0; 9];
    key[0] = 5;
    key[1..9].copy_from_slice(&ds_id.pack());
    key
}

pub(super) fn dead_list_key(ds_id: DatasetId, cur_gen: Generation, offset: DiskOffset) -> [u8; 25] {
    let mut key = [0; 25];
    key[0] = DEADLIST;
    key[1..9].copy_from_slice(&ds_id.pack());
    key[9..17].copy_from_slice(&cur_gen.pack());
    BigEndian::write_u64(&mut key[17..], offset.as_u64());
    key
}

pub(super) fn offset_from_dead_list_key(key: &[u8]) -> DiskOffset {
    DiskOffset::from_u64(BigEndian::read_u64(&key[17..]))
}
