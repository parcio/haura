//! This file contains creation definition for messages in the root tree of a
//! database. Several prefix are used, which are defined here as constants.
//!
//! To add new messages define an additional prefix and describe the purpose of
//! it here.

// NOTE: Dataset counter and segment is doubly occupied, as only a single
// counter may ever exist and all segment entries have keys of length 9 this is
// fine.
pub(super) const DATASET_ID_COUNTER: u8 = 0;
pub(super) const SEGMENT: u8 = 0;

pub(super) const DATASET_NAME_TO_ID: u8 = 1;
pub(super) const DATASET_DATA: u8 = 2;
pub(super) const SNAPSHOT_DS_ID_AND_NAME_TO_ID: u8 = 3;
pub(super) const SNAPSHOT_DATA: u8 = 4;
pub(super) const DEADLIST: u8 = 5;
pub(crate) const OBJECT_STORE_ID_COUNTER_PREFIX: u8 = 6;
pub(crate) const OBJECT_STORE_NAME_TO_ID_PREFIX: u8 = 7;
pub(crate) const OBJECT_STORE_DATA_PREFIX: u8 = 8;
pub(super) const DISK_SPACE: u8 = 9;

// DATASETS

pub(super) mod dataset {
    //! The required definitions and helpers to handle slices representing a
    //! dataset keys.  Safe handling is only guarantee when using these provided
    //! functions, byte-wise handling is discouraged.
    use crate::database::DatasetId;

    use super::{DATASET_DATA, DATASET_ID_COUNTER, DATASET_NAME_TO_ID};

    const DS_ID_OFFSET: usize = 1;
    const DATA_FULL: usize = 9;

    pub fn id_counter() -> [u8; 1] {
        [DATASET_ID_COUNTER]
    }

    // Full Key for the name to id mapping
    pub fn name_to_id(name: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + name.len());
        key.push(DATASET_NAME_TO_ID);
        key.extend_from_slice(name);
        key
    }

    // Full Key for the id to data mapping
    pub fn data_key(id: DatasetId) -> [u8; DATA_FULL] {
        let mut key = [0; DATA_FULL];
        key[0] = DATASET_DATA;
        key[DS_ID_OFFSET..].copy_from_slice(&id.pack());
        key
    }

    // Above-Upper End of dataset data keys for the use in non-inclusive range queries.
    pub fn data_key_max() -> [u8; 1] {
        [DATASET_DATA + 1]
    }
}

// SEGMENTS

pub(super) mod segment {
    use byteorder::{BigEndian, ByteOrder};

    use crate::allocator::SegmentId;

    use super::SEGMENT;

    const S_ID_OFFSET: usize = 1;
    const FULL: usize = 9;

    pub fn id_to_key(segment_id: SegmentId) -> [u8; FULL] {
        let mut key = [0; FULL];
        key[0] = SEGMENT;
        BigEndian::write_u64(&mut key[S_ID_OFFSET..], segment_id.0);
        key
    }
}

// SNAPSHOTS

pub(super) mod snapshot {
    //! The required definitions and helpers to handle slices representing a
    //! snapshot key.  Safe handling is only guarantee when using these provided
    //! functions, byte-wise handling is discouraged.
    use crate::database::{DatasetId, Generation};

    use super::{SNAPSHOT_DATA, SNAPSHOT_DS_ID_AND_NAME_TO_ID};

    const DS_ID_OFFSET: usize = 1;
    const SS_ID_OFFSET: usize = 9;
    const FULL: usize = 17;

    // Full Key indicating a snapshot name to snapshot id mapping.
    pub fn key(ds_id: DatasetId, name: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(FULL + name.len());
        key.push(SNAPSHOT_DS_ID_AND_NAME_TO_ID);
        key.extend_from_slice(&ds_id.pack());
        key.extend_from_slice(name);
        key
    }

    pub fn data_key(ds_id: DatasetId, ss_id: Generation) -> [u8; FULL] {
        let mut key = [0; FULL];
        key[0] = SNAPSHOT_DATA;
        key[DS_ID_OFFSET..SS_ID_OFFSET].copy_from_slice(&ds_id.pack());
        key[SS_ID_OFFSET..].copy_from_slice(&ss_id.pack());
        key
    }

    // Partial Key
    pub fn data_key_max(mut ds_id: DatasetId) -> [u8; SS_ID_OFFSET] {
        ds_id.0 += 1;
        let mut key = [0; SS_ID_OFFSET];
        key[0] = SNAPSHOT_DATA;
        key[DS_ID_OFFSET..SS_ID_OFFSET].copy_from_slice(&ds_id.pack());
        key
    }
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

// SPACE ACCOUNTING

pub(super) mod space_accounting {
    //! Each space accounting entry is characterized by the 1 bit prefix
    //! followed by a 16 bit disk id.  The tier summarization is for simplicity
    //! encoded in the superblock instead.

    use byteorder::{BigEndian, ByteOrder};

    use crate::storage_pool::GlobalDiskId;

    use super::DISK_SPACE;

    const FULL: usize = 3;
    const D_ID_OFFSET: usize = 1;

    pub fn key(disk_id: GlobalDiskId) -> [u8; FULL] {
        let mut key = [0u8; FULL];
        key[0] = DISK_SPACE;
        BigEndian::write_u16(&mut key[D_ID_OFFSET..], disk_id.as_u16());
        key
    }

    /// This function should always receive a buffer of exactly FULL length.
    /// Other usages are not intended nor supported.
    pub fn read_key(buf: &[u8]) -> GlobalDiskId {
        debug_assert!(buf.len() == FULL);
        GlobalDiskId(BigEndian::read_u16(&buf[D_ID_OFFSET..]))
    }

    pub fn min_key() -> [u8; 1] {
        [DISK_SPACE]
    }

    pub fn max_key() -> [u8; 1] {
        [DISK_SPACE + 1]
    }
}
