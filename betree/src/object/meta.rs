use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use speedy::{Endianness, Readable, Writable};

use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    object::ObjectId,
    tree::MessageAction,
    StoragePreference, PreferredAccessType,
};

use std::{
    convert::TryInto,
    io::{self, Cursor},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(crate) static ENDIAN: Endianness = Endianness::LittleEndian;

#[derive(Debug, Clone, Readable, Writable)]
/// The meta data information about a single object. For each object one of these
/// structures is stored and updated according to ongoing operations.
pub struct ObjectInfo {
    /// Assigned identifier in the object store. Unique only in the context of
    /// the belonging object store.
    pub object_id: ObjectId,
    /// The total size of the object in bytes.
    pub size: u64,
    /// Timestamp of the last modification to the object.
    pub mtime: SystemTime,
    /// Most recently used storage preference. Can be used for reinitialization.
    pub pref: StoragePreference,
    /// The specified access pattern hint.
    pub access_pattern: PreferredAccessType,
}

/// Every message represents an overwrite or merge of a set of [ObjectInfo] properties.
/// `size` and `mtime` are merged with `max`, whereas `object_id` is just overwritten.
///
/// The `max` merge is required to allow concurrent writes of mutually ignorant clients
/// without writing over a larger `size` message.
#[derive(Default)]
pub(super) struct MetaMessage {
    pub(super) object_id: Option<ObjectId>,
    pub(super) size: Option<u64>,
    pub(super) mtime: Option<SystemTime>,
    pub(super) pref: Option<StoragePreference>,
    pub(super) access_pattern: Option<PreferredAccessType>,
}

const CONTENT_FLAG_NONE: u8 = MetaMessage::delete().to_content_flags();
const CONTENT_FLAG_ALL: u8 = (MetaMessage {
    object_id: Some(ObjectId(0)),
    size: Some(0),
    mtime: Some(UNIX_EPOCH),
    pref: Some(StoragePreference::NONE),
    access_pattern: Some(PreferredAccessType::Unknown),
})
.to_content_flags();

/// Following functions are closely coupled together, any change to [MetaMessage] must be reflected
/// in all of them.
impl MetaMessage {
    pub const fn new(
        object_id: Option<ObjectId>,
        size: Option<u64>,
        mtime: Option<SystemTime>,
        pref: Option<StoragePreference>,
        access_pattern: Option<PreferredAccessType>,
    ) -> Self {
        MetaMessage {
            object_id,
            size,
            mtime,
            pref,
            access_pattern,
        }
    }

    pub const fn delete() -> MetaMessage {
        MetaMessage::new(None, None, None, None, None)
    }

    pub fn set_info(info: &ObjectInfo) -> MetaMessage {
        MetaMessage::new(
            Some(info.object_id),
            Some(info.size),
            Some(info.mtime),
            Some(info.pref),
            Some(info.access_pattern),
        )
    }

    /// Encodes which of the properties this message changes.
    /// MetaMessage uses bit mask flags the current distribution is (8 bits):
    ///
    const fn to_content_flags(&self) -> u8 {
        (if self.object_id.is_some() { 1 } else { 0 })
            | (if self.size.is_some() { 2 } else { 0 })
            | (if self.mtime.is_some() { 4 } else { 0 })
            | (if self.pref.is_some() { 8 } else { 0 })
    }

    const fn encoded_length(&self) -> usize {
        1 + (if self.object_id.is_some() { 8 } else { 0 })
            + (if self.size.is_some() { 8 } else { 0 })
            + (if self.mtime.is_some() { 8 } else { 0 })
            + (if self.pref.is_some() { 1 } else { 0 })
    }

    pub(crate) fn pack(&self) -> CowBytes {
        let mut v = Vec::with_capacity(self.encoded_length());
        v.push(self.to_content_flags());

        if let Some(object_id) = self.object_id {
            let _ = v.write_u64::<LittleEndian>(object_id.0);
        }
        if let Some(size) = self.size {
            let _ = v.write_u64::<LittleEndian>(size);
        }
        if let Some(mtime) = self.mtime {
            let us_since_epoch = mtime
                .duration_since(UNIX_EPOCH)
                .map_err(|_| ())
                .expect("mtime is earlier than epoch")
                .as_micros()
                .try_into()
                .expect("mtime is past u64 (us) range");

            let _ = v.write_u64::<LittleEndian>(us_since_epoch);
        }
        if let Some(pref) = self.pref {
            let _ = v.write_u8(pref.as_u8());
        }

        CowBytes::from(v)
    }

    pub(crate) fn unpack(msg: &SlicedCowBytes) -> io::Result<MetaMessage> {
        let mut cursor = Cursor::new(&msg[..]);
        let mut message = MetaMessage::default();

        let content_flags = cursor.read_u8()?;
        if content_flags & 1 != 0 {
            message.object_id = Some(ObjectId(cursor.read_u64::<LittleEndian>()?));
        }
        if content_flags & 2 != 0 {
            message.size = Some(cursor.read_u64::<LittleEndian>()?);
        }
        if content_flags & 4 != 0 {
            let us_since_epoch = cursor.read_u64::<LittleEndian>()?;
            let mtime = UNIX_EPOCH + Duration::from_micros(us_since_epoch);
            message.mtime = Some(mtime);
        }
        if content_flags & 8 != 0 {
            message.pref = Some(StoragePreference::from_u8(cursor.read_u8()?));
        }

        Ok(message)
    }
}

/// MetaMessageAction consists of two different message types, split by the key for which the
/// message is intended:
///
/// - a fixed metadata entry consists simply of the object name
/// - a custom metadata is the concatenation of the object name, a zero byte, and the custom key
///
/// A fixed metadata entry has a fixed size, and contains the three metadata values an object will always
/// have. The storage stack will only manage fixed metadata by itself, and leave custom metadata
/// to the user.
///
/// Fixed entries have special merge semantics, which are necessary to allow metadata updates as a
/// result of write operations without a read-modify-write cycle:
///
/// - the object id is replaced by the upper message if both messages have an id
/// - size and mtime are merged with max if both messages have the respective field,
///   to avoid "resetting" back to an earlier value with concurrent writes.
///
/// A custom entry is user-specified per-object key-value metadata. As they are stored inline with
/// fixed metadata, the total size should be kept low to maintain fast object metadata iteration speeds.
/// As a result of this, upserts are not supported for custom metadata, only allowing replacement
/// or deletion.
///
/// Fixed metadata messages have no Rust structure, their encoding is:
/// - `[0]`, as a deletion message
/// - `[1]<user-provided value>`, as a replacement message
#[derive(Debug, Default, Clone)]
pub struct MetaMessageAction;

const FIXED_DELETE: u8 = 0;
const FIXED_REPLACE: u8 = 1;

pub(super) fn is_fixed_key(key: &[u8]) -> bool {
    !key.contains(&0)
}

pub(super) fn delete_custom() -> CowBytes {
    [FIXED_DELETE][..].into()
}

pub(super) fn set_custom(value: &[u8]) -> CowBytes {
    let mut v = Vec::with_capacity(1 + value.len());
    v.push(FIXED_REPLACE);
    v.extend_from_slice(value);
    v.into()
}

impl MessageAction for MetaMessageAction {
    fn apply(&self, key: &[u8], msg: &SlicedCowBytes, data: &mut Option<SlicedCowBytes>) {
        if is_fixed_key(key) {
            let msg = MetaMessage::unpack(msg).expect("Unable to unpack message for application");

            match msg {
                MetaMessage {
                    object_id: Some(object_id),
                    size: Some(size),
                    mtime: Some(mtime),
                    pref: Some(pref),
                    access_pattern: Some(access_pattern),
                } => {
                    // message overwrites entirely, don't bother unpacking existing data
                    let info = ObjectInfo {
                        object_id,
                        size,
                        mtime,
                        pref,
                        access_pattern,
                    };
                    *data =
                        Some(CowBytes::from(info.write_to_vec_with_ctx(ENDIAN).unwrap()).into());
                }
                MetaMessage {
                    object_id: None,
                    size: None,
                    mtime: None,
                    pref: None,
                    access_pattern: None,
                } => {
                    // message deletes entirely
                    *data = None;
                }
                MetaMessage {
                    object_id,
                    size,
                    mtime,
                    pref,
                    access_pattern,
                } => {
                    if let Some(d) = data {
                        let mut info = ObjectInfo::read_from_buffer_with_ctx(ENDIAN, d).unwrap();

                        if let Some(object_id) = object_id {
                            info.object_id = object_id;
                        }
                        if let Some(size) = size {
                            info.size = size;
                        }
                        if let Some(mtime) = mtime {
                            info.mtime = mtime;
                        }
                        if let Some(pref) = pref {
                            info.pref = pref;
                        }
                        if let Some(access_pattern) = access_pattern {
                            info.access_pattern = access_pattern;
                        }

                        *data = Some(
                            CowBytes::from(info.write_to_vec_with_ctx(ENDIAN).unwrap()).into(),
                        );
                    }
                }
            }
        } else {
            // this is a custom metadata entry
            match msg[0] {
                FIXED_DELETE => *data = None,
                FIXED_REPLACE => *data = Some(msg.clone().slice_from(1)),
                _ => unreachable!(),
            }
        }
    }

    fn merge(
        &self,
        key: &[u8],
        upper_msg: SlicedCowBytes,
        lower_msg: SlicedCowBytes,
    ) -> SlicedCowBytes {
        if is_fixed_key(key) {
            let upper = MetaMessage::unpack(&upper_msg).unwrap();

            fn or_max<T: Ord>(a: Option<T>, b: Option<T>) -> Option<T> {
                match (a, b) {
                    (None, None) => None,
                    (Some(x), None) | (None, Some(x)) => Some(x),
                    (Some(a), Some(b)) => Some(a.max(b)),
                }
            }

            match upper.to_content_flags() {
                // upper sets everything, lower has no influence
                CONTENT_FLAG_ALL => upper_msg,
                // upper sets nothing, does not alter lower
                CONTENT_FLAG_NONE => lower_msg,
                // combine upper and lower, upper has priority for object_id
                _ => {
                    let lower = MetaMessage::unpack(&lower_msg).unwrap();
                    let new = MetaMessage {
                        object_id: upper.object_id.or(lower.object_id),
                        size: or_max(upper.size, lower.size),
                        mtime: or_max(upper.mtime, lower.mtime),
                        // Prefer newer if set
                        pref: upper.pref.or(lower.pref),
                        access_pattern: upper.access_pattern.or(lower.access_pattern),
                    };
                    new.pack().into()
                }
            }
        } else {
            // this is a custom metadata entry, and the upper message always wins
            upper_msg
        }
    }
}
