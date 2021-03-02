use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use speedy::{Endianness, Readable, Writable};

use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    tree::MessageAction,
};

use std::{
    convert::TryInto,
    io::{self, Cursor},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(crate) static ENDIAN: Endianness = Endianness::LittleEndian;

#[derive(Debug, Clone, Readable, Writable)]
pub struct ObjectInfo {
    pub(crate) object_id: u64,
    pub(crate) size: u64,
    pub(crate) mtime: SystemTime,
}

/// Every message represents an overwrite or merge of a set of [ObjectInfo] properties.
/// `size` and `mtime` are merged with `max`, whereas `object_id` is just overwritten.
///
/// The `max` merge is required to allow concurrent writes of mutually ignorant clients
/// without writing over a larger `size` message.
#[derive(Default)]
pub(crate) struct MetaMessage {
    object_id: Option<u64>,
    size: Option<u64>,
    mtime: Option<SystemTime>,
}

const CONTENT_FLAG_NONE: u8 = MetaMessage::delete().to_content_flags();
const CONTENT_FLAG_ALL: u8 = (MetaMessage {
    object_id: Some(0),
    size: Some(0),
    mtime: Some(UNIX_EPOCH),
})
.to_content_flags();

/// Following functions are closely coupled together, any change to [MetaMessage] must be reflected
/// in all of them.
impl MetaMessage {
    pub const fn new(object_id: Option<u64>, size: Option<u64>, mtime: Option<SystemTime>) -> Self {
        MetaMessage {
            object_id,
            size,
            mtime,
        }
    }

    pub const fn delete() -> MetaMessage {
        MetaMessage::new(None, None, None)
    }

    pub fn set_info(info: &ObjectInfo) -> MetaMessage {
        MetaMessage::new(Some(info.object_id), Some(info.size), Some(info.mtime))
    }

    /// Encodes which of the properties this message changes.
    const fn to_content_flags(&self) -> u8 {
        (if self.object_id.is_some() { 1 } else { 0 })
            | (if self.size.is_some() { 2 } else { 0 })
            | (if self.mtime.is_some() { 4 } else { 0 })
    }

    const fn encoded_length(&self) -> usize {
        1 + (if self.object_id.is_some() { 8 } else { 0 })
            + (if self.size.is_some() { 8 } else { 0 })
            + (if self.mtime.is_some() { 8 } else { 0 })
    }

    pub(crate) fn pack(&self) -> CowBytes {
        let mut v = Vec::with_capacity(self.encoded_length());
        v.push(self.to_content_flags());

        if let Some(object_id) = self.object_id {
            let _ = v.write_u64::<LittleEndian>(object_id);
        }
        if let Some(size) = self.size {
            let _ = v.write_u64::<LittleEndian>(size);
        }
        if let Some(mtime) = self.mtime {
            let us_since_epoch = mtime
                .duration_since(UNIX_EPOCH)
                .expect("mtime is earlier than epoch")
                .as_micros()
                .try_into()
                .expect("mtime is past u64 (us) range");

            let _ = v.write_u64::<LittleEndian>(us_since_epoch);
        }

        CowBytes::from(v)
    }

    pub(crate) fn unpack(msg: &SlicedCowBytes) -> io::Result<MetaMessage> {
        let mut cursor = Cursor::new(&msg[..]);
        let mut message = MetaMessage::default();

        let content_flags = cursor.read_u8()?;
        if content_flags & 1 != 0 {
            message.object_id = Some(cursor.read_u64::<LittleEndian>()?);
        }
        if content_flags & 2 != 0 {
            message.size = Some(cursor.read_u64::<LittleEndian>()?);
        }
        if content_flags & 4 != 0 {
            let us_since_epoch = cursor.read_u64::<LittleEndian>()?;
            let mtime = UNIX_EPOCH + Duration::from_micros(us_since_epoch);
            message.mtime = Some(mtime);
        }

        Ok(message)
    }
}

#[derive(Debug, Default)]
pub struct MetaMessageAction;

impl MessageAction for MetaMessageAction {
    fn apply(&self, _key: &[u8], msg: &SlicedCowBytes, data: &mut Option<SlicedCowBytes>) {
        let msg = MetaMessage::unpack(msg).expect("Unable to unpack message for application");

        match msg {
            MetaMessage {
                object_id: Some(object_id),
                size: Some(size),
                mtime: Some(mtime),
            } => {
                // message overwrites entirely, don't bother unpacking existing data
                let info = ObjectInfo {
                    object_id,
                    size,
                    mtime,
                };
                *data = Some(CowBytes::from(info.write_to_vec_with_ctx(ENDIAN).unwrap()).into());
            }
            MetaMessage {
                object_id: None,
                size: None,
                mtime: None,
            } => {
                // message deletes entirely
                *data = None;
            }
            MetaMessage {
                object_id,
                size,
                mtime,
            } => {
                if let Some(d) = data {
                    let mut info = ObjectInfo::read_from_buffer_with_ctx(ENDIAN, &d).unwrap();

                    if let Some(object_id) = object_id {
                        info.object_id = object_id;
                    }
                    if let Some(size) = size {
                        info.size = size;
                    }
                    if let Some(mtime) = mtime {
                        info.mtime = mtime;
                    }

                    *data =
                        Some(CowBytes::from(info.write_to_vec_with_ctx(ENDIAN).unwrap()).into());
                }
            }
        }
    }

    fn merge(
        &self,
        _key: &[u8],
        upper_msg: SlicedCowBytes,
        lower_msg: SlicedCowBytes,
    ) -> SlicedCowBytes {
        let upper = MetaMessage::unpack(&upper_msg).unwrap();

        fn or_max<T: Ord>(a: Option<T>, b: Option<T>) -> Option<T> {
            match (a, b) {
                (None, None) => None,
                (Some(x), None) | (None, Some(x)) => Some(x),
                (Some(a), Some(b)) => Some(a.max(b))
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
                    mtime: or_max(upper.mtime, lower.mtime)
                };
                CowBytes::from(new.pack()).into()
            }
        }
    }
}
