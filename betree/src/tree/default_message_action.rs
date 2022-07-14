//! This module provides the default message action, capable of inserts, deletes, and upserts with
//! byte-granularity.
//!
//! To avoid CPU-intensive serialisation/deserialisation, the message is modified in place, without
//! deserialising the rest.
//!
//! ## Message format
//!
//! ```text
//! Delete => [<0, u8>]
//! Insert => [<1, u8>, <bytes to be inserted>] # no length marker, encoded externally
//! Upsert => [<2, u8>, <upserts>]
//!
//! An upsert is encoded as
//!
//! - mode: LE u32
//!     - two most-significant bits indicate mode:
//!         - 00 normal byte upsert
//!         - 01 is reserved for bit upsert
//!         - 10 set bits to 0
//!         - 11 set bits to 1
//!     - remaining 30 bits are offset in unit according to first bit
//!
//! - if byte upsert mode:
//!     - number of bytes to set: LE u32
//!     - byte content: exactly as many u8 as indicated in the preceding length field
//!
//! - if bit set mode:
//!     - number of bits to set: LE u32
//! ```

use super::MessageAction;
use crate::cow_bytes::{CowBytes, SlicedCowBytes};
use bitvec::{order::Lsb0, view::BitView};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use std::{fmt::Debug, iter, mem};

/// This is the default message action. It supports inserts, deletes, and
/// upserts.
#[derive(Default, Debug, Copy, Clone)]
pub struct DefaultMessageAction;

#[repr(u8)]
enum MsgType {
    OverwriteNone = 0,
    OverwriteSome = 1,
    Upsert = 2,
}

impl MsgType {
    fn from(discriminant: u8) -> MsgType {
        match discriminant {
            0 => Self::OverwriteNone,
            1 => Self::OverwriteSome,
            2 => Self::Upsert,
            _ => unreachable!(),
        }
    }
}

enum UpsertType {
    Bytes = 0b00,
    BitsFalse = 0b10,
    BitsTrue = 0b11,
}

impl UpsertType {
    fn from(discriminant: u8) -> UpsertType {
        match discriminant {
            0b00 => Self::Bytes,
            0b10 => Self::BitsFalse,
            0b11 => Self::BitsTrue,
            _ => unreachable!(),
        }
    }
}

pub enum Upsert<'a> {
    Bytes {
        offset_bytes: u32,
        data: &'a [u8],
    },
    Bits {
        offset_bits: u32,
        amount_bits: u32,
        value: bool,
    },
}

impl<'a> Upsert<'a> {
    fn estimate_size(&self) -> usize {
        match self {
            Self::Bytes { data, .. } => mem::size_of::<u32>() + mem::size_of::<u32>() + data.len(),
            Self::Bits { .. } => mem::size_of::<u32>() + mem::size_of::<u32>(),
        }
    }
}

fn as_overwrite(b: SlicedCowBytes) -> Option<Option<SlicedCowBytes>> {
    if b.len() == 0 {
        return None;
    }

    match MsgType::from(b[0]) {
        MsgType::OverwriteNone => Some(None),
        MsgType::OverwriteSome => Some(Some(b.slice_from(1))),
        MsgType::Upsert => None,
    }
}

fn iter_upserts(mut b: &[u8]) -> Option<impl Iterator<Item = Upsert>> {
    if b.get(0) != Some(&(MsgType::Upsert as u8)) {
        return None;
    }
    b = &b[1..];

    Some(iter::from_fn(move || {
        if b.len() < 2 * mem::size_of::<u32>() {
            // Not enough bytes left for len + offset, end of upserts
            return None;
        }

        let (mode, rest) = b.split_at(4);
        let (mode, offset) = {
            let b = LittleEndian::read_u32(mode);
            (b >> 30, b & ((1 << 30) - 1))
        };

        // All modes currently read a length
        let (len, rest) = rest.split_at(4);
        let len = LittleEndian::read_u32(len);
        b = rest;

        match UpsertType::from(mode as u8) {
            UpsertType::Bytes => {
                if len as usize > rest.len() {
                    log::error!("Invalid data length in upsert");
                    return None;
                }
                let (data, rest) = rest.split_at(len as usize);
                b = rest;

                Some(Upsert::Bytes {
                    offset_bytes: offset,
                    data,
                })
            }

            UpsertType::BitsFalse => Some(Upsert::Bits {
                offset_bits: offset,
                amount_bits: len,
                value: false,
            }),

            UpsertType::BitsTrue => Some(Upsert::Bits {
                offset_bits: offset,
                amount_bits: len,
                value: true,
            }),
        }
    }))
}

fn append_upsert(v: &mut Vec<u8>, upsert: &Upsert) {
    // Writes to a Vec can only fail with OOM, and that can't be caught with Results,
    // so the unwraps are fine™
    match upsert {
        &Upsert::Bytes { offset_bytes, data } => {
            v.write_u32::<LittleEndian>(offset_bytes).unwrap();
            v.write_u32::<LittleEndian>(data.len() as u32).unwrap();

            v.extend_from_slice(data);
        }
        &Upsert::Bits {
            offset_bits,
            amount_bits,
            value,
        } => {
            let tag = if value {
                UpsertType::BitsTrue as u32
            } else {
                UpsertType::BitsFalse as u32
            };
            v.write_u32::<LittleEndian>((tag << 30) | offset_bits)
                .unwrap();
            v.write_u32::<LittleEndian>(amount_bits).unwrap();
        }
    }
}

impl DefaultMessageAction {
    fn apply_upserts<'upsert>(
        upserts: impl Iterator<Item = Upsert<'upsert>>,
        msg_data: &mut Option<SlicedCowBytes>,
    ) {
        let mut n_upserts = 0;

        let mut data = msg_data
            .as_ref()
            .map(|b| CowBytes::from(&b[..]))
            .unwrap_or_default();

        for upsert in upserts {
            n_upserts += 1;

            match upsert {
                Upsert::Bytes {
                    offset_bytes,
                    data: new_data,
                } => {
                    let end_offset = offset_bytes as usize + new_data.len();

                    if data.len() <= offset_bytes as usize {
                        data.fill_zeros_up_to(offset_bytes as usize);
                        data.push_slice(new_data);
                    } else {
                        data.fill_zeros_up_to(end_offset);

                        let slice = &mut data[offset_bytes as usize..end_offset as usize];
                        slice.copy_from_slice(new_data);
                    }
                }
                Upsert::Bits {
                    offset_bits,
                    amount_bits,
                    value,
                } => {
                    let end_bit = offset_bits + amount_bits;
                    let end_byte = end_bit / 8 + if end_bit % 8 == 0 { 0 } else { 1 };
                    if end_byte as usize >= data.len() {
                        data.fill_zeros_up_to(end_byte as usize);
                    }

                    data.view_bits_mut::<Lsb0>()[offset_bits as usize..end_bit as usize]
                        .set_all(value);
                }
            }
        }

        if n_upserts > 8 {
            log::warn!("Applied {} upserts", n_upserts);
        }
        *msg_data = Some(data.into());
    }

    fn build_overwrite_msg(data: Option<&[u8]>) -> SlicedCowBytes {
        let mut v = Vec::with_capacity(1 + data.map_or(0, |b| b.len()));
        v.push(if data.is_some() {
            MsgType::OverwriteSome as u8
        } else {
            MsgType::OverwriteNone as u8
        });
        if let Some(b) = data {
            v.extend_from_slice(b);
        }
        CowBytes::from(v).into()
    }

    fn build_upsert_msg(upserts: &[Upsert]) -> SlicedCowBytes {
        let estimated_size = 1 + upserts.iter().map(Upsert::estimate_size).sum::<usize>();
        let mut v = Vec::with_capacity(estimated_size);
        v.push(MsgType::Upsert as u8);

        for upsert in upserts {
            append_upsert(&mut v, upsert);
        }

        debug_assert_eq!(estimated_size, v.len());
        CowBytes::from(v).into()
    }

    /// Return a new message which unconditionally inserts the given `data`.
    pub fn insert_msg(data: &[u8]) -> SlicedCowBytes {
        Self::build_overwrite_msg(Some(data))
    }

    /// Return a new message which deletes data.
    pub fn delete_msg() -> SlicedCowBytes {
        Self::build_overwrite_msg(None)
    }

    /// Return a new message which will update or insert the given `data` at `offset`.
    pub fn upsert_msg(offset_bytes: u32, data: &[u8]) -> SlicedCowBytes {
        Self::build_upsert_msg(&[Upsert::Bytes { offset_bytes, data }])
    }

    /// Return an empty message which will act as a carry for keyinfo updates.
    pub fn noop_msg() -> SlicedCowBytes {
        Self::build_upsert_msg(&[])
    }

    /// Return a new message which will set the specified bit range to `value`.
    pub fn upsert_bits_msg(offset_bits: u32, amount_bits: u32, value: bool) -> SlicedCowBytes {
        Self::build_upsert_msg(&[Upsert::Bits {
            offset_bits,
            amount_bits,
            value,
        }])
    }
}

impl MessageAction for DefaultMessageAction {
    fn apply(&self, _key: &[u8], msg: &SlicedCowBytes, data: &mut Option<SlicedCowBytes>) {
        match MsgType::from(msg[0]) {
            MsgType::OverwriteNone | MsgType::OverwriteSome => {
                let new_data = as_overwrite(msg.clone()).expect("Message was not an overwrite");
                *data = new_data;
            }
            MsgType::Upsert => {
                // There are no upserts if len < 1
                if msg.len() >= 1 {
                    let upserts = iter_upserts(msg).expect("Message was not an upsert");
                    Self::apply_upserts(upserts, data);
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
        match (MsgType::from(upper_msg[0]), MsgType::from(lower_msg[0])) {
            // upper overwrite always wins
            (MsgType::OverwriteNone, _) => upper_msg,
            (MsgType::OverwriteSome, _) => upper_msg,
            (MsgType::Upsert, lower_type) => {
                if upper_msg.len() <= 1 {
                    // no upserts in message
                    return lower_msg;
                }

                let upper_upserts = iter_upserts(&upper_msg).expect("Message was not an upsert");

                match lower_type {
                    MsgType::OverwriteNone | MsgType::OverwriteSome => {
                        let mut data =
                            as_overwrite(lower_msg).expect("Message was not an overwrite");

                        Self::apply_upserts(upper_upserts, &mut data);
                        Self::build_overwrite_msg(data.as_ref().map(|b| &b[..]))
                    }
                    MsgType::Upsert => {
                        // Upserts can simply be appended, (-1) because we only need one MsgType u8
                        let mut v = Vec::with_capacity(lower_msg.len() + upper_msg.len() - 1);

                        v.extend_from_slice(&lower_msg[..]);
                        v.extend_from_slice(&upper_msg[1..]);

                        CowBytes::from(v).into()
                    }
                }
            }
        }
    }
}

#[cfg(test)]
pub use self::tests::DefaultMessageActionMsg;

#[cfg(test)]
mod tests {
    use super::{DefaultMessageAction, MsgType, Upsert};
    use crate::{arbitrary::GenExt, cow_bytes::SlicedCowBytes};
    use quickcheck::{Arbitrary, Gen};
    use rand::Rng;

    #[derive(Debug, Clone)]
    pub struct DefaultMessageActionMsg(pub SlicedCowBytes);

    impl Arbitrary for DefaultMessageActionMsg {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut rng = g.rng();
            let b = MsgType::from(rng.gen_range(0..3));
            match b {
                MsgType::Upsert => {
                    // TODO multiple?
                    let offset = rng.gen_range(0..10);
                    let data: Vec<_> = Arbitrary::arbitrary(g);
                    DefaultMessageActionMsg(DefaultMessageAction::build_upsert_msg(&[
                        Upsert::Bytes {
                            offset_bytes: offset,
                            data: &data,
                        },
                    ]))
                }
                MsgType::OverwriteNone => {
                    DefaultMessageActionMsg(DefaultMessageAction::delete_msg())
                }
                MsgType::OverwriteSome => {
                    let data: Vec<_> = Arbitrary::arbitrary(g);
                    DefaultMessageActionMsg(DefaultMessageAction::insert_msg(&data))
                }
            }
        }
    }
}
