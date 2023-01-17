use super::{errors::*, StorageInfo};
use crate::{
    buffer::{Buf, BufWrite},
    checksum::{Builder, State, XxHash, XxHashBuilder},
    size::StaticSize,
    storage_pool::{StoragePoolLayer, NUM_STORAGE_CLASSES},
    vdev::{Block, BLOCK_SIZE},
};
use bincode::{deserialize, serialize_into};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::{self, Seek};

static MAGIC: &[u8] = b"HEAFSv3\0\n";

/// A superblock contains the location of the root tree,
/// and is read during database initialisation.
#[derive(Serialize, Deserialize, Debug)]
pub struct Superblock<P> {
    magic: [u8; 9],
    pub(crate) root_ptr: P,
    pub(crate) tiers: [StorageInfo; NUM_STORAGE_CLASSES],
}

fn checksum(b: &[u8]) -> XxHash {
    let mut state = XxHashBuilder.build();
    state.ingest(b);
    state.finish()
}

impl<P: DeserializeOwned> Superblock<P> {
    /// Interpret a byte slice as a database superblock.
    /// Errors if the supposed superblock doesn't begin with
    /// a specific version byte sequence (currently `b"HEAFSv3\0\n", but
    /// this sequence is explicitly not part of the stability guarantees),
    /// or the contained checksum doesn't match the actual checksum of the superblock.
    pub fn unpack(b: &[u8]) -> Result<Superblock<P>> {
        let checksum_size = XxHash::static_size();
        let correct_checksum = checksum(&b[..b.len() - checksum_size]);
        let actual_checksum = deserialize(&b[b.len() - checksum_size..])?;
        if correct_checksum != actual_checksum {
            return Err(Error::InvalidSuperblock)
        }
        let this: Self = deserialize(b)?;
        if this.magic != MAGIC {
            return Err(Error::InvalidSuperblock)
        }
        Ok(this)
    }
}

impl Superblock<super::ObjectPointer> {
    /// Try to find a superblock among the first two blocks
    /// of each top-level vdev, returning the newest one if multiple are found.
    pub fn fetch_superblocks<S: StoragePoolLayer>(
        pool: &S,
    ) -> Result<Option<Superblock<super::ObjectPointer>>> {
        let v1 = pool.read_raw(Block(1), Block(0))?;
        let v2 = pool.read_raw(Block(1), Block(1))?;
        Ok(v1
            .into_iter()
            .chain(v2)
            .filter_map(|sb_data| Self::unpack(&sb_data).ok())
            .max_by_key(|sb| sb.root_ptr.generation()))
    }

    /// Write a superblock to each top-level vdev.
    pub fn write_superblock<S: StoragePoolLayer>(
        pool: &S,
        ptr: &super::ObjectPointer,
        tiers: &[StorageInfo; NUM_STORAGE_CLASSES],
    ) -> Result<()> {
        let sb_data = Self::pack(ptr, tiers)?;
        let sb_offset = if ptr.generation().0 & 1 == 0 {
            Block(0)
        } else {
            Block(1)
        };
        pool.write_raw(sb_data, sb_offset)?;
        Ok(())
    }

    /// Overwrite all superblock locations with zeroes.
    pub fn clear_superblock<S: StoragePoolLayer>(pool: &S) -> Result<()> {
        let empty_data = Buf::zeroed(Block(1));
        pool.write_raw(empty_data.clone(), Block(0))?;
        pool.write_raw(empty_data, Block(1))?;
        Ok(())
    }
}

impl<P: Serialize> Superblock<P> {
    fn pack(p: &P, tiers: &[StorageInfo; NUM_STORAGE_CLASSES]) -> Result<Buf> {
        let mut data = BufWrite::with_capacity(Block(1));
        {
            let mut this = Superblock {
                magic: [0; 9],
                root_ptr: p,
                tiers: *tiers,
            };
            this.magic.copy_from_slice(MAGIC);
            serialize_into(&mut data, &this)?;
        }
        let checksum_size = XxHash::static_size();
        data.seek(io::SeekFrom::End(-i64::from(checksum_size as u32)))?;
        let checksum = checksum(&data.as_ref()[..BLOCK_SIZE - checksum_size]);
        serialize_into(&mut data, &checksum)?;
        Ok(data.into_buf())
    }
}
