use crate::{
    checksum::{Checksum, XxHash},
    storage_pool::{DiskOffset, StoragePoolLayer},
    vdev::{Block, Error as VdevError},
};
use futures::{executor::block_on, prelude::*};

use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct InMemory {
    data: Arc<Mutex<Vec<u8>>>,
}

impl StoragePoolLayer for InMemory {
    type Checksum = XxHash;
    type Configuration = u64;

    fn new(configuration: &Self::Configuration) -> Result<Self, io::Error> {
        Ok(InMemory {
            data: Arc::new(Mutex::new(vec![0; *configuration as usize])),
        })
    }

    /// Reads `size` blocks from the given `offset`.
    fn read(
        &self,
        size: Block<u32>,
        offset: DiskOffset,
        checksum: Self::Checksum,
    ) -> Result<Box<[u8]>, VdevError> {
        block_on(self.read_async(size, offset, checksum)?.into_future())
    }

    /// Future returned by `read_async`.
    type ReadAsync = Pin<Box<dyn Future<Output = Result<Box<[u8]>, VdevError>> + Send>>;

    /// Reads `size` blocks asynchronously from the given `offset`.
    fn read_async(
        &self,
        size: Block<u32>,
        offset: DiskOffset,
        checksum: Self::Checksum,
    ) -> Result<Self::ReadAsync, VdevError> {
        Ok(Box::pin(future::ok({
            if offset.disk_id() != 0 {
                Vec::new().into_boxed_slice()
            } else {
                let offset = offset.block_offset().to_bytes() as usize;
                self.data.lock().unwrap()[offset..offset + size.to_bytes() as usize]
                    .to_vec()
                    .into_boxed_slice()
            }
        })))
    }

    /// Issues a write request that might happen in the background.
    fn begin_write(&self, data: Box<[u8]>, offset: DiskOffset) -> Result<(), VdevError> {
        if offset.disk_id() != 0 {
            return Ok(());
        }
        self.write_raw(data, offset.block_offset())
    }

    /// Writes the given `data` at `offset` for every `LeafVdev`.
    fn write_raw(&self, data: Box<[u8]>, offset: Block<u64>) -> Result<(), VdevError> {
        let offset = offset.to_bytes() as usize;
        self.data.lock().unwrap()[offset..offset + data.len()].copy_from_slice(&data);
        Ok(())
    }

    /// Reads `size` blocks from  the given `offset` for every `LeafVdev`.
    fn read_raw(&self, size: Block<u32>, offset: Block<u64>) -> Vec<Box<[u8]>> {
        let data = self.data.lock().unwrap();
        let offset = offset.to_bytes() as usize;
        let range = offset..offset + size.to_bytes() as usize;

        vec![data[range].to_vec().into_boxed_slice()]
    }

    /// Returns the actual size of a data block for a specific `Vdev`
    /// which may be larger due to parity data.
    fn actual_size(&self, disk_id: u16, size: Block<u32>) -> Block<u32> {
        size
    }

    /// Returns the size for a specific `Vdev`.
    fn size_in_blocks(&self, disk_id: u16) -> Block<u64> {
        Block::from_bytes(self.data.lock().unwrap().len() as u64)
    }

    /// Return the number of leaf vdevs for a specific `Vdev`.
    fn num_disks(&self, disk_id: u16) -> usize {
        if disk_id == 0 {
            1
        } else {
            0
        }
    }

    /// Returns the effective free size for a specific `Vdev`.
    fn effective_free_size(&self, disk_id: u16, free_size: Block<u64>) -> Block<u64> {
        // NOTE: Is this correct?
        if disk_id == 0 {
            self.size_in_blocks(0)
        } else {
            Block::from_bytes(0)
        }
    }

    /// Returns the number of `Vdev`s.
    fn disk_count(&self) -> u16 {
        1
    }

    /// Flushes the write-back queue and the underlying storage backend.
    fn flush(&self) -> Result<(), VdevError> {
        Ok(())
    }
}
