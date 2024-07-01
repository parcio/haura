use super::{
    errors::Result as StoragePoolResult, DiskOffset, StoragePoolConfiguration, StoragePoolLayer,
    NUM_STORAGE_CLASSES,
};
use crate::{
    bounded_future_queue::BoundedFutureQueue, buffer::Buf, checksum::Checksum, tree::StorageKind, vdev::{self, Block, Dev, Error as VdevError, Vdev, VdevRead, VdevWrite}, PreferredAccessType, StoragePreference
};
use futures::{
    executor::{block_on, ThreadPool},
    prelude::*,
    stream::FuturesUnordered,
    task::SpawnExt,
};
use std::{convert::TryInto, marker::PhantomData, ops::Index, pin::Pin, sync::Arc};

/// Actual implementation of the `StoragePoolLayer`.
#[derive(Clone)]
pub struct StoragePoolUnit<C: Checksum> {
    inner: Arc<Inner<C>>,
}

pub(super) type WriteBackQueue = BoundedFutureQueue<
    DiskOffset,
    Pin<Box<dyn Future<Output = Result<(), VdevError>> + Send + Sync + 'static>>,
>;

struct StorageTier {
    devs: Box<[Dev]>,
    preferred_access_type: PreferredAccessType,
    kind: StorageKind,
}

impl StorageTier {
    fn len(&self) -> usize {
        self.devs.len()
    }

    fn iter(&self) -> impl Iterator<Item = &Dev> {
        self.devs.iter()
    }
}

impl Index<usize> for StorageTier {
    type Output = Dev;

    fn index(&self, index: usize) -> &Self::Output {
        &self.devs[index]
    }
}

impl Default for StorageTier {
    fn default() -> Self {
        Self {
            devs: Box::new([]),
            preferred_access_type: PreferredAccessType::Unknown,
            kind: StorageKind::Hdd,
        }
    }
}

impl From<(Box<[Dev]>, PreferredAccessType, StorageKind)> for StorageTier {
    fn from(item: (Box<[Dev]>, PreferredAccessType, StorageKind)) -> Self {
        Self {
            devs: item.0,
            preferred_access_type: item.1,
            kind: item.2,
        }
    }
}

struct Inner<C: Checksum> {
    tiers: [StorageTier; NUM_STORAGE_CLASSES],
    _check: PhantomData<Box<C>>,
    write_back_queue: WriteBackQueue,
    pool: ThreadPool,
    cfg: StoragePoolConfiguration,
    default_storage_class: u8,
}

impl<C: Checksum> Inner<C> {
    fn by_offset(&self, offset: DiskOffset) -> &Dev {
        &self.tiers[offset.storage_class() as usize][offset.disk_id() as usize]
    }
}

impl<C: Checksum> StoragePoolLayer for StoragePoolUnit<C> {
    type Checksum = C;
    type Configuration = StoragePoolConfiguration;
    type Metrics = StoragePoolMetrics;

    fn new(configuration: &Self::Configuration, default_storage_class: u8) -> StoragePoolResult<Self> {
        let tiers: [StorageTier; NUM_STORAGE_CLASSES] = {
            let mut vec: Vec<StorageTier> = configuration
                .tiers
                .iter()
                .map(|tier_cfg| {
                    tier_cfg
                        .build()
                        .map(Vec::into_boxed_slice)
                        .map(|tier| (tier, tier_cfg.preferred_access_type, tier_cfg.storage_kind).into())
                })
                .collect::<Result<Vec<_>, _>>()?;

            assert!(vec.len() <= NUM_STORAGE_CLASSES, "too many storage classes");
            vec.resize_with(NUM_STORAGE_CLASSES, Default::default);
            let boxed: Box<[StorageTier; NUM_STORAGE_CLASSES]> =
                vec.into_boxed_slice().try_into().map_err(|_| ()).unwrap();
            *boxed
        };

        let devices_len = tiers.iter().map(|tier| tier.len()).sum::<usize>();
        let queue_depth = configuration.queue_depth_factor as usize * devices_len;
        Ok(StoragePoolUnit {
            inner: Arc::new(Inner {
                cfg: configuration.clone(),
                default_storage_class,
                tiers,
                _check: PhantomData::default(),
                write_back_queue: BoundedFutureQueue::new(queue_depth),
                pool: {
                    let mut pool = ThreadPool::builder();
                    pool.name_prefix("storage_pool");
                    if let Some(size) = configuration.thread_pool_size {
                        pool.pool_size(size as usize);
                    }
                    if configuration.thread_pool_pinned {
                        pool.after_start(|idx| {
                            // FIXME: Wasteful, queries and allocates once per worker
                            let core_ids =
                                core_affinity::get_core_ids().expect("Unable to query core ids");
                            core_affinity::set_for_current(core_ids[idx % core_ids.len()]);
                        });
                    }
                    pool.create()?
                },
            }),
        })
    }

    type SliceAsync = Pin<Box<dyn Future<Output = Result<&'static [u8], VdevError>> + Send>>;

    fn get_slice(
        &self,
        offset: DiskOffset,
        start: usize,
        end: usize,
    ) -> Result<Self::SliceAsync, VdevError> {
        self.inner.write_back_queue.wait(&offset)?;
        let inner = self.inner.clone();
        Ok(Box::pin(self.inner.pool.spawn_with_handle(async move {
            inner
                .by_offset(offset)
                .get_slice(offset.block_offset(), start, end)
                .await
        })?))
    }

    type ReadAsync = Pin<Box<dyn Future<Output = Result<Buf, VdevError>> + Send>>;

    fn read_async(
        &self,
        size: Block<u32>,
        offset: DiskOffset,
        checksum: C,
    ) -> Result<Self::ReadAsync, VdevError> {
        // TODO: can move this onto pool without deadlock?
        self.inner.write_back_queue.wait(&offset)?;
        let inner = self.inner.clone();
        Ok(Box::pin(self.inner.pool.spawn_with_handle(async move {
            // inner.write_back_queue.wait_async(offset).await;
            inner
                .by_offset(offset)
                .read(size, offset.block_offset(), checksum)
                .await
        })?))
    }

    fn begin_write(&self, data: Buf, offset: DiskOffset) -> Result<(), VdevError> {
        let inner = self.inner.clone();

        let (enqueue_done, wait_for_enqueue) = futures::channel::oneshot::channel();
        let write = self.inner.pool.spawn_with_handle(async move {
            wait_for_enqueue.await.unwrap();

            let res = inner
                .by_offset(offset)
                .write(data, offset.block_offset())
                .await;
            inner.write_back_queue.mark_completed(&offset).await;
            res
        })?;

        let ret = self.inner.write_back_queue.enqueue(offset, Box::pin(write));

        // Sending fails if receiver is dropped at this point,
        // which means the future
        enqueue_done
            .send(())
            .expect("Couldn't unlock enqueued write task");

        ret
    }

    fn write_raw(&self, data: Buf, offset: Block<u64>) -> Result<(), VdevError> {
        let vec = self
            .inner
            .tiers
            .iter()
            .flat_map(|tier| tier.iter())
            .map(|vdev| vdev.write_raw(data.clone(), offset))
            .collect::<FuturesUnordered<_>>()
            .try_collect();
        block_on(vec).map(|_: Vec<()>| ())
    }

    fn read_raw(&self, size: Block<u32>, offset: Block<u64>) -> Result<Vec<Buf>, VdevError> {
        let mut vec = Vec::new();
        for class in self.inner.tiers.iter() {
            for vdev in class.iter() {
                let v = block_on(vdev.read_raw(size, offset).into_future())?;
                vec.extend(v);
            }
        }
        Ok(vec)
    }

    fn actual_size(&self, storage_class: u8, disk_id: u16, size: Block<u32>) -> Block<u32> {
        self.inner.tiers[storage_class as usize][disk_id as usize].actual_size(size)
    }

    fn size_in_blocks(&self, storage_class: u8, disk_id: u16) -> Block<u64> {
        self.inner.tiers[storage_class as usize][disk_id as usize].size()
    }

    fn num_disks(&self, storage_class: u8, disk_id: u16) -> usize {
        self.inner.tiers[storage_class as usize][disk_id as usize].num_disks()
    }

    fn effective_free_size(
        &self,
        storage_class: u8,
        disk_id: u16,
        free_size: Block<u64>,
    ) -> Block<u64> {
        self.inner.tiers[storage_class as usize][disk_id as usize].effective_free_size(free_size)
    }

    fn disk_count(&self, storage_class: u8) -> u16 {
        self.inner.tiers[storage_class as usize].len() as u16
    }

    fn storage_class_count(&self) -> u8 {
        NUM_STORAGE_CLASSES as u8
    }

    fn flush(&self) -> Result<(), VdevError> {
        trace!("Entering flush");
        self.inner.write_back_queue.flush()?;
        trace!("Entering flush");
        for tier in self.inner.tiers.iter() {
            for vdev in tier.iter() {
                vdev.flush()?;
            }
        }
        trace!("Leaving flush");
        Ok(())
    }

    fn metrics(&self) -> Self::Metrics {
        let mut tiers = [None, None, None, None];

        for (tier, out) in self.inner.tiers.iter().zip(tiers.iter_mut()) {
            *out = Some(StorageTierMetrics {
                vdevs: tier.iter().map(Vdev::stats).collect(),
            });
        }

        StoragePoolMetrics { tiers }
    }

    fn access_type_preference(&self, t: crate::PreferredAccessType) -> crate::StoragePreference {
        for (pref, tier) in self.inner.tiers.iter().enumerate() {
            if tier.preferred_access_type == t {
                return StoragePreference::from_u8(pref as u8);
            }
        }
        StoragePreference::NONE
    }

    fn storage_kind_map(&self) -> [StorageKind; NUM_STORAGE_CLASSES] {
        let mut map = [StorageKind::default(); NUM_STORAGE_CLASSES];
        for idx in 0..NUM_STORAGE_CLASSES {
            map[idx] = self.inner.tiers[idx].kind;
        }
        map
    }

    fn default_storage_class(&self) -> u8 {
        self.inner.default_storage_class
    }
}

#[derive(serde::Serialize)]
pub struct StoragePoolMetrics {
    tiers: [Option<StorageTierMetrics>; NUM_STORAGE_CLASSES],
}

#[derive(serde::Serialize)]
pub struct StorageTierMetrics {
    vdevs: Vec<vdev::Statistics>,
}
