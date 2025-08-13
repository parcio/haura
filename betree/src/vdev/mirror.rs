use super::{
    errors::*, AtomicStatistics, Block, Result, ScrubResult, Statistics, Vdev, VdevLeafRead,
    VdevLeafWrite, VdevRead, VdevWrite,
};
use crate::{buffer::Buf, checksum::Checksum};
use async_trait::async_trait;
use futures::{
    prelude::*,
    stream::{FuturesOrdered, FuturesUnordered},
};
use std::sync::atomic::Ordering;

/// This `vdev` will mirror all data to its child vdevs.
pub struct Mirror<V> {
    vdevs: Box<[V]>,
    id: String,
    stats: AtomicStatistics,
}

impl<V> Mirror<V> {
    /// Creates a new `Mirror`.
    pub fn new(vdevs: Box<[V]>, id: String) -> Self {
        Mirror {
            vdevs,
            id,
            stats: Default::default(),
        }
    }
}

struct ReadResult {
    data: Option<Buf>,
    failed_disks: Vec<usize>,
}

impl<V: VdevLeafWrite> Mirror<V> {
    async fn handle_repair<R>(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        r: ReadResult,
    ) -> Result<R>
    where
        R: From<ScrubResult>,
    {
        let ReadResult { data, failed_disks } = r;
        self.stats.read.fetch_add(size.as_u64(), Ordering::Relaxed);

        let data = match data {
            Some(data) => data,
            None => {
                self.stats
                    .failed_reads
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
                return Err(VdevError::Read(self.id.clone()));
            }
        };
        let faulted = failed_disks.len() as u32;
        let mut total_repaired = 0;
        let mut s: FuturesUnordered<_> = failed_disks
            .into_iter()
            .map(|idx| {
                self.vdevs[idx]
                    .write_raw(data.clone(), offset, true)
                    .into_future()
            })
            .collect();
        while let Some(write_result) = s.next().await {
            if write_result.is_err() {
                // TODO
            } else {
                total_repaired += 1;
            }
        }
        self.stats
            .repaired
            .fetch_add(u64::from(total_repaired) * size.as_u64(), Ordering::Relaxed);
        Ok(ScrubResult {
            data,
            faulted: size * faulted,
            repaired: size * total_repaired,
        }
        .into())
    }
}

#[async_trait]
impl<V: Vdev + VdevRead + VdevLeafRead + VdevLeafWrite + 'static> VdevRead for Mirror<V> {
    async fn read<C: Checksum>(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Result<Buf> {
        // Switch disk every 32 MiB. (which is 2^25 bytes)
        // TODO 32 MiB too large?
        let start_idx = (offset.to_bytes() >> 25) as usize % self.vdevs.len();
        let mut failed_disks = Vec::new();
        let mut data = None;
        let disk_cnt = self.vdevs.len();
        for idx in 0..disk_cnt {
            let idx = (idx + start_idx) % self.vdevs.len();
            let f = self.vdevs[idx].read(size, offset, checksum.clone());
            match f.into_future().await {
                Ok(x) => {
                    data = Some(x);
                    break;
                }
                Err(_) => failed_disks.push(idx),
            }
        }
        let r = ReadResult { data, failed_disks };
        self.handle_repair(size, offset, r).await
    }

    async fn scrub<C: Checksum>(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Result<ScrubResult> {
        let futures: FuturesOrdered<_> = self
            .vdevs
            .iter()
            .map(|disk| disk.read(size, offset, checksum.clone()).into_future())
            .collect();
        let futures: Vec<_> = futures.collect().await;
        let mut data = None;
        let mut failed_disks = Vec::new();
        for (idx, result) in futures.into_iter().enumerate() {
            match result {
                Ok(x) => data = Some(x),
                Err(_) => failed_disks.push(idx),
            }
        }
        let r = ReadResult { data, failed_disks };
        self.handle_repair(size, offset, r).await
    }

    async fn read_raw(&self, size: Block<u32>, offset: Block<u64>) -> Result<Vec<Buf>> {
        let futures: FuturesUnordered<_> = self
            .vdevs
            .iter()
            .map(|disk| {
                let data = Buf::zeroed(size).into_full_mut();
                VdevLeafRead::read_raw(disk, data, offset).into_future()
            })
            .collect();
        let result = futures.collect::<Vec<_>>().await;
        let mut v = Vec::new();
        for x in result.into_iter().flatten() {
            v.push(x.into_full_buf());
        }
        if v.is_empty() {
            Err(VdevError::Read(self.id.clone()))
        } else {
            Ok(v)
        }
    }
}

#[async_trait]
impl<V: VdevLeafWrite + 'static> VdevWrite for Mirror<V> {
    async fn write(&self, data: Buf, offset: Block<u64>) -> Result<()> {
        let size = Block::from_bytes(data.len() as u32);
        self.stats
            .written
            .fetch_add(size.as_u64(), Ordering::Relaxed);
        let futures: FuturesUnordered<_> = self
            .vdevs
            .iter()
            .map(|disk| disk.write_raw(data.clone(), offset, false).into_future())
            .collect();
        let results: Vec<_> = futures.collect().await;
        let total_writes = results.len();
        let mut failed_writes = 0;
        for result in results {
            failed_writes += result.is_err() as usize;
        }
        if failed_writes < total_writes {
            Ok(())
        } else {
            self.stats
                .failed_writes
                .fetch_add(size.as_u64(), Ordering::Relaxed);
            return Err(VdevError::Write(self.id.clone()));
        }
    }

    fn flush(&self) -> Result<()> {
        for vdev in self.vdevs.iter() {
            vdev.flush()?;
        }
        Ok(())
    }

    async fn write_raw(&self, data: Buf, offset: Block<u64>) -> Result<()> {
        let futures: FuturesUnordered<_> = self
            .vdevs
            .iter()
            .map(|disk| disk.write_raw(data.clone(), offset, false).into_future())
            .collect();

        let results: Vec<_> = futures.collect().await;
        for result in results {
            if let Err(e) = result {
                self.stats.failed_writes.fetch_add(
                    Block::from_bytes(data.len() as u32).as_u64(),
                    Ordering::Relaxed,
                );
                return Err(e);
            }
        }
        Ok(())
    }
}

impl<V: Vdev> Vdev for Mirror<V> {
    fn actual_size(&self, size: Block<u32>) -> Block<u32> {
        // Only correct for leaf vdevs
        size
    }

    fn size(&self) -> Block<u64> {
        self.vdevs.iter().map(Vdev::size).min().unwrap()
    }

    fn num_disks(&self) -> usize {
        self.vdevs.len()
    }

    fn effective_free_size(&self, free_size: Block<u64>) -> Block<u64> {
        // Only correct for leaf vdevs
        free_size
    }
    fn id(&self) -> &str {
        &self.id
    }

    fn stats(&self) -> Statistics {
        self.stats.as_stats()
    }

    fn for_each_child(&self, f: &mut dyn FnMut(&dyn Vdev)) {
        for vdev in self.vdevs.iter() {
            f(vdev);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Mirror;
    use crate::{
        buffer::Buf,
        checksum::{Builder, Checksum, State, XxHashBuilder},
        vdev::{
            test::{generate_data, test_writes_are_persistent, FailingLeafVdev, FailureMode},
            Block, Vdev, VdevRead, VdevWrite,
        },
    };
    use futures::executor::block_on;
    use quickcheck::TestResult;

    fn build_mirror_vdev(
        disk_size: Block<u32>,
        num_disks: u8,
    ) -> Result<Mirror<FailingLeafVdev>, TestResult> {
        if num_disks < 2 || disk_size == Block(0) {
            return Err(TestResult::discard());
        }
        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(disk_size, format!("{id}")))
            .collect();
        let vdev = Mirror::new(disks.into_boxed_slice(), String::from("mirror"));
        Ok(vdev)
    }

    #[quickcheck]
    fn size(disk_size: u8, num_disks: u8) -> TestResult {
        let disk_size = Block(disk_size as u32);
        let vdev = try_ret!(build_mirror_vdev(disk_size, num_disks));
        TestResult::from_bool(vdev.size() == Block::<u64>::from(disk_size))
    }

    #[quickcheck]
    fn effective_free_size(disk_size: u8, num_disks: u8) -> TestResult {
        let disk_size = Block(disk_size as u32);
        let vdev = try_ret!(build_mirror_vdev(disk_size, num_disks));
        TestResult::from_bool(
            vdev.effective_free_size(vdev.size()) == Block::<u64>::from(disk_size),
        )
    }

    #[quickcheck]
    fn actual_size(block_size: u8, num_disks: u8) -> TestResult {
        let vdev = try_ret!(build_mirror_vdev(Block(10), num_disks));

        let block_size = Block(block_size as u32);
        TestResult::from_bool(vdev.actual_size(block_size) == block_size)
    }

    #[quickcheck]
    fn writes_without_failure(writes: Vec<(u8, u8)>, num_disks: u8) -> TestResult {
        let vdev = try_ret!(build_mirror_vdev(Block(512), num_disks));
        test_writes_are_persistent(&writes, &vdev);
        TestResult::passed()
    }

    #[quickcheck]
    fn writes_with_failing_write(
        writes: Vec<(u8, u8)>,
        num_disks: u8,
        non_failing_disk_idx: u8,
        failure_mode: FailureMode,
    ) -> TestResult {
        if num_disks < 2 || failure_mode == FailureMode::NoFail {
            return TestResult::discard();
        }
        let non_failing_disk_idx = non_failing_disk_idx % num_disks;
        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(Block(512), format!("{id}")))
            .collect();
        for (idx, disk) in disks.iter().enumerate() {
            if idx != non_failing_disk_idx as usize {
                disk.fail_writes(failure_mode);
            }
        }
        let vdev = Mirror::new(disks.into_boxed_slice(), String::from("mirror"));
        test_writes_are_persistent(&writes, &vdev);
        TestResult::passed()
    }

    #[quickcheck]
    fn writes_with_failing_read(
        writes: Vec<(u8, u8)>,
        num_disks: u8,
        non_failing_disk_idx: u8,
        failure_mode: FailureMode,
    ) -> TestResult {
        if num_disks < 2 || failure_mode == FailureMode::NoFail {
            return TestResult::discard();
        }
        let non_failing_disk_idx = non_failing_disk_idx % num_disks;
        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(Block(512), format!("{id}")))
            .collect();
        for (idx, disk) in disks.iter().enumerate() {
            if idx != non_failing_disk_idx as usize {
                disk.fail_reads(failure_mode);
            }
        }
        let vdev = Mirror::new(disks.into_boxed_slice(), String::from("mirror"));
        test_writes_are_persistent(&writes, &vdev);
        TestResult::passed()
    }

    #[quickcheck]
    fn writes_with_failing_read_and_write(
        writes: Vec<(u8, u8)>,
        num_disks: u8,
        non_failing_disk_idx: u8,
        failure_mode: FailureMode,
    ) -> TestResult {
        if num_disks < 3 || failure_mode == FailureMode::NoFail {
            return TestResult::discard();
        }
        let non_failing_disk_idx = non_failing_disk_idx % num_disks;
        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(Block(512), format!("{id}")))
            .collect();
        for (idx, disk) in disks.iter().enumerate() {
            if idx != non_failing_disk_idx as usize {
                disk.fail_reads(failure_mode);
                disk.fail_writes(failure_mode);
            }
        }
        let vdev = Mirror::new(disks.into_boxed_slice(), String::from("mirror"));
        test_writes_are_persistent(&writes, &vdev);
        TestResult::passed()
    }

    #[test]
    fn writes_fail_with_all_failing_disks() {
        let disks: Vec<_> = (0..10)
            .map(|id| FailingLeafVdev::new(Block(256), format!("{id}")))
            .collect();
        let data = vec![1; Block(1u32).to_bytes() as usize].into_boxed_slice();

        for disk in &disks {
            disk.fail_writes(FailureMode::FailOperation);
        }
        let vdev = Mirror::new(disks.into_boxed_slice(), String::from("mirror"));
        assert!(block_on(vdev.write(Buf::from(data), Block(0))).is_err());
    }

    #[quickcheck]
    fn scrub_detects_bad_data_and_repairs_data(
        writes: Vec<(u8, u8)>,
        num_disks: u8,
        write_non_failing_disk_idx: u8,
        write_failure_mode: FailureMode,
        read_non_failing_disk_idx: u8,
        read_failure_mode: FailureMode,
    ) -> TestResult {
        if num_disks < 3 || write_failure_mode == FailureMode::NoFail {
            return TestResult::discard();
        }
        let write_non_failing_disk_idx = (write_non_failing_disk_idx % num_disks) as usize;
        let read_non_failing_disk_idx = (read_non_failing_disk_idx % num_disks) as usize;

        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(Block(512), format!("{id}")))
            .collect();
        let vdev = Mirror::new(disks.into_boxed_slice(), String::from("mirror"));

        for (idx, &(offset, size)) in writes.iter().enumerate() {
            let offset = Block(offset as u64);
            let size = Block(size as u32);

            for idx in 0..num_disks as usize {
                if idx != write_non_failing_disk_idx {
                    vdev.vdevs[idx].fail_writes(write_failure_mode);
                }
            }
            let data = generate_data(idx, offset, size);
            let checksum = {
                let mut state = XxHashBuilder.build();
                state.ingest(&data);
                state.finish()
            };
            assert!(block_on(vdev.write(data, offset)).is_ok());

            let scrub_result = block_on(vdev.scrub(size, offset, checksum)).unwrap();
            assert!(checksum.verify(&scrub_result.data).is_ok());
            let faulted_blocks = scrub_result.faulted;
            if write_failure_mode == FailureMode::FailOperation {
                assert_eq!(scrub_result.repaired, Block(0));
            }

            block_on(vdev.read(size, offset, checksum)).unwrap();

            for idx in 0..num_disks as usize {
                vdev.vdevs[idx].fail_writes(FailureMode::NoFail);
            }

            let scrub_result = block_on(vdev.scrub(size, offset, checksum)).unwrap();
            assert!(checksum.verify(&scrub_result.data).is_ok());
            assert_eq!(scrub_result.faulted, faulted_blocks);
            assert_eq!(scrub_result.repaired, faulted_blocks);

            for idx in 0..num_disks as usize {
                if idx != read_non_failing_disk_idx {
                    vdev.vdevs[idx].fail_reads(read_failure_mode);
                }
            }

            block_on(vdev.read(size, offset, checksum)).unwrap();

            for idx in 0..num_disks as usize {
                vdev.vdevs[idx].fail_reads(FailureMode::NoFail);
            }
        }
        TestResult::passed()
    }
}
