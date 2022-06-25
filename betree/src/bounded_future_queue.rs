//! This module provides a bounded queue of futures which are identified by a
//! key.
//!
//! This module tries to uphold the following guarantees:
//!
//! - During a flush, no further futures can be enqueued
//! - wait, wait_async, and contains_key can only indicate future completion if it has actually
//!   been completed, either via block_on or .await
//!

use futures::{
    executor::block_on,
    future::{IntoFuture, Shared},
    prelude::*,
};
use parking_lot::{Mutex, RwLock};
use std::{borrow::Borrow, hash::Hash};

use indexmap::IndexMap;

/// A bounded queue of `F`s which are identified by a key `K`.
pub struct BoundedFutureQueue<K, F>
where
    F: TryFuture,
    F::Output: Clone,
{
    futures: RwLock<IndexMap<K, Shared<IntoFuture<F>>>>,
    limit: usize,
    flush_lock: Mutex<()>,
}

impl<K: Clone + Eq + Hash, F: TryFuture<Ok = ()>> BoundedFutureQueue<K, F>
where
    F::Output: Clone,
    F::Error: Clone,
{
    /// Creates a new queue with the given `limit`.
    pub fn new(limit: usize) -> Self {
        BoundedFutureQueue {
            futures: RwLock::new(IndexMap::new()),
            limit,
            flush_lock: Mutex::new(()),
        }
    }

    /// Enqueues a new `Future`. This function will block if the queue is full.
    pub fn enqueue(&self, key: K, future: F) -> Result<(), F::Error>
    where
        K: Clone,
    {
        {
            let _lock = self.flush_lock.lock();
            self.wait(&key)?;
            let previous = self
                .futures
                .write()
                .insert(key, future.into_future().shared());
            assert!(previous.is_none());
        }

        if self.futures.read().len() > self.limit {
            self.drain_while_above_limit(self.limit)?;
        }
        Ok(())
    }

    /// Remove a task from the queue
    pub async fn mark_completed(&self, key: &K) {
        self.futures.write().shift_remove(key);
    }

    fn drain_any_future(&self) -> Option<Result<(), F::Error>> {
        trace!("Trying to drain futures from queue");
        let maybe_entry = self
            .futures
            .write()
            .get_index(0)
            .map(|(k, v)| (k.clone(), v.clone()));

        if let Some((k, v)) = maybe_entry {
            let ret = block_on(v);

            self.futures.write().shift_remove(&k);
            trace!("Removing future from queue");

            Some(ret)
        } else {
            None
        }
    }

    async fn drain_specific_future(&self, key: K) -> Option<Result<(), F::Error>> {
        let maybe_fut = self
            .futures
            .write()
            .get_key_value(&key)
            .map(|(k, v)| (k.clone(), v.clone()));

        if let Some((key, fut)) = maybe_fut {
            let ret = Some(fut.await);
            self.futures.write().shift_remove(&key);
            ret
        } else {
            None
        }
    }

    fn drain_while_above_limit(&self, limit: usize) -> Result<(), F::Error> {
        while self.futures.read().len() > limit {
            match self.drain_any_future() {
                None => break,
                Some(res) => res?,
            }
        }

        Ok(())
    }

    /// Flushes the queue.
    pub fn flush(&self) -> Result<(), F::Error> {
        trace!("Entering write back queue flush");
        let _lock = self.flush_lock.lock();
        trace!("Acquired flush lock");

        self.drain_while_above_limit(0)
    }

    /// Waits asynchronously for the given `key`.
    /// May flush the whole queue if a future returned an error beforehand.
    pub fn wait_async<'a, Q: Borrow<K> + 'a>(
        &'a self,
        key: Q,
    ) -> impl TryFuture<Ok = (), Error = F::Error> + 'a {
        async move {
            self.drain_specific_future(key.borrow().clone())
                .await
                .unwrap_or(Ok(()))
        }
    }

    /// Waits for the given `key`.
    /// May flush the whole queue if a future returned an error beforehand.
    pub fn wait(&self, key: &K) -> Result<(), F::Error> {
        block_on(self.wait_async(key).into_future())
    }
}
