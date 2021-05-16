use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;

use futures_util::future::try_join_all;
use glommio::channels::channel_mesh::MeshBuilder;
use glommio::LocalExecutorBuilder;
use tokio::sync::mpsc::{channel as bounded_channel, Sender};
use tokio::sync::oneshot::channel as oneshot;
use tokio::sync::Mutex;

use crate::context::Context;
use crate::error::Result;
use crate::file::FileManager;
use crate::io_worker::{IOWorker, Task};
use crate::iterator::{Iterator, ShardMuxTimeIterator, ShardTimeIterator, TimeIterator};
use crate::option::{Options, ReadOption, ScanOption};
use crate::types::{Bytes, Entry, TimeRange};
use crate::util::Comparator;

/// Size of channels that used to do IPC between shards.
const CHANNEL_MESH_SIZE: usize = 128;

#[derive(Clone)]
pub struct HelixDB {
    core: Arc<HelixCore>,
}

impl HelixDB {
    pub fn open<P: AsRef<Path>>(path: P, opts: Options) -> Self {
        Self {
            core: Arc::new(HelixCore::new(path, opts)),
        }
    }

    /// Open HelixDB with default [Options]
    pub fn open_default<P: AsRef<Path>>(path: P) -> Self {
        let opts = Options::default();
        Self::open(path, opts)
    }

    pub async fn put(&self, write_batch: Vec<Entry>) -> Result<()> {
        self.core.sharding_put(write_batch).await
    }

    pub async fn direct_put(&self, shard_id: usize, write_batch: Vec<Entry>) -> Result<()> {
        self.core.put_unchecked(shard_id, write_batch).await
    }

    pub async fn get(&self, timestamp: i64, key: Bytes, opt: ReadOption) -> Result<Option<Entry>> {
        self.core.sharding_get(timestamp, key, opt).await
    }

    pub async fn direct_get(
        &self,
        shard_id: usize,
        timestamp: i64,
        key: Bytes,
        opt: ReadOption,
    ) -> Result<Option<Entry>> {
        self.core.get_unchecked(shard_id, timestamp, key, opt).await
    }

    pub async fn scan<C: Comparator + 'static>(
        &self,
        time_range: TimeRange,
        key_range: (Bytes, Bytes),
        opt: ScanOption,
    ) -> Result<impl Iterator> {
        self.core.scan::<C>(time_range, key_range, opt).await
    }
}

unsafe impl Send for HelixDB {}
unsafe impl Sync for HelixDB {}

pub(crate) struct HelixCore {
    /// Join handles of shards' working threads.
    worker_handle: Vec<JoinHandle<()>>,
    task_txs: Vec<Sender<Task>>,
    ctx: Arc<Context>,
}

impl HelixCore {
    fn new<P: AsRef<Path>>(path: P, opts: Options) -> Self {
        let file_manager = FileManager::with_base_dir(path).unwrap();
        let ctx = Arc::new(Context {
            file_manager,
            fn_registry: opts.fn_registry,
        });
        let tsr = Arc::new(Mutex::new(opts.tsr));

        let mut worker_handle = Vec::with_capacity(opts.num_shard);
        let mut task_txs = Vec::with_capacity(opts.num_shard);
        let mesh_builder = MeshBuilder::full(opts.num_shard, CHANNEL_MESH_SIZE);

        for tid in 0..opts.num_shard as u64 {
            let tsr = tsr.clone();
            let ctx = ctx.clone();
            let (tx, rx) = bounded_channel(opts.task_buffer_size);
            let mesh_builder = mesh_builder.clone();

            let handle = LocalExecutorBuilder::new()
                .pin_to_cpu(tid as usize)
                .spawn(move || async move {
                    let (sender, receiver) = mesh_builder.join().await.unwrap();
                    let worker = IOWorker::try_new(tid, tsr, ctx, sender).await.unwrap();
                    worker.run(rx, receiver).await
                })
                .unwrap();

            worker_handle.push(handle);
            task_txs.push(tx);
        }

        Self {
            worker_handle,
            task_txs,
            ctx,
        }
    }

    /// Dispatch entries in write batch to corresponding shards.
    async fn sharding_put(&self, write_batch: Vec<Entry>) -> Result<()> {
        let mut tasks = HashMap::<usize, Vec<_>>::new();

        for entry in write_batch {
            let shard_id = self.ctx.fn_registry.sharding_fn()(&entry.key);
            tasks.entry(shard_id).or_default().push(entry);
        }

        let mut futures = Vec::with_capacity(tasks.len());
        for (shard_id, write_batch) in tasks {
            futures.push(self.put_unchecked(shard_id, write_batch));
        }

        try_join_all(futures).await?;
        Ok(())
    }

    /// Put on specified shard without routing.
    async fn put_unchecked(&self, worker: usize, write_batch: Vec<Entry>) -> Result<()> {
        let (tx, rx) = oneshot();
        let task = Task::Put(write_batch, tx);

        self.task_txs[worker].send(task).await?;

        rx.await?
    }

    async fn sharding_get(
        &self,
        timestamp: i64,
        key: Bytes,
        opt: ReadOption,
    ) -> Result<Option<Entry>> {
        let shard_id = self.ctx.fn_registry.sharding_fn()(&key);
        self.get_unchecked(shard_id, timestamp, key, opt).await
    }

    /// Get on specified shard without routing.
    async fn get_unchecked(
        &self,
        worker: usize,
        timestamp: i64,
        key: Bytes,
        opt: ReadOption,
    ) -> Result<Option<Entry>> {
        let (tx, rx) = oneshot();
        let task = Task::Get(timestamp, key, tx, opt);

        self.task_txs[worker].send(task).await?;

        rx.await?
    }

    async fn scan<C: Comparator + 'static>(
        &self,
        time_range: TimeRange,
        key_range: (Bytes, Bytes),
        opt: ScanOption,
    ) -> Result<impl Iterator> {
        let iters: Vec<_> = (0..self.shards())
            .map(|worker| (worker, key_range.clone()))
            .map(async move |(worker, key_range)| -> Result<_> {
                let (tx, rx) = bounded_channel(opt.prefetch_buf_size);

                self.task_txs[worker]
                    .send(Task::Scan(
                        time_range,
                        key_range.0,
                        key_range.1,
                        tx,
                        Arc::new(C::cmp),
                    ))
                    .await?;
                Ok(ShardTimeIterator::new(rx).await)
            })
            .collect();

        let iters = try_join_all(iters).await?;
        let mux_iter = ShardMuxTimeIterator::<C>::new(iters, opt.prefetch_buf_size).await;
        let iter = TimeIterator::new(mux_iter);

        Ok(iter)
    }

    fn shards(&self) -> usize {
        self.worker_handle.len()
    }
}

impl Drop for HelixCore {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.task_txs));

        for handle in std::mem::take(&mut self.worker_handle) {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn example() {
        let base_dir = tempdir().unwrap();
        let db = HelixDB::open_default(base_dir.path());

        let entry = Entry {
            timestamp: 0,
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        };
        db.put(vec![entry.clone()]).await.unwrap();

        let result = db
            .get(0, b"key".to_vec(), ReadOption::default())
            .await
            .unwrap();
        assert_eq!(result.unwrap(), entry);
    }
}
