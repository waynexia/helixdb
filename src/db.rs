use std::collections::HashMap;
use std::intrinsics::unlikely;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use futures_util::future::{join_all, try_join_all};
use glommio::channels::channel_mesh::MeshBuilder;
use glommio::{LocalExecutor, LocalExecutorBuilder};
use tokio::sync::mpsc::{channel as bounded_channel, Sender};
use tokio::sync::oneshot::channel as oneshot;
use tokio::sync::Mutex;
use tracing::info;

use crate::context::Context;
use crate::error::{HelixError, Result};
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

    pub async fn close(self) {
        info!("Closing HelixDB");
        self.core.close().await;
    }
}

unsafe impl Send for HelixDB {}
unsafe impl Sync for HelixDB {}

pub(crate) struct HelixCore {
    /// Join handles of shards' working threads.
    worker_handle: Vec<JoinHandle<()>>,
    task_txs: Vec<Sender<Task>>,
    ctx: Arc<Context>,
    is_closed: AtomicBool,
}

impl HelixCore {
    fn new<P: AsRef<Path>>(path: P, mut opts: Options) -> Self {
        let file_manager = FileManager::with_base_dir(path).unwrap();
        let ctx = Arc::new(Context {
            file_manager,
            fn_registry: opts.fn_registry.take().unwrap(),
        });
        let tsr = Arc::new(Mutex::new(opts.tsr.take().unwrap()));
        let level_info = LocalExecutor::default().run(async {
            // initialize components requiring runtime.
            Arc::new(Mutex::new(
                ctx.file_manager.open_level_info().await.unwrap(),
            ))
        });

        let mut worker_handle = Vec::with_capacity(opts.num_shard);
        let mut task_txs = Vec::with_capacity(opts.num_shard);
        let mesh_builder = MeshBuilder::full(opts.num_shard, CHANNEL_MESH_SIZE);

        for tid in 0..opts.num_shard as u64 {
            let tsr = tsr.clone();
            let ctx = ctx.clone();
            let opts = opts.clone_partial();
            let (tx, rx) = bounded_channel(opts.task_buffer_size);
            let mesh_builder = mesh_builder.clone();
            let level_info = level_info.clone();

            let handle = LocalExecutorBuilder::new()
                .pin_to_cpu(tid as usize)
                .spawn(move || async move {
                    let (sender, receiver) = mesh_builder.join().await.unwrap();
                    let worker = IOWorker::try_new(tid, opts, tsr, level_info, ctx, sender)
                        .await
                        .unwrap();
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
            is_closed: AtomicBool::new(false),
        }
    }

    /// Dispatch entries in write batch to corresponding shards.
    async fn sharding_put(&self, write_batch: Vec<Entry>) -> Result<()> {
        self.check_closed()?;

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
        self.check_closed()?;

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
        self.check_closed()?;

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
        self.check_closed()?;

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
        self.check_closed()?;

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

    async fn close(&self) {
        self.is_closed.store(true, Ordering::SeqCst);

        for index in 0..self.shards() {
            let _ = self.task_txs[index].send(Task::Shutdown).await;
        }

        join_all(self.task_txs.iter().map(|sender| sender.closed())).await;
    }

    fn shards(&self) -> usize {
        self.worker_handle.len()
    }

    fn check_closed(&self) -> Result<()> {
        if unlikely(self.is_closed.load(Ordering::SeqCst)) {
            return Err(HelixError::Closed);
        }

        Ok(())
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
    use std::convert::TryInto;

    use tempfile::tempdir;

    use super::*;
    use crate::{FnRegistry, LexicalComparator, SimpleTimestampReviewer};

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

    async fn scan_test_scaffold(
        num_shard: usize,
        num_timestamp: i64,
        num_key: u64,
        compact_interval: i64,
    ) {
        assert!(num_timestamp > 0, "timestamp number should be positive");

        let mut fn_registry = FnRegistry::new_noop();
        fn_registry.register_sharding_key_fn(Arc::new(move |key| {
            u64::from_le_bytes(key.to_owned().try_into().unwrap()) as usize % num_shard
        }));
        let simple_tsr = SimpleTimestampReviewer::new(compact_interval, i64::MAX);
        let opts = Options::default()
            .shards(num_shard)
            .set_fn_registry(fn_registry)
            .set_timestamp_reviewer(Box::new(simple_tsr));
        let base_dir = tempdir().unwrap();
        let db = HelixDB::open(base_dir.path(), opts);

        // write
        for timestamp in 0..num_timestamp {
            let entries = (0..num_key)
                .into_iter()
                .map(|key| Entry {
                    timestamp,
                    key: key.to_le_bytes().to_vec(),
                    value: b"value".to_vec(),
                })
                .collect();
            db.put(entries).await.unwrap();
        }

        println!("write finished");

        // scan
        let mut iter = db
            .scan::<LexicalComparator>(
                (0, num_timestamp).into(),
                (0u64.to_le_bytes().to_vec(), num_key.to_le_bytes().to_vec()),
                ScanOption {
                    prefetch_buf_size: 1,
                },
            )
            .await
            .unwrap();

        let mut count = 0;
        while iter.is_valid() {
            iter.next().await.unwrap();
            count += 1;
        }

        assert_eq!(num_timestamp as u64 * num_key, count);
    }

    #[tokio::test]
    async fn scan_1_shard_without_compaction() {
        scan_test_scaffold(1, 10, 128, 1024).await;
    }

    #[tokio::test]
    async fn scan_many_shards_without_compaction() {
        scan_test_scaffold(num_cpus::get(), 10, 128, 1024).await;
    }

    #[tokio::test]
    async fn scan_many_shards_with_compaction() {
        scan_test_scaffold(2, 64, 8, 32).await;
    }

    #[tokio::test]
    async fn recover_from_restart() {
        let base_dir = tempdir().unwrap();
        let opts = Options::default()
            .set_timestamp_reviewer(Box::new(SimpleTimestampReviewer::new(5, 100)))
            .shards(1);
        let db = HelixDB::open(base_dir.path(), opts);

        let tasks = (0..50)
            .map(|ts| {
                db.put(vec![Entry {
                    timestamp: ts,
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                }])
            })
            .collect::<Vec<_>>();
        try_join_all(tasks).await.unwrap();
        db.close().await;

        let opts = Options::default()
            .set_timestamp_reviewer(Box::new(SimpleTimestampReviewer::new(5, 100)))
            .shards(1);
        let db = HelixDB::open(base_dir.path(), opts);
        for ts in 0..50 {
            let result = db
                .get(ts, b"key".to_vec(), ReadOption::default())
                .await
                .unwrap();
            assert_eq!(result.unwrap().value, b"value".to_vec());
        }
    }
}
