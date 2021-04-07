use futures_util::future::try_join_all;
use glommio::LocalExecutorBuilder;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use crate::context::Context;
use crate::error::Result;
use crate::file::FileManager;
use crate::fn_registry::FnRegistry;
use crate::io_worker::IOWorker;
use crate::level::SimpleTimestampReviewer;
use crate::types::{Bytes, Entry};

/// Size of channels that used to do IPC between shards.
const CHANNEL_MESH_SIZE: usize = 128;

pub struct HelixDB {
    core: Arc<HelixCore>,
}

impl HelixDB {
    pub fn new<P: AsRef<Path>>(base_dir: P, num_workers: usize) -> Self {
        let core = HelixCore::default(base_dir, num_workers);

        Self {
            core: Arc::new(core),
        }
    }

    pub async fn put(&self, write_batch: Vec<Entry>) -> Result<()> {
        self.core.sharding_put(write_batch).await
    }

    pub async fn direct_put(&self, shard_id: usize, write_batch: Vec<Entry>) -> Result<()> {
        self.core.put_unchecked(shard_id, write_batch).await
    }

    pub async fn get(&self, timestamp: i64, key: Bytes) -> Result<Option<Entry>> {
        self.core.sharding_get(timestamp, key).await
    }

    pub async fn direct_get(
        &self,
        shard_id: usize,
        timestamp: i64,
        key: Bytes,
    ) -> Result<Option<Entry>> {
        self.core.get_unchecked(shard_id, timestamp, key).await
    }

    pub async fn scan(&self) {
        todo!()
    }
}

struct HelixCore {
    // todo: remove lock
    workers: Vec<Mutex<IOWorker>>,
    ctx: Arc<Context>,
}

impl HelixCore {
    fn new() -> Self {
        todo!()
    }

    /// Dispatch entries in write batch to corresponding shards.
    async fn sharding_put(&self, write_batch: Vec<Entry>) -> Result<()> {
        let mut tasks = HashMap::<usize, Vec<_>>::new();

        for entry in write_batch {
            let shard_id = self.ctx.fn_registry.sharding_fn()(&entry.key);
            tasks.entry(shard_id).or_default().push(entry);
        }

        let mut handles = Vec::with_capacity(tasks.len());
        for (shard_id, write_batch) in tasks {
            handles.push(self.workers[shard_id].lock().unwrap().put(write_batch));
        }

        try_join_all(handles).await?;

        Ok(())
    }

    /// Put on specified shard without routing.
    async fn put_unchecked(&self, worker: usize, write_batch: Vec<Entry>) -> Result<()> {
        self.workers[worker].lock().unwrap().put(write_batch).await
    }

    async fn sharding_get(&self, timestamp: i64, key: Bytes) -> Result<Option<Entry>> {
        let shard_id = self.ctx.fn_registry.sharding_fn()(&key);

        self.workers[shard_id]
            .lock()
            .unwrap()
            .get(&(timestamp, key))
            .await
    }

    /// Get on specified shard without routing.
    async fn get_unchecked(
        &self,
        worker: usize,
        timestamp: i64,
        key: Bytes,
    ) -> Result<Option<Entry>> {
        self.workers[worker]
            .lock()
            .unwrap()
            .get(&(timestamp, key))
            .await
    }

    // todo: remove this, finish Config
    fn default<P: AsRef<Path>>(base_dir: P, num_workers: usize) -> Self {
        let file_manager = FileManager::with_base_dir(base_dir).unwrap();
        let fn_registry = FnRegistry::new_noop();
        let ctx = Arc::new(Context {
            file_manager,
            fn_registry,
        });
        let timestamp_reviewer = Arc::new(Mutex::new(SimpleTimestampReviewer::new(10, 30)));

        // let mesh_builder = MeshBuilder::full(num_workers, CHANNEL_MESH_SIZE);
        let mut workers = Vec::with_capacity(num_workers);
        for tid in 0..num_workers as u64 {
            let executor = LocalExecutorBuilder::new()
                .pin_to_cpu(tid as usize)
                .make()
                .unwrap();
            let worker =
                IOWorker::try_new(tid, timestamp_reviewer.clone(), ctx.clone(), executor).unwrap();
            workers.push(Mutex::new(worker));
        }

        Self { workers, ctx }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use tempfile::tempdir;

    #[tokio::test]
    async fn example() {
        let base_dir = tempdir().unwrap();
        let db = HelixDB::new(base_dir.path(), 4);

        let entry = Entry {
            timestamp: 0,
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        };
        db.put(vec![entry.clone()]).await.unwrap();

        let result = db.get(0, b"key".to_vec()).await.unwrap();
        assert_eq!(result.unwrap(), entry);
    }
}
