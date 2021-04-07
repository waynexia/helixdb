use glommio::{channels::channel_mesh::MeshBuilder, Local};
use glommio::{enclose, LocalExecutorBuilder};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::context::Context;
use crate::error::Result;
use crate::file::FileManager;
use crate::fn_registry::FnRegistry;
use crate::io_worker::IOWorker;
use crate::level::SimpleTimestampReviewer;
use crate::types::{Bytes, Entry};

const DEFAULT_TICK_ORDER: Ordering = Ordering::Relaxed;
/// Size of channels that used to do IPC between shards.
const CHANNEL_MESH_SIZE: usize = 128;

pub struct HelixDB {
    core: Arc<HelixCore>,
    tick: Arc<AtomicUsize>,
    num_workers: usize,
}

impl HelixDB {
    pub fn new<P: AsRef<Path>>(base_dir: P, num_workers: usize) -> Self {
        let core = HelixCore::default(base_dir, num_workers);

        Self {
            core: Arc::new(core),
            tick: Arc::new(AtomicUsize::new(0)),
            num_workers,
        }
    }

    pub async fn put(&self, write_batch: Vec<Entry>) -> Result<()> {
        let tick = self.tick.fetch_add(1, DEFAULT_TICK_ORDER) % self.num_workers;
        self.core.put_unchecked(tick, write_batch).await
    }

    pub async fn get(&self, timestamp: i64, key: Bytes) -> Result<Option<Entry>> {
        let tick = self.tick.fetch_add(1, DEFAULT_TICK_ORDER) % self.num_workers;
        self.core.get_unchecked(tick, timestamp, key).await
    }

    pub async fn scan(&self) {
        todo!()
    }
}

struct HelixCore {
    // todo: remove lock
    workers: Vec<Mutex<IOWorker>>,
}

impl HelixCore {
    fn new() -> Self {
        todo!()
    }

    /// `worker` should be a valid value
    async fn put_unchecked(&self, worker: usize, write_batch: Vec<Entry>) -> Result<()> {
        self.workers[worker].lock().unwrap().put(write_batch).await
    }

    /// `worker` should be a valid value
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

        Self { workers }
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

        db.put(vec![Entry {
            timestamp: 0,
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        }])
        .await
        .unwrap();

        let value = db.get(0, b"key".to_vec()).await.unwrap();
        println!("{:?}", value);
    }
}
