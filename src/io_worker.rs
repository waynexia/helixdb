use std::cmp::Ordering;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use glommio::channels::channel_mesh::{
    Receivers as ChannelMeshReceiver,
    Senders as ChannelMeshSender,
};
use glommio::sync::Gate;
use glommio::Task as GlommioTask;
use tokio::sync::mpsc::{channel as bounded_channel, Receiver, Sender};
use tokio::sync::oneshot::{channel as oneshot, Sender as Notifier};
use tokio::sync::Mutex;
use tracing::trace;

use crate::context::Context;
use crate::error::Result;
use crate::file::FileManager;
use crate::level::{Levels, TimestampReviewer};
use crate::option::{Options, ReadOption, ScanOption};
use crate::types::{Bytes, Entry, LevelInfo, ThreadId, TimeRange, Timestamp};
use crate::TimestampAction;

thread_local!(
    // todo: the api of glommio::Gate seems not very suit for our use case.
    // Expecting for a more ergonomic way to register critical task.
    // It is essentially a counter.
    /// A TLS variable for graceful shutdown.
    ///
    /// It will wait until all tasks spawned via it are finished when closing.
    pub static GATE: Rc<Gate> = Rc::new(Gate::new())
);

/// A un-Send handle to accept and process requests. This struct can either be
/// used via [HelixDB] in multi-shards scenario, or directly as a handle to submit
/// tasks when only one shard is present.
pub struct IOWorker {
    tid: ThreadId,
    levels: Rc<Levels>,
    // todo: maybe add channel mesh for scan
}

impl IOWorker {
    crate async fn try_new(
        tid: ThreadId,
        opts: Options,
        timestamp_reviewer: Arc<Mutex<Box<dyn TimestampReviewer + 'static>>>,
        level_info: Arc<Mutex<LevelInfo>>,
        ctx: Arc<Context>,
        ts_action_sender: ChannelMeshSender<TimestampAction>,
    ) -> Result<Self> {
        let levels = Levels::try_new(
            tid,
            opts,
            timestamp_reviewer,
            ctx,
            Some(ts_action_sender),
            level_info,
        )
        .await?;

        Ok(Self { tid, levels })
    }

    /// Create a standalone worker, which won't communicate with other shards / workers
    /// (and shouldn't have others when using this).
    ///
    /// This is usually used when there is only one core and shard. Otherwise [HelixDB::open]
    /// is a better choice.
    ///
    /// This requires runtime to run (currently only [glommio::LocalExecutor] is supported)
    /// and can be used only inside that runtime.
    ///
    /// The in-param is the same with [HelixDB::Open]. Notice that [Options::shards] has no
    /// effect.
    pub async fn try_new_standalone<P: AsRef<Path>>(path: P, mut opts: Options) -> Result<Self> {
        let tid = 0;
        let file_manager = FileManager::with_base_dir(path).unwrap();
        let ctx = Arc::new(Context {
            file_manager,
            fn_registry: opts.fn_registry.take().unwrap(),
        });
        let tsr = Arc::new(Mutex::new(opts.tsr.take().unwrap()));
        let level_info = Arc::new(Mutex::new(
            ctx.file_manager.open_level_info().await.unwrap(),
        ));

        let levels = Levels::try_new(tid, opts, tsr, ctx, None, level_info).await?;

        Ok(Self { tid, levels })
    }

    /// Won't return until shut down.
    crate async fn run(
        self,
        mut rx: Receiver<Task>,
        mut ts_action_receiver: ChannelMeshReceiver<TimestampAction>,
    ) {
        let connected_receivers: Vec<_> = ts_action_receiver
            .streams()
            .into_iter()
            .map(|(_, rx)| rx)
            .collect();

        for rx in connected_receivers {
            let levels = self.levels.clone();
            let tid = self.tid;
            GlommioTask::local(async move {
                while let Some(action) = rx.recv().await {
                    trace!("{} received action {:?}", tid, action);
                    let _ = levels.handle_actions(vec![action]).await;
                }
            })
            .detach();
        }

        // the `Error` case of `Gate::spawn()` is glommio runtime cannot find given task
        // queue which needn't to take into consideration since we don't specify
        // task queue.
        while let Some(task) = rx.recv().await {
            match task {
                Task::Put(entries, tx) => {
                    let levels = self.levels.clone();
                    GATE.with(|gate| {
                        gate.spawn(async move {
                            levels.put(entries, tx).await;
                        })
                        .unwrap()
                        .detach()
                    });
                }
                Task::Get(ts, key, tx, opt) => {
                    let levels = self.levels.clone();
                    GATE.with(|gate| {
                        gate.spawn(async move {
                            let result = levels.get(&(ts, key), opt).await;
                            let _ = tx.send(result);
                        })
                        .unwrap()
                        .detach()
                    });
                }
                Task::Scan(time_range, key_start, key_end, sender, cmp) => {
                    let levels = self.levels.clone();
                    GATE.with(|gate| {
                        gate.spawn(async move {
                            let _ = levels
                                .scan(time_range, key_start, key_end, sender, cmp)
                                .await;
                        })
                        .unwrap()
                        .detach()
                    });
                }
                Task::Shutdown => {
                    trace!("going to close shard {}", self.tid);

                    let gate = GATE.with(|gate| gate.clone());
                    let _ = gate.close().await;

                    trace!("shard {} is closed", self.tid);
                    break;
                }
            }
        }
    }

    pub async fn put(&self, entries: Vec<Entry>) -> Result<()> {
        let (tx, rx) = oneshot();
        let levels = self.levels.clone();
        GATE.with(|gate| {
            gate.spawn(async move {
                levels.put(entries, tx).await;
            })
            .unwrap()
            .detach()
        });

        rx.await?
    }

    pub async fn get(&self, ts: Timestamp, key: Bytes, opt: ReadOption) -> Result<Option<Entry>> {
        let levels = self.levels.clone();
        // todo: through gate
        levels.get(&(ts, key), opt).await
    }

    pub async fn scan(
        &self,
        time_range: TimeRange,
        key_start: Bytes,
        key_end: Bytes,
        opt: ScanOption,
        cmp: Arc<dyn Fn(&[u8], &[u8]) -> Ordering>,
    ) -> Result<()> {
        let (tx, rx) = bounded_channel(opt.prefetch_buf_size);
        let levels = self.levels.clone();

        // todo: through gate
        self.levels
            .scan(time_range, key_start, key_end, tx, cmp)
            .await?;

        todo!("iterator interface")
    }
}

pub enum Task {
    // todo: add put option
    Put(Vec<Entry>, Notifier<Result<()>>),
    Get(
        Timestamp,
        Bytes,
        Notifier<Result<Option<Entry>>>,
        ReadOption,
    ),
    /// time range, start key, end key, result sender, comparator
    Scan(
        TimeRange,
        Bytes,
        Bytes,
        Sender<Vec<Entry>>,
        Arc<dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync>,
    ),
    Shutdown,
}

// todo: finish this
impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HelixTask").finish()
    }
}

#[cfg(test)]
mod test {
    use futures_util::future::select_all;
    use glommio::channels::channel_mesh::{Full, MeshBuilder};
    use glommio::{LocalExecutor, LocalExecutorBuilder};
    use tempfile::tempdir;

    use super::*;

    // todo: investigate this. The receiver will receive lots of message from "peer
    // 0" without any sender.
    #[test]
    #[ignore]
    fn channel_mesh_select_recv_loop() {
        let mesh_builder: MeshBuilder<(), Full> = MeshBuilder::full(8, 2);

        for _ in 0..7 {
            let mesh_builder = mesh_builder.clone();
            LocalExecutorBuilder::new()
                .spawn(move || async move {
                    let (_, mut rx) = mesh_builder.join().await.unwrap();

                    let connected_receivers: Vec<_> =
                        rx.streams().into_iter().map(|(_, rx)| rx).collect();

                    loop {
                        let recvs = connected_receivers
                            .iter()
                            .map(|rx| {
                                let fut = rx.recv();
                                Box::pin(fut)
                            })
                            .collect::<Vec<_>>();

                        let action = select_all(recvs).await;
                        let (_, index, _) = action;
                        println!("{} received action from {}", rx.peer_id(), index);
                    }
                })
                .unwrap();
        }

        LocalExecutorBuilder::new()
            .spawn(move || async move {
                let (tx, _) = mesh_builder.join().await.unwrap();

                for _ in 0..4 {
                    for peer in 0..7 {
                        tx.send_to(peer, ()).await.unwrap();
                    }
                    println!("finished once")
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn standalone_worker_example() {
        let ex = LocalExecutor::default();
        ex.run(async {
            let base_dir = tempdir().unwrap();
            let handle = IOWorker::try_new_standalone(base_dir.path(), Options::default())
                .await
                .unwrap();

            let entry = Entry {
                timestamp: 0,
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            };
            handle.put(vec![entry.clone()]).await.unwrap();

            let result = handle
                .get(0, b"key".to_vec(), ReadOption::default())
                .await
                .unwrap();
            assert_eq!(result.unwrap(), entry);
        })
    }
}
