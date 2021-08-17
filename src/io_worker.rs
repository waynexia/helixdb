use std::cmp::Ordering;
use std::ptr;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use glommio::channels::channel_mesh::{
    Receivers as ChannelMeshReceiver,
    Senders as ChannelMeshSender,
};
use glommio::sync::Gate;
use glommio::{Latency, Local, Shares, Task as GlommioTask};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot::Sender as Notifier;
use tokio::sync::Mutex;
use tracing::trace;

use crate::compact_sched::{CompactScheduler, QueueUpCompSched};
use crate::context::Context;
use crate::error::Result;
use crate::level::{Levels, TimestampReviewer};
use crate::option::{Options, ReadOption};
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

/// A un-Send handle to accept and process requests.
pub struct IOWorker {
    tid: ThreadId,
    levels: Rc<Levels<QueueUpCompSched>>,
    // todo: maybe add channel mesh for scan
}

impl IOWorker {
    pub async fn try_new(
        tid: ThreadId,
        opts: Options,
        timestamp_reviewer: Arc<Mutex<Box<dyn TimestampReviewer + 'static>>>,
        level_info: Arc<Mutex<LevelInfo>>,
        ctx: Arc<Context>,
        ts_action_sender: ChannelMeshSender<TimestampAction>,
    ) -> Result<Self> {
        let level_weak = Weak::new();
        let compact_task_queue =
            Local::create_task_queue(Shares::default(), Latency::NotImportant, "compact_tq");
        let sched = Rc::new(QueueUpCompSched::new(
            opts.compact_prompt_interval,
            2,
            level_weak.clone(),
            compact_task_queue,
        ));

        let levels = Levels::try_new(
            tid,
            opts.clone(),
            timestamp_reviewer,
            ctx,
            ts_action_sender,
            level_info,
            sched.clone(),
        )
        .await?;

        unsafe {
            ptr::write(level_weak.as_ptr() as _, Rc::as_ptr(&levels));
        }
        sched.install(compact_task_queue)?;

        Ok(Self { tid, levels })
    }

    /// Won't return until shut down.
    pub async fn run(
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
    use glommio::LocalExecutorBuilder;

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
}
