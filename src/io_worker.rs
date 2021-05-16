use std::cmp::Ordering;
use std::rc::Rc;
use std::sync::Arc;

use glommio::channels::channel_mesh::{
    Receivers as ChannelMeshReceiver,
    Senders as ChannelMeshSender,
};
use glommio::Task as GlommioTask;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot::Sender as Notifier;
use tokio::sync::Mutex;
use tracing::trace;

use crate::context::Context;
use crate::error::Result;
use crate::level::{Levels, TimestampReviewer};
use crate::option::ReadOption;
use crate::types::{Bytes, Entry, ThreadId, TimeRange, Timestamp};
use crate::TimestampAction;

/// A un-Send handle to accept and process requests.
pub struct IOWorker {
    tid: ThreadId,
    levels: Rc<Levels>,
    // todo: maybe add channel mesh for scan
}

impl IOWorker {
    pub async fn try_new(
        tid: ThreadId,
        timestamp_reviewer: Arc<Mutex<Box<dyn TimestampReviewer + 'static>>>,
        ctx: Arc<Context>,
        ts_action_sender: ChannelMeshSender<TimestampAction>,
    ) -> Result<Self> {
        let levels = Levels::try_new(tid, timestamp_reviewer, ctx, ts_action_sender).await?;

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
        while let Some(task) = rx.recv().await {
            match task {
                Task::Put(entries, tx) => {
                    let levels = self.levels.clone();
                    GlommioTask::local(async move {
                        levels.put(entries, tx).await;
                    })
                    .detach();
                }
                Task::Get(ts, key, tx, opt) => {
                    let levels = self.levels.clone();
                    GlommioTask::local(async move {
                        let result = levels.get(&(ts, key), opt).await;
                        let _ = tx.send(result);
                    })
                    .detach();
                }
                Task::Scan(time_range, key_start, key_end, sender, cmp) => {
                    let levels = self.levels.clone();
                    GlommioTask::local(async move {
                        let _ = levels
                            .scan(time_range, key_start, key_end, sender, cmp)
                            .await;
                    })
                    .detach();
                }
                Task::Shutdown => break,
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

    // todo: investigate this. The receiver will receive lots of message from "peer 0" without any sender.
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
