use std::cmp::Ordering;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use glommio::Task as GlommioTask;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot::Sender as Notifier;
use tokio::sync::Mutex;

use crate::context::Context;
use crate::error::Result;
use crate::level::{Levels, TimestampReviewer};
use crate::option::ReadOption;
use crate::types::{Bytes, Entry, ThreadId, TimeRange, Timestamp};

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
    ) -> Result<Self> {
        let levels = Levels::try_new(tid, timestamp_reviewer, ctx).await?;
        let levels = levels;

        Ok(Self { tid, levels })
    }

    /// Won't return until shut down.
    pub async fn run(self, mut rx: Receiver<Task>) {
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
