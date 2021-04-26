use std::rc::Rc;
use std::sync::Arc;

use glommio::Task as GlommioTask;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;

use crate::context::Context;
use crate::error::Result;
use crate::level::{Levels, TimestampReviewer};
use crate::types::{Bytes, Entry, ThreadId, Timestamp};

/// A un-Send handle to accept and process requests.
pub struct IOWorker {
    tid: ThreadId,
    levels: Rc<Levels>,
    // todo: maybe add channel mesh
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
                Task::Get(ts, key, tx) => {
                    let levels = self.levels.clone();
                    GlommioTask::local(async move {
                        let result = levels.get(&(ts, key)).await;
                        let _ = tx.send(result);
                    })
                    .detach();
                }
                Task::Scan => unimplemented!(),
                Task::Shutdown => break,
            }
        }
    }
}

#[derive(Debug)]
pub enum Task {
    // todo: add put option
    Put(Vec<Entry>, Sender<Result<()>>),
    Get(Timestamp, Bytes, Sender<Result<Option<Entry>>>),
    Scan,
    Shutdown,
}
