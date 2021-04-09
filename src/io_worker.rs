use glommio::Task as GlommioTask;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
// use tokio::sync::mpsc::Receiver;
use crossbeam_channel::Receiver;
use tokio::sync::oneshot::Sender;

use crate::context::Context;
use crate::error::Result;
use crate::level::{Levels, TimestampReviewer};
use crate::types::{Bytes, Entry, ThreadId, Timestamp};

/// A un-Send handle to accept and process requests.
pub struct IOWorker {
    tid: ThreadId,
    levels: Rc<Mutex<Levels>>,
    // todo: maybe add channel mesh
}

impl IOWorker {
    pub async fn try_new(
        tid: ThreadId,
        timestamp_reviewer: Arc<Mutex<Box<dyn TimestampReviewer + 'static>>>,
        ctx: Arc<Context>,
    ) -> Result<Self> {
        let levels = Levels::try_new(tid, timestamp_reviewer, ctx).await?;
        let levels = Rc::new(Mutex::new(levels));

        Ok(Self { tid, levels })
    }

    /// Won't return until shut down.
    pub async fn run(self, rx: Receiver<Task>) {
        while let Ok(task) = rx.recv() {
            match task {
                Task::Put(entries, tx) => {
                    let levels = self.levels.clone();
                    GlommioTask::local(async move {
                        let result = levels.lock().unwrap().put(entries).await;
                        let _ = tx.send(result);
                    })
                    .detach()
                    // todo: find out why this hang without .await
                    .await;
                }
                Task::Get(ts, key, tx) => {
                    let levels = self.levels.clone();
                    GlommioTask::local(async move {
                        let result = levels.lock().unwrap().get(&(ts, key)).await;
                        let _ = tx.send(result);
                    })
                    .detach()
                    .await;
                }
                Task::Scan => unimplemented!(),
                Task::Shutdown => break,
            }
        }
    }
}

#[derive(Debug)]
pub enum Task {
    Put(Vec<Entry>, Sender<Result<()>>),
    Get(Timestamp, Bytes, Sender<Result<Option<Entry>>>),
    Scan,
    Shutdown,
}
