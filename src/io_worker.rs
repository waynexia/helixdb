use glommio::LocalExecutor;
use std::future::Future;
use std::sync::{Arc, Mutex};

use crate::context::Context;
use crate::error::Result;
use crate::level::{Levels, TimestampReviewer};
use crate::types::{Bytes, Entry, ThreadId, Timestamp};

/// A un-Send handle to accept and process requests.
pub struct IOWorker {
    tid: ThreadId,
    ctx: Arc<Context>,
    levels: Levels,
    // todo: add channel mesh
    executor: LocalExecutor,
}

impl IOWorker {
    pub fn try_new(
        tid: ThreadId,
        timestamp_reviewer: Arc<Mutex<dyn TimestampReviewer>>,
        ctx: Arc<Context>,
        executor: LocalExecutor,
    ) -> Result<Self> {
        let levels = executor.run(Levels::try_new(tid, timestamp_reviewer, ctx.clone()))?;

        Ok(Self {
            tid,
            ctx,
            levels,
            executor,
        })
    }

    pub fn put(&mut self, entries: Vec<Entry>) -> impl Future<Output = Result<()>> {
        let result = self.executor.run(self.levels.put(entries));

        async move { result }
    }

    pub fn get(
        &mut self,
        time_key: &(Timestamp, Bytes),
    ) -> impl Future<Output = Result<Option<Entry>>> {
        let result = self.executor.run(self.levels.get(time_key));

        async move { result }
    }
}
