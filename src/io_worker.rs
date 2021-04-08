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
    levels: Levels,
    // todo: add channel mesh
    executor: LocalExecutor,
}

impl IOWorker {
    pub fn try_new(
        tid: ThreadId,
        timestamp_reviewer: Arc<Mutex<Box<dyn TimestampReviewer + 'static>>>,
        ctx: Arc<Context>,
        executor: LocalExecutor,
    ) -> Result<Self> {
        let levels = executor.run(Levels::try_new(tid, timestamp_reviewer, ctx))?;

        Ok(Self {
            tid,
            levels,
            executor,
        })
    }

    pub fn put(&mut self, entries: Vec<Entry>) -> Result<()> {
        self.executor.run(self.levels.put(entries))
    }

    pub fn get(&mut self, time_key: &(Timestamp, Bytes)) -> Result<Option<Entry>> {
        self.executor.run(self.levels.get(time_key))
    }
}
