use std::sync::{Arc, Mutex};

use crate::context::Context;
use crate::error::Result;
use crate::level::{Levels, TimestampReviewer};
use crate::types::{Bytes, Entry, ThreadId, Timestamp};

/// A un-Send handle to accept and process requests.
pub struct IOWorker {
    levels: Levels,
}

impl IOWorker {
    pub async fn try_new(
        tid: ThreadId,
        timestamp_reviewer: Arc<Mutex<dyn TimestampReviewer>>,
        ctx: Arc<Context>,
    ) -> Result<Self> {
        let levels = Levels::try_new(tid, timestamp_reviewer, ctx).await?;

        Ok(Self { levels })
    }

    pub async fn put(&mut self, entries: Vec<Entry>) -> Result<()> {
        todo!()
    }

    pub async fn get(&mut self, time_key: &(Timestamp, Bytes)) -> Result<Option<Entry>> {
        todo!()
    }
}
