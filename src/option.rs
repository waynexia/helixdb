use crate::fn_registry::FnRegistry;
use crate::level::{SimpleTimestampReviewer, TimestampAction, TimestampReviewer};

pub struct Options {
    // parameters
    /// Number of shards. It is recommended to equal to the number of system processors.
    pub(crate) num_shard: usize,
    /// Queue length of each shard's task receiver.
    pub(crate) task_buffer_size: usize,
    // helixdb context
    pub(crate) fn_registry: FnRegistry,
    pub(crate) tsr: Box<dyn TimestampReviewer>,
}

impl Options {
    pub fn default() -> Self {
        Self {
            num_shard: 8,
            task_buffer_size: 128,

            fn_registry: FnRegistry::new_noop(),
            tsr: Box::new(SimpleTimestampReviewer::new(1024, 1024 * 8)),
        }
    }

    pub fn shards(mut self, num_shard: usize) -> Self {
        self.num_shard = num_shard;
        self
    }

    pub fn set_fn_registry(mut self, fn_registry: FnRegistry) -> Self {
        self.fn_registry = fn_registry;
        self
    }

    pub fn set_timestamp_reviewer(mut self, tsr: Box<dyn TimestampReviewer>) -> Self {
        self.tsr = tsr;
        self
    }
}
