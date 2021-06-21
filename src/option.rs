use crate::cache::CacheConfig;
use crate::fn_registry::FnRegistry;
use crate::level::{SimpleTimestampReviewer, TimestampReviewer, WriteBatchConfig};

/// Options for opening HelixDB
// #[derive(Debug, Clone,Copy)]
pub struct Options {
    // parameters
    /// Number of shards. It is recommended to equal to the number of system processors.
    pub(crate) num_shard: usize,
    /// Queue length of each shard's task receiver.
    pub(crate) task_buffer_size: usize,
    /// Configurations of cache.
    pub(crate) cache: CacheConfig,
    ///
    pub(crate) write_batch: WriteBatchConfig,

    // helixdb context
    pub(crate) fn_registry: Option<FnRegistry>,
    pub(crate) tsr: Option<Box<dyn TimestampReviewer>>,
}

impl Clone for Options {
    /// a
    fn clone(&self) -> Self {
        Self {
            num_shard: self.num_shard,
            task_buffer_size: self.task_buffer_size,
            cache: self.cache,
            write_batch: self.write_batch,

            fn_registry: None,
            tsr: None,
        }
    }
}

impl Options {
    pub fn default() -> Self {
        Self {
            num_shard: 8,
            task_buffer_size: 128,
            cache: CacheConfig::default(),
            write_batch: WriteBatchConfig::default(),

            fn_registry: Some(FnRegistry::new_noop()),
            tsr: Some(Box::new(SimpleTimestampReviewer::new(1024, 1024 * 8))),
        }
    }

    /// Returns a copy of the value.
    /// This function may not works as expected. It is a "partial" clone.
    ///
    /// Some fields in this [`Options`] struct isn't suit for clone, like `fn_registry` or `tsr`.
    /// They are wrapped by a `Option`, and will only leave a `None` after called `clone_partial()`.
    ///
    /// This is to making [`Options`] more general and unified. Other fields works as what common
    /// [`std::marker::Clone`] does.
    /// # Example
    /// *Just a example and it isn't runnable since `fn_registry` is a private field.*
    /// ```compile_fail
    /// # use helixdb::option::Options;
    /// let options = Options::default();
    /// assert!(options.fn_registry.is_some());
    /// // after calling `clone_partial()` some will be `None` because they won't be cloned actually.
    /// let options_cloned = options.clone_partial();
    /// assert!(options_cloned.fn_registry.is_none());
    /// ```
    pub fn clone_partial(&self) -> Self {
        Self {
            num_shard: self.num_shard,
            task_buffer_size: self.task_buffer_size,
            cache: self.cache,
            write_batch: self.write_batch,

            fn_registry: None,
            tsr: None,
        }
    }

    pub fn shards(mut self, num_shard: usize) -> Self {
        self.num_shard = num_shard;
        self
    }

    pub fn set_fn_registry(mut self, fn_registry: FnRegistry) -> Self {
        self.fn_registry = Some(fn_registry);
        self
    }

    pub fn cache<F>(mut self, f: F) -> Self
    where
        F: FnOnce(CacheConfig) -> CacheConfig,
    {
        self.cache = f(self.cache);
        self
    }

    pub fn write_batch<F>(mut self, f: F) -> Self
    where
        F: FnOnce(WriteBatchConfig) -> WriteBatchConfig,
    {
        self.write_batch = f(self.write_batch);
        self
    }

    pub fn set_timestamp_reviewer(mut self, tsr: Box<dyn TimestampReviewer>) -> Self {
        self.tsr = Some(tsr);
        self
    }

    pub fn set_task_buffer_size(mut self, buffer_size: usize) -> Self {
        self.task_buffer_size = buffer_size;
        self
    }
}

#[derive(Clone, Copy)]
pub struct ReadOption {
    /// Read request will decompress a compressed value then try to find requested timestamp
    /// if true. Default value: true.
    pub(crate) decompress: bool,
}

impl Default for ReadOption {
    fn default() -> Self {
        Self { decompress: true }
    }
}

impl ReadOption {
    pub fn no_decompress(mut self) -> Self {
        self.decompress = false;
        self
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ScanOption {
    pub prefetch_buf_size: usize,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn partial_clone() {
        let options = Options::default();
        assert!(options.fn_registry.is_some());
        assert!(options.tsr.is_some());

        // after calling `clone()` some will be `None` because they won't be cloned actually.
        let options_cloned = options.clone_partial();
        assert!(options_cloned.fn_registry.is_none());
        assert!(options_cloned.tsr.is_none());
    }
}
