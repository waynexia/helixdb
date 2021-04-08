use std::collections::HashMap;
use std::convert::TryFrom;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crate::cache::{Cache, CacheConfig, KeyCacheEntry, KeyCacheResult};
use crate::context::Context;
use crate::error::Result;
use crate::file::{Rick, SSTable, TableBuilder, VLog, ValueLogBuilder};
use crate::index::MemIndex;
use crate::types::{Bytes, Entry, LevelId, LevelInfo, ThreadId, Timestamp};

pub struct LevelConfig {
    /// Use one file to store non-Rick (SSTable) entries or not.
    pub sharding_sstable: bool,
    /// Max levels can hold. This option should be greater than 0.
    /// Levels will be L0 to L`max_level` (inclusive).
    /// Might be useless due to TimeStamp Reviewer?
    pub max_level: usize,
    /// The max difference of timestamps inside one level.
    /// Might be useless due to TimeStamp Reviewer?
    pub level_duration: u64,
}

/// APIs require unique reference (&mut self) because this `Level` is designed to be used
/// inside one thread (!Send). The fields should also be !Send if possible.
pub struct Levels {
    tid: ThreadId,
    // todo: remove this mutex
    timestamp_reviewer: Arc<Mutex<Box<dyn TimestampReviewer>>>,
    ctx: Arc<Context>,
    memindex: MemIndex,
    rick: Rick,
    level_info: LevelInfo,
    cache: Cache,
}

impl Levels {
    pub async fn try_new(
        tid: ThreadId,
        timestamp_reviewer: Arc<Mutex<Box<dyn TimestampReviewer>>>,
        ctx: Arc<Context>,
    ) -> Result<Self> {
        let rick = Rick::from(ctx.file_manager.open_rick(tid).await?);
        let level_info = ctx.file_manager.open_level_info().await?;

        let cache = Cache::new(CacheConfig::default());

        Ok(Self {
            tid,
            timestamp_reviewer,
            ctx,
            memindex: MemIndex::default(),
            rick,
            level_info,
            cache,
        })
    }

    pub async fn put(&mut self, entries: Vec<Entry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let max_timestamp = entries
            .iter()
            .max_by_key(|entry| entry.timestamp)
            .unwrap()
            .timestamp;

        let indices = self.rick.write(entries).await?;
        self.memindex.insert_entries(indices)?;

        // review timestamp and handle actions.
        let review_actions = self
            .timestamp_reviewer
            .lock()
            .unwrap()
            .observe(max_timestamp);
        self.handle_actions(review_actions).await?;

        Ok(())
    }

    pub async fn get(&mut self, time_key: &(Timestamp, Bytes)) -> Result<Option<Entry>> {
        let level = self.level_info.get_level_id(time_key.0);
        match level {
            None => Ok(None),
            Some(0) => self.get_from_rick(time_key).await,
            Some(l) => self.get_from_table(time_key, l).await,
        }
    }

    #[inline]
    async fn get_from_rick(&mut self, time_key: &(Timestamp, Bytes)) -> Result<Option<Entry>> {
        if let Some(offset) = self.memindex.get(time_key)? {
            let entry = self.rick.read(offset).await?;

            return Ok(Some(entry));
        }

        Ok(None)
    }

    // todo: refine
    #[inline]
    async fn get_from_table(
        &mut self,
        time_key: &(Timestamp, Bytes),
        level_id: LevelId,
    ) -> Result<Option<Entry>> {
        let mut key_cache_entry = KeyCacheEntry::new(time_key);

        let entry = match self.cache.get_key(time_key) {
            KeyCacheResult::Value(value) => {
                return Ok(Some(Entry {
                    timestamp: time_key.0,
                    key: time_key.1.to_owned(),
                    value,
                }))
            }
            KeyCacheResult::Compressed(compressed) => {
                let entries = self
                    .ctx
                    .fn_registry
                    .decompress_entries(&time_key.1, &compressed)?;

                let index = match entries
                    .binary_search_by_key(&time_key.0, |(ts, _)| *ts)
                    .ok()
                {
                    Some(thing) => thing,
                    None => return Ok(None),
                };
                let (timestamp, value) = &entries[index];
                key_cache_entry.value = Some(value);
                key_cache_entry.compressed = Some(&compressed);
                self.cache.put_key(key_cache_entry);

                Ok(Some(Entry {
                    timestamp: *timestamp,
                    key: time_key.1.clone(),
                    value: value.clone(),
                }))
            }
            KeyCacheResult::Position(tid, level_id, offset, size) => {
                let vlog = VLog::from(self.ctx.file_manager.open_vlog(tid, level_id).await?);
                let raw_bytes = vlog.get(offset as u64, size as u64).await?;

                let entries = self
                    .ctx
                    .fn_registry
                    .decompress_entries(&time_key.1, &raw_bytes)?;

                let index = match entries
                    .binary_search_by_key(&time_key.0, |(ts, _)| *ts)
                    .ok()
                {
                    Some(thing) => thing,
                    None => return Ok(None),
                };
                let (timestamp, value) = &entries[index];

                key_cache_entry.value = Some(value);
                key_cache_entry.compressed = Some(&raw_bytes);
                self.cache.put_key(key_cache_entry);

                Ok(Some(Entry {
                    timestamp: *timestamp,
                    key: time_key.1.clone(),
                    value: value.clone(),
                }))
            }
            KeyCacheResult::NotFound => {
                let handle = if let Some(handle) =
                    self.cache.get_table_handle(&(self.tid, level_id).into())
                {
                    handle
                } else {
                    let table_file = self
                        .ctx
                        .file_manager
                        .open_sstable(self.tid, level_id)
                        .await?;
                    let handle = SSTable::new(table_file, self.tid, level_id)
                        .handle(self.ctx.clone())
                        .await?;

                    let handle = Rc::new(handle);
                    self.cache
                        .put_table_handle((self.tid, level_id).into(), handle.clone());
                    handle
                };

                let entry = handle.get(time_key).await?;
                if let Some(entry) = &entry {
                    key_cache_entry.value = Some(&entry.value);
                    self.cache.put_key(key_cache_entry);
                }
                Ok(entry)
            }
        };

        entry
    }

    async fn handle_actions(&mut self, actions: Vec<TimestampAction>) -> Result<()> {
        for action in actions {
            match action {
                TimestampAction::Compact(start_ts, end_ts) => {
                    let next_level_id = self
                        .level_info
                        .add_level(start_ts, end_ts, &self.ctx.file_manager)
                        .await?;
                    self.compact(start_ts, end_ts, next_level_id).await?;
                }
                TimestampAction::Outdate(_) => self.outdate().await?,
            }
        }

        Ok(())
    }

    /// Compact entries from rick in [start_ts, end_ts] to next level.
    ///
    /// todo: how to handle rick file is not fully covered by given time range?.
    async fn compact(
        &mut self,
        start_ts: Timestamp,
        end_ts: Timestamp,
        level_id: LevelId,
    ) -> Result<()> {
        let mut table_builder = TableBuilder::from(
            self.ctx
                .file_manager
                .open_sstable(self.tid, level_id)
                .await?,
        );
        let vlog_builder = self.ctx.file_manager.open_vlog(self.tid, level_id).await?;
        let mut vlog_builder = ValueLogBuilder::try_from(vlog_builder)?;
        let mut value_positions = vec![];

        // make entry_map
        let entries = self.rick.entry_list().await?;
        let mut entry_map = HashMap::new();
        for entry in entries {
            let Entry {
                timestamp,
                key,
                value,
            } = entry;

            let pair_list: &mut Vec<_> = entry_map.entry(key).or_default();
            pair_list.push((timestamp, value));
        }

        // call compress_fn to compact points.
        let mut keys = Vec::with_capacity(entry_map.len());
        for (key, ts_value) in entry_map {
            let compress_fn_name = self.ctx.fn_registry.dispatch_fn()(&key);
            let compress_fn = self.ctx.fn_registry.udcf(compress_fn_name)?.compress();
            let compressed_data = compress_fn(key.clone(), ts_value);

            let (offset, size) = vlog_builder.add_entry(compressed_data).await?;
            value_positions.push((offset, size));
            keys.push(key);
        }

        // make sstable
        table_builder.add_entries(keys, value_positions);
        table_builder.finish().await?;

        self.rick.clean().await?;

        Ok(())
    }

    async fn outdate(&mut self) -> Result<()> {
        self.level_info
            .remove_last_level(&self.ctx.file_manager)
            .await?;

        todo!()
    }
}

/// "Timestamp" in `HelixDB` is a logical concept. It is not bound with the real
/// time. [TimestampReviewer] defines how timestamp should be considered. Including
/// when to do a compaction, when to outdate a part of data etc.
pub trait TimestampReviewer {
    fn observe(&mut self, timestamp: Timestamp) -> Vec<TimestampAction>;
}

/// Actions given by [TimestampReviewer].
#[derive(Debug, PartialEq, Eq)]
pub enum TimestampAction {
    /// Compact data between two timestamps (both inclusive).
    Compact(Timestamp, Timestamp),
    /// Outdate data which timestamp is smaller than given.
    Outdate(Timestamp),
}

/// A simple timestamp review implementation. It has two config entries
/// `rick_range` and `outdate_range`. `rick_range` defines the range of
/// rick and sstable files. `outdate_range` defines how much data should
/// be kept. `outdate_range` should be integer times of `rick_range` even
/// if it is unchecked.
///
/// This implementation is not bound with real world time. It assume the
/// timestamp comes from `observe()` call is the newest. And just triggers
/// compaction and outdate only based on this. In real scenario
/// when timestamp has more meaning or restriction, more complex logic can
/// be achieved.
pub struct SimpleTimestampReviewer {
    // config part
    rick_range: Timestamp,
    outdate_range: Timestamp,

    // status part
    last_compacted: Timestamp,
    last_outdated: Timestamp,
}

impl SimpleTimestampReviewer {
    pub fn new(rick_range: Timestamp, outdate_range: Timestamp) -> Self {
        Self {
            rick_range,
            outdate_range,
            last_compacted: 0,
            last_outdated: 0,
        }
    }
}

impl TimestampReviewer for SimpleTimestampReviewer {
    fn observe(&mut self, timestamp: Timestamp) -> Vec<TimestampAction> {
        let mut actions = vec![];
        if timestamp - self.last_compacted + 1 >= self.rick_range {
            actions.push(TimestampAction::Compact(self.last_compacted, timestamp));
            self.last_compacted = timestamp + 1;
        }
        if timestamp - self.last_outdated + 1 >= self.outdate_range {
            actions.push(TimestampAction::Outdate(
                self.last_outdated + self.rick_range - 1,
            ));
            self.last_outdated += self.rick_range;
        }

        actions
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::file::FileManager;
    use crate::fn_registry::FnRegistry;

    use glommio::LocalExecutor;
    use tempfile::tempdir;

    #[tokio::test]
    async fn simple_timestamp_reviewer_trigger_compact_and_outdate() {
        let mut tsr = SimpleTimestampReviewer::new(10, 30);

        let mut actions = vec![];
        let expected = vec![
            TimestampAction::Compact(0, 9),
            TimestampAction::Compact(10, 19),
            TimestampAction::Compact(20, 29),
            TimestampAction::Outdate(9),
            TimestampAction::Compact(30, 39),
            TimestampAction::Outdate(19),
        ];

        for i in 0..40 {
            actions.append(&mut tsr.observe(i));
        }

        assert_eq!(actions, expected);
    }

    #[test]
    fn put_get_on_rick() {
        let ex = LocalExecutor::default();
        ex.run(async {
            let base_dir = tempdir().unwrap();
            let file_manager = FileManager::with_base_dir(base_dir.path()).unwrap();
            let fn_registry = FnRegistry::new_noop();
            let ctx = Arc::new(Context {
                file_manager,
                fn_registry,
            });
            let timestamp_reviewer: Arc<Mutex<Box<dyn TimestampReviewer>>> =
                Arc::new(Mutex::new(Box::new(SimpleTimestampReviewer::new(10, 30))));
            let mut levels = Levels::try_new(1, timestamp_reviewer, ctx).await.unwrap();

            let entries = vec![
                (1, b"key1".to_vec(), b"value1".to_vec()).into(),
                (2, b"key1".to_vec(), b"value1".to_vec()).into(),
                (3, b"key1".to_vec(), b"value1".to_vec()).into(),
                (1, b"key2".to_vec(), b"value2".to_vec()).into(),
                (2, b"key2".to_vec(), b"value2".to_vec()).into(),
                (3, b"key3".to_vec(), b"value1".to_vec()).into(),
            ];

            levels.put(entries.clone()).await.unwrap();

            for entry in entries {
                assert_eq!(entry, levels.get(entry.time_key()).await.unwrap().unwrap());
            }

            // overwrite a key
            let new_entry: Entry = (1, b"key1".to_vec(), b"value3".to_vec()).into();
            levels.put(vec![new_entry.clone()]).await.unwrap();
            assert_eq!(
                new_entry,
                levels.get(new_entry.time_key()).await.unwrap().unwrap()
            );
        });
    }

    #[test]
    fn put_get_with_compaction() {
        let ex = LocalExecutor::default();
        ex.run(async {
            let base_dir = tempdir().unwrap();
            let file_manager = FileManager::with_base_dir(base_dir.path()).unwrap();
            let fn_registry = FnRegistry::new_noop();
            let ctx = Arc::new(Context {
                file_manager,
                fn_registry,
            });
            let timestamp_reviewer: Arc<Mutex<Box<dyn TimestampReviewer>>> =
                Arc::new(Mutex::new(Box::new(SimpleTimestampReviewer::new(10, 30))));
            let mut levels = Levels::try_new(1, timestamp_reviewer, ctx.clone())
                .await
                .unwrap();

            for timestamp in 0..25 {
                levels
                    .put(vec![(timestamp, b"key".to_vec(), b"value".to_vec()).into()])
                    .await
                    .unwrap();
            }

            for timestamp in 0..25 {
                levels
                    .get(&(timestamp, b"key".to_vec()))
                    .await
                    .unwrap()
                    .unwrap();
            }
        });
    }
}
