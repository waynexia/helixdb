use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

use crate::entry::{Bytes, Entry, Timestamp};
use crate::error::Result;
use crate::file::{FileManager, FileType, Rick, TableBuilder, ValueLogBuilder};
use crate::fn_registry::FnRegistry;
use crate::index::MemIndex;

pub struct LevelConfig {
    /// Use one file to store non-L0 entries or not.
    pub sharding_non_l0_file: bool,
    /// Max levels can hold. This option should be greater than 0.
    /// Levels will be L0 to L`max_level` (inclusive).
    pub max_level: usize,
    /// The max difference of timestamps inside one level.
    pub level_duration: u64,
}

/// APIs require unique reference (&mut self) because this `Level` is designed to be used
/// inside one thread (!Send). The fields should also be !Send if possible.
pub struct Levels {
    // todo: remove this mutex
    timestamp_reviewer: Arc<Mutex<dyn TimestampReviewer>>,
    fn_registry: FnRegistry,
    file_manager: Arc<FileManager>,
    memindex: MemIndex,
    rick: Rick,
}

impl Levels {
    pub fn try_new(
        timestamp_reviewer: Arc<Mutex<dyn TimestampReviewer>>,
        fn_registry: FnRegistry,
        file_manager: Arc<FileManager>,
    ) -> Result<Self> {
        let (rick, _) = file_manager.create(FileType::Rick)?;
        let rick = Rick::from(rick);

        Ok(Self {
            timestamp_reviewer,
            fn_registry,
            file_manager,
            memindex: MemIndex::default(),
            rick,
        })
    }

    pub fn put(&mut self, entries: Vec<Entry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let max_timestamp = entries
            .iter()
            .max_by_key(|entry| entry.timestamp)
            .unwrap()
            .timestamp;

        let indice = self.rick.write(entries)?;
        self.memindex.insert_entries(indice)?;

        // review timestamp and handle actions.
        let review_actions = self
            .timestamp_reviewer
            .lock()
            .unwrap()
            .observe(max_timestamp);
        self.handle_actions(review_actions)?;

        Ok(())
    }

    pub fn get(&mut self, time_key: &(Timestamp, Bytes)) -> Result<Option<Entry>> {
        if let Some(offset) = self.memindex.get(time_key)? {
            let entry = self.rick.read(offset)?;

            return Ok(Some(entry));
        }

        Ok(None)
    }

    fn handle_actions(&mut self, actions: Vec<TimestampAction>) -> Result<()> {
        for action in actions {
            match action {
                TimestampAction::Compact(start_ts, end_ts) => self.compact(start_ts, end_ts)?,
                TimestampAction::Outdate(_) => todo!(),
            }
        }

        Ok(())
    }

    /// Compact entries from rick in [start_ts, end_ts] to next level.
    ///
    /// todo: how to handle rick file is not fully covered by given time range?.
    fn compact(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> Result<()> {
        let mut table_builder = TableBuilder::from(self.file_manager.create(FileType::SSTable)?.0);
        let (vlog_builder, vlog_filename) = self.file_manager.create(FileType::VLog)?;
        let mut vlog_builder = ValueLogBuilder::try_from(vlog_builder)?;
        let mut value_positions = vec![];

        // make entry_map
        let entries = self.rick.entry_list()?;
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
            let compress_fn_name = self.fn_registry.dispatch_fn()(&key);
            let compress_fn = self.fn_registry.udcf(compress_fn_name)?.compress();
            let compressed_data = compress_fn(key.clone(), ts_value);

            let (offset, size) = vlog_builder.add_entry(compressed_data)?;
            value_positions.push((offset, size));
            keys.push(key);
        }

        // make sstable
        table_builder.add_entries(keys, value_positions);
        table_builder.finish(vlog_filename)?;

        Ok(())
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
struct SimpleTimestampReviewer {
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

    use tempfile::tempdir;

    #[test]
    fn level_put_and_get() {
        let base_dir = tempdir().unwrap();
        let file_manager = Arc::new(FileManager::with_base_dir(base_dir.path()).unwrap());
        let fn_registry = FnRegistry::new_noop();
        let timestamp_reviewer = Arc::new(Mutex::new(SimpleTimestampReviewer::new(10, 30)));
        let mut levels = Levels::try_new(timestamp_reviewer, fn_registry, file_manager).unwrap();

        let entries = vec![
            (1, b"key1".to_vec(), b"value1".to_vec()).into(),
            (2, b"key1".to_vec(), b"value1".to_vec()).into(),
            (3, b"key1".to_vec(), b"value1".to_vec()).into(),
            (1, b"key2".to_vec(), b"value2".to_vec()).into(),
            (2, b"key2".to_vec(), b"value2".to_vec()).into(),
            (3, b"key1".to_vec(), b"value1".to_vec()).into(),
        ];

        levels.put(entries.clone()).unwrap();

        for entry in entries {
            assert_eq!(entry, levels.get(entry.time_key()).unwrap().unwrap());
        }

        // overwrite a key
        let new_entry: Entry = (1, b"key1".to_vec(), b"value3".to_vec()).into();
        levels.put(vec![new_entry.clone()]).unwrap();
        assert_eq!(
            new_entry,
            levels.get(new_entry.time_key()).unwrap().unwrap()
        );
    }

    #[test]
    fn simple_timestamp_reviewer_trigger_compact_and_outdate() {
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
}
