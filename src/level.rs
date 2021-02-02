use std::collections::BTreeMap;
use std::fs::File;
use std::{collections::btree_map, sync::Arc};

use crate::entry::{Bytes, Timestamp};

pub type CompactEntries = BTreeMap<(Timestamp, Bytes), Vec<Bytes>>;
pub type CompactedEntry = BTreeMap<(Timestamp, Bytes), Bytes>;

/// Custom compaction function. This will be called when compacting L0
/// files to L1.
pub type CompactionFn = Arc<dyn Fn(CompactEntries) -> CompactedEntry + Send + Sync>;

pub struct LevelConfig {
    /// Use one file to store non-L0 entries or not.
    pub sharding_non_l0_file: bool,
    /// Max levels can hold. This option should be greater than 0.
    /// Levels will be L0 to L`max_level` (inclusive).
    pub max_level: usize,
    /// The max difference of timestamp inside one level.
    pub level_duration: u64,
}

pub struct Levels {
    base_dir: String,
    curr_l0_file: File,
    levels: Vec<Arc<File>>,
}
