#[cfg(test)]
use std::collections::btree_map;
use std::collections::{BTreeMap, HashMap};
use std::ops::AddAssign;

use crate::error::Result;
use crate::types::{Bytes, Offset, TimeRange, Timestamp};

#[derive(Default, Debug)]
pub struct MemIndex {
    /// (timestamp, key) => value's position in rick file.
    pub index: BTreeMap<(Timestamp, Bytes), Offset>,
    /// Counting user key.
    pub user_keys: HashMap<Bytes, usize>,
}

impl MemIndex {
    pub fn from_existing(index: BTreeMap<(Timestamp, Bytes), u64>) -> Self {
        let user_keys = HashMap::new();
        let mut result = Self {
            index: BTreeMap::new(),
            user_keys,
        };

        for (_, user_key) in index.keys() {
            result.update_user_key(user_key);
        }
        result.index = index;

        result
    }

    pub fn insert(&mut self, entry: (Timestamp, Bytes, u64)) -> Result<()> {
        let (timestamp, key, value) = entry;
        self.update_user_key(&key);
        self.index.insert((timestamp, key), value);

        Ok(())
    }

    pub fn insert_entries(&mut self, entries: Vec<(Timestamp, Bytes, u64)>) -> Result<()> {
        for entry in entries {
            let (timestamp, key, value) = entry;
            self.update_user_key(&key);
            self.index.insert((timestamp, key), value);
        }

        Ok(())
    }

    pub fn get(&self, time_key: &(Timestamp, Bytes)) -> Result<Option<Offset>> {
        Ok(self.index.get(time_key).copied())
    }

    #[cfg(test)]
    pub fn into_iter(self) -> btree_map::IntoIter<(i64, std::vec::Vec<u8>), u64> {
        self.index.into_iter()
    }

    /// Get all existing user keys.
    pub fn user_keys(&self) -> Vec<Bytes> {
        self.user_keys.keys().cloned().collect()
    }

    pub fn load_time_range(&self, range: TimeRange) -> Vec<u64> {
        let mut offsets = vec![];
        for ((ts, _), offset) in &self.index {
            if range.contains(*ts) {
                offsets.push(*offset);
            }
        }

        offsets
    }

    pub fn purge_time_range(&mut self, range: TimeRange) {
        self.index.retain(|(ts, _), _| !range.contains(*ts));
    }

    fn update_user_key(&mut self, user_key: &[u8]) {
        if !self.user_keys.contains_key(user_key) {
            self.user_keys.insert(user_key.to_vec(), 1);
        } else {
            self.user_keys.get_mut(user_key).unwrap().add_assign(1);
        }
    }
}
