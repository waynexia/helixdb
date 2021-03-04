use crate::error::Result;
use crate::types::{Bytes, Entry, Timestamp};

use std::collections::{btree_map, BTreeMap};

#[derive(Default, Debug)]
pub struct MemIndex {
    pub index: BTreeMap<(Timestamp, Bytes), u64>,
}

impl MemIndex {
    pub fn insert_entries(&mut self, entries: Vec<(Timestamp, Bytes, u64)>) -> Result<()> {
        for entry in entries {
            let (timestamp, key, value) = entry;
            self.index.insert((timestamp, key), value);
        }

        Ok(())
    }

    pub fn get(&self, time_key: &(Timestamp, Bytes)) -> Result<Option<u64>> {
        Ok(self.index.get(time_key).copied())
    }

    pub fn into_iter(self) -> btree_map::IntoIter<(i64, std::vec::Vec<u8>), u64> {
        self.index.into_iter()
    }
}
