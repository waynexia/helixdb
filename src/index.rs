use crate::entry::{Bytes, Entry, Timestamp};
use crate::error::Result;
use crate::io_worker::{IOWorker, ValuePosition};

use std::collections::BTreeMap;

pub struct MemIndex {
    pub index: BTreeMap<(Timestamp, Bytes), ValuePosition>,
    pub worker: IOWorker,
}

impl MemIndex {
    pub fn put(&mut self, write_batch: Vec<Entry>) -> Result<()> {
        let indices = self.worker.write(write_batch)?;
        for (timestamp, key, position) in indices {
            self.index.insert((timestamp, key), position);
        }

        Ok(())
    }

    pub fn get(&mut self, timestamp: Timestamp, key: Bytes) -> Result<Option<Entry>> {
        if let Some(position) = self.index.get(&(timestamp, key)) {
            let result = self.worker.read(position)?;
            Ok(result)
        } else {
            Ok(None)
        }
    }
}
