use std::collections::BTreeMap;

use crate::context::Context;
use crate::entry::{Bytes, Timestamp};
use crate::error::{HelixError, Result};
use crate::file::VLog;
use crate::iterator::Iterator;

pub struct TableIterator {
    /// map of key to (value offset, value size)
    index: BTreeMap<Bytes, (u64, u64)>,
    curr_key: Bytes,
    vlog: VLog,
}

impl TableIterator {
    pub fn try_new(
        index: BTreeMap<Bytes, (u64, u64)>,
        vlog_filename: String,
        ctx: &Context,
    ) -> Result<Self> {
        // let vlog =
        let file_manager = &ctx.file_manager;
        let vlog = VLog::from(file_manager.open(vlog_filename)?);
        Ok(Self {
            index,
            curr_key: vec![],
            vlog,
        })
    }
}

impl Iterator for TableIterator {
    fn seek(&mut self, _timestamp: Timestamp, key: Bytes) -> Result<()> {
        if !self.index.contains_key(&key) {
            return Err(crate::error::HelixError::NotFound);
        }
        self.curr_key = key;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        todo!()
    }

    fn timestamp(&self) -> Result<Timestamp> {
        todo!()
    }

    fn key(&self) -> Result<&Bytes> {
        Ok(&self.curr_key)
    }

    fn value(&self) -> Result<Bytes> {
        let (offset, length) = self.index.get(&self.curr_key).ok_or(HelixError::NotFound)?;
        let value = self.vlog.get(*offset, *length)?;
        Ok(value)
    }
}
