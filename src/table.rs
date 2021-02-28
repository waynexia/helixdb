use std::collections::HashMap;

use crate::context::Context;
use crate::entry::{Bytes, Timestamp};
use crate::error::Result;
use crate::file::{TableMeta, VLog};
use crate::iterator::Iterator;

pub struct TableIterator<'a> {
    /// map of key to (value offset, value size)
    index: HashMap<Bytes, usize>,
    raw_entry_positions: Vec<(Bytes, u64, u64)>,
    _table_meta: TableMeta,
    vlog: VLog,
    ctx: &'a Context,

    // states
    curr_key: Bytes,
    // curr_ts: Timestamp,
    curr_entries: Vec<(Timestamp, Bytes)>,
    curr_key_cursor: usize,
    curr_entry_cursor: usize,
    cache: DecompressCache,
    is_valid: bool,
}

impl<'a> TableIterator<'a> {
    pub fn try_new(
        index: HashMap<Bytes, usize>,
        raw_entry_positions: Vec<(Bytes, u64, u64)>,
        _table_meta: TableMeta,
        vlog_filename: String,
        ctx: &'a Context,
    ) -> Result<Self> {
        let file_manager = &ctx.file_manager;
        let vlog = VLog::from(file_manager.open(vlog_filename)?);
        Ok(Self {
            curr_key: vec![],
            // curr_ts: table_meta.start(),
            curr_entries: vec![],
            curr_key_cursor: 0,
            curr_entry_cursor: 0,
            cache: DecompressCache::default(),
            is_valid: true,

            index,
            raw_entry_positions,
            _table_meta,
            vlog,
            ctx,
        })
    }

    fn decompress_curr_entries(&mut self) -> Result<()> {
        if let Some(entries) = self.cache.get(&self.curr_key) {
            self.curr_entries = entries.clone();
        } else {
            let (_, offset, length) = self.raw_entry_positions[self.curr_key_cursor];
            let raw_entries = self.vlog.get(offset, length)?;

            let decompress_fn_name = self.ctx.fn_registry.dispatch_fn()(&self.curr_key);
            let decompress_fn = self.ctx.fn_registry.udcf(decompress_fn_name)?;
            let entries = decompress_fn.decompress()(self.curr_key.clone(), raw_entries);
            self.cache.insert(self.curr_key.clone(), entries.clone());

            self.curr_entries = entries;
        }

        Ok(())
    }

    fn seek(&mut self, timestamp: Timestamp, key: Bytes) -> Result<()> {
        if let Some(cursor) = self.index.get(&key) {
            self.curr_key_cursor = *cursor;
        } else {
            return Err(crate::error::HelixError::NotFound);
        }

        self.curr_key = key;
        self.decompress_curr_entries()?;

        // todo: handle not found
        for i in 0..self.curr_entries.len() {
            if self.curr_entries[i].0 >= timestamp {
                self.curr_entry_cursor = i;
                break;
            }
        }

        Ok(())
    }

    fn next(&mut self) {
        if !self.is_valid {
            return;
        }

        self.curr_entry_cursor += 1;
        if self.curr_entry_cursor >= self.curr_entries.len() {
            self.curr_key_cursor += 1;
            if self.curr_key_cursor > self.raw_entry_positions.len() {
                self.is_valid = false;
                return;
            }
            self.curr_entry_cursor = 0;
            self.curr_key = self.raw_entry_positions[self.curr_key_cursor].0.to_owned();
        }
    }
}

impl<'a> Iterator for TableIterator<'a> {
    fn seek(&mut self, timestamp: Timestamp, key: Bytes) -> Result<()> {
        self.seek(timestamp, key)
    }

    fn next(&mut self) -> Result<()> {
        self.next();
        Ok(())
    }

    fn timestamp(&self) -> Result<Timestamp> {
        let timestamp = self.curr_entries[self.curr_entry_cursor].0;
        Ok(timestamp)
    }

    fn key(&self) -> Result<&Bytes> {
        Ok(&self.curr_key)
    }

    fn value(&self) -> Result<Bytes> {
        Ok(self.curr_entries[self.curr_entry_cursor].1.clone())
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }
}

// todo: make a general cache
#[derive(Default)]
struct DecompressCache {
    cache: HashMap<Bytes, Vec<(Timestamp, Bytes)>>,
}

impl DecompressCache {
    #[allow(clippy::ptr_arg)]
    fn get(&self, key: &Bytes) -> Option<&Vec<(Timestamp, Bytes)>> {
        self.cache.get(key)
    }

    fn insert(&mut self, key: Bytes, entries: Vec<(Timestamp, Bytes)>) {
        self.cache.insert(key, entries);
    }
}
