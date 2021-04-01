use lru::LruCache;
use std::cell::RefCell;
use std::rc::Rc;

use crate::table::{SSTableHandle, TableIdentifier};
use crate::types::{Bytes, LevelId, ThreadId, Timestamp};

pub struct CacheConfig {
    /// Number of `SSTableHandle` cache entries.
    pub table_handle_size: usize,
    /// Number of "Key - Value" cache entries.
    pub kv_cache_size: usize,
    /// Number of "Key - Compressed values" cache entries.
    pub kc_cache_size: usize,
    /// Number of "Key - Position in value log" cache entries.
    pub kp_cache_size: usize,

    /// The largest entry size will be held by kv_cache.
    pub kv_cache_threshold: usize,
    /// The largest entry size will be held by kc_cache.
    pub kc_cache_threshold: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            table_handle_size: 128,
            kp_cache_size: 512,
            kv_cache_size: 256,
            kc_cache_size: 64,
            kv_cache_threshold: 1024,
            kc_cache_threshold: 4096,
        }
    }
}

/// # Entry Cache
/// There are three types of entry cache: kv (for Key to Value), kc (for Key to Compressed
/// value bytes) and kp (for Key to corresponding value's Position in value log).
///
/// As the total space for caching is limited, cache small and frequent (or hot) is better.
pub struct Cache {
    handle_cache: RefCell<LruCache<TableIdentifier, Rc<SSTableHandle>>>,

    kv_cache: RefCell<LruCache<(Timestamp, Bytes), Bytes>>,
    kc_cache: RefCell<LruCache<(Timestamp, Bytes), Bytes>>,
    // todo: make it a `VLogIdentifier`.
    kp_cache: RefCell<LruCache<(Timestamp, Bytes), (ThreadId, LevelId, usize, usize)>>,
}

impl Cache {
    pub fn new(config: CacheConfig) -> Self {
        todo!()
    }

    pub fn get_table_handle(&self, table_id: &TableIdentifier) -> Option<Rc<SSTableHandle>> {
        self.handle_cache.borrow_mut().get(table_id).cloned()
    }

    pub fn put_table_handle(&self, table_id: TableIdentifier, handle: Rc<SSTableHandle>) {
        self.handle_cache.borrow_mut().put(table_id, handle);
    }

    // todo: use `TimeKey` struct instead.
    pub fn get_key(&self, time_key: &(Timestamp, Bytes)) -> KeyCacheResult {
        if let Some(value) = self.kv_cache.borrow_mut().get(time_key) {
            return KeyCacheResult::Value(value.to_owned());
        } else if let Some(compressed) = self.kc_cache.borrow_mut().get(time_key) {
            return KeyCacheResult::Compressed(compressed.to_owned());
        } else if let Some((tid, lid, offset, size)) = self.kp_cache.borrow_mut().get(time_key) {
            return KeyCacheResult::Position(*tid, *lid, *offset, *size);
        }

        KeyCacheResult::NotFound
    }

    pub fn put_key(&self) {
        todo!()
    }
}

pub enum KeyCacheResult {
    Value(Bytes),
    Compressed(Bytes),
    Position(ThreadId, LevelId, usize, usize),
    NotFound,
}
