use std::cell::RefCell;
use std::fmt::Debug;
use std::rc::Rc;

use lru::LruCache;

use crate::error::Result;
use crate::table::{TableIdentifier, TableReadHandle};
use crate::types::{Bytes, LevelId, ThreadId, Timestamp};

#[derive(Debug, Clone, Copy)]
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
            table_handle_size: 32,
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
    config: CacheConfig,
    handle_cache: RefCell<LruCache<TableIdentifier, Rc<TableReadHandle>>>,

    kv_cache: RefCell<LruCache<(Timestamp, Bytes), Bytes>>,
    kc_cache: RefCell<LruCache<(Timestamp, Bytes), Bytes>>,
    // todo: make it a `VLogIdentifier`.
    #[allow(clippy::type_complexity)]
    kp_cache: RefCell<LruCache<(Timestamp, Bytes), (ThreadId, LevelId, usize)>>,
}

impl Cache {
    pub fn with_config(config: CacheConfig) -> Self {
        Self {
            handle_cache: RefCell::new(LruCache::new(config.table_handle_size)),
            kv_cache: RefCell::new(LruCache::new(config.kv_cache_size)),
            kc_cache: RefCell::new(LruCache::new(config.kc_cache_size)),
            kp_cache: RefCell::new(LruCache::new(config.kp_cache_size)),

            config,
        }
    }

    pub fn default() -> Self {
        Self::with_config(CacheConfig::default())
    }

    pub fn get_table_handle(&self, table_id: &TableIdentifier) -> Option<Rc<TableReadHandle>> {
        self.handle_cache.borrow_mut().get(table_id).cloned()
    }

    pub async fn put_table_handle(
        &self,
        table_id: TableIdentifier,
        handle: Rc<TableReadHandle>,
    ) -> Result<()> {
        let outdated = self.handle_cache.borrow_mut().put(table_id, handle);
        if let Some(handle) = outdated {
            handle.try_close().await?;
        }

        Ok(())
    }

    // todo: use `TimeKey` struct instead.
    pub fn get_key(&self, time_key: &(Timestamp, Bytes)) -> KeyCacheResult {
        if let Some(value) = self.kv_cache.borrow_mut().get(time_key) {
            return KeyCacheResult::Value(value.to_owned());
        } else if let Some(compressed) = self.kc_cache.borrow_mut().get(time_key) {
            return KeyCacheResult::Compressed(compressed.to_owned());
        } else if let Some((tid, lid, offset)) = self.kp_cache.borrow_mut().get(time_key) {
            return KeyCacheResult::Position(*tid, *lid, *offset);
        }

        KeyCacheResult::NotFound
    }

    pub fn put_key(&self, key_entry: KeyCacheEntry) {
        if let Some(value) = key_entry.value {
            if value.len() < self.config.kv_cache_threshold {
                self.kv_cache
                    .borrow_mut()
                    .put(key_entry.key.to_owned(), value.to_owned());
            }
        } else if let Some(compressed) = key_entry.compressed {
            if compressed.len() < self.config.kv_cache_threshold {
                self.kc_cache
                    .borrow_mut()
                    .put(key_entry.key.to_owned(), compressed.to_owned());
            }
        } else if let Some(position) = key_entry.position {
            self.kp_cache
                .borrow_mut()
                .put(key_entry.key.to_owned(), position);
        }
    }
}

pub enum KeyCacheResult {
    Value(Bytes),
    Compressed(Bytes),
    /// Thread id and level id is for constructing rick file's identifier.
    /// The third `usize` is offset.
    Position(ThreadId, LevelId, usize),
    NotFound,
}

impl Debug for KeyCacheResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut f = f.debug_struct("KeyCacheResult");
        match self {
            KeyCacheResult::Value(bytes) => f.field("Value", &bytes.len()),
            KeyCacheResult::Compressed(bytes) => f.field("Compressed", &bytes.len()),
            KeyCacheResult::Position(tid, lid, offset) => f.field("Position", &(tid, lid, offset)),
            KeyCacheResult::NotFound => f.field("NotFound", &()),
        };
        f.finish()
    }
}

/// For inserting Key into cache.
pub struct KeyCacheEntry<'a> {
    pub key: &'a (Timestamp, Bytes),
    pub value: Option<&'a Bytes>,
    pub compressed: Option<&'a Bytes>,
    pub position: Option<(ThreadId, LevelId, usize)>,
}

impl<'a> KeyCacheEntry<'a> {
    pub fn new(key: &'a (Timestamp, Bytes)) -> Self {
        Self {
            key,
            value: None,
            compressed: None,
            position: None,
        }
    }
}
