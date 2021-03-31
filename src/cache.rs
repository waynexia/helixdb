use lru::LruCache;
use std::cell::RefCell;
use std::rc::Rc;

use crate::table::{SSTableHandle, TableIdentifier};

pub struct CacheConfig {
    pub table_handle_size: usize,
}

pub struct Cache {
    handle_cache: RefCell<LruCache<TableIdentifier, Rc<SSTableHandle>>>,
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
}
