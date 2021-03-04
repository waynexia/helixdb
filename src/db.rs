use crate::types::{Bytes, Entry};

pub struct HelixDB;

impl HelixDB {
    pub fn put(&self, write_batch: Vec<Entry>) {}

    pub fn get(&self, timestamp: i64, key: Bytes) {}

    pub fn scan(&self) {}
}
