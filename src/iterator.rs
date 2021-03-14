use async_trait::async_trait;

use crate::error::Result;
use crate::types::{Bytes, Timestamp};

// todo: make `value()` returns reference
#[async_trait]
pub trait Iterator {
    async fn seek(&mut self, timestamp: Timestamp, key: Bytes) -> Result<()>;

    fn next(&mut self) -> Result<()>;

    fn timestamp(&self) -> Result<Timestamp>;

    fn key(&self) -> Result<&Bytes>;

    fn value(&self) -> Result<Bytes>;

    fn is_valid(&self) -> bool;
}
