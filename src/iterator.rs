use crate::error::Result;
use crate::types::{Bytes, Timestamp};

// todo: make `value()` returns reference
pub trait Iterator {
    fn seek(&mut self, timestamp: Timestamp, key: Bytes) -> Result<()>;

    fn next(&mut self) -> Result<()>;

    fn timestamp(&self) -> Result<Timestamp>;

    fn key(&self) -> Result<&Bytes>;

    fn value(&self) -> Result<Bytes>;

    fn is_valid(&self) -> bool;
}
