use crate::entry::{Bytes, Timestamp};
use crate::error::Result;

pub trait Iterator {
    fn seek(&mut self, timestamp: Timestamp, key: Bytes) -> Result<()>;

    fn next(&mut self) -> Result<()>;

    fn timestamp(&self) -> Result<Timestamp>;

    fn key(&self) -> Result<&Bytes>;

    fn value(&self) -> Result<Bytes>;
}
