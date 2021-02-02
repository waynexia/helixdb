use core::time;
use std::io::Write;
use std::mem::{self, size_of};
use std::sync::Arc;

pub type Bytes = Vec<u8>;
pub type Timestamp = i64;

/// Binary format:
///
/// | timestamp (8B) | key size (8B) | value size (8B) | key (key size) | value (value size) |
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Entry {
    pub timestamp: Timestamp,
    pub key: Bytes,
    pub value: Bytes,
}

impl Entry {
    pub fn encode(&mut self) -> Bytes {
        // key, value, key size, value size, timestamp
        let len = self.key.len() + self.value.len() + size_of::<u64>() * 3;
        let mut buf = Vec::with_capacity(len);
        buf.extend_from_slice(&self.timestamp.to_le_bytes());
        buf.extend_from_slice(&self.key.len().to_le_bytes());
        buf.extend_from_slice(&self.value.len().to_le_bytes());
        buf.append(&mut self.key);
        buf.append(&mut self.value);

        buf
    }

    pub const fn prefix_length() -> usize {
        8 * 3
    }

    pub fn decode(bytes: Bytes, prefix: EntryPrefix) -> Self {
        debug_assert_eq!(bytes.len(), prefix.key_length + prefix.value_length);

        let (key, value) = unsafe {
            let (base_ptr, _, _) = bytes.into_raw_parts();
            (
                Vec::from_raw_parts(base_ptr, prefix.key_length, prefix.key_length),
                Vec::from_raw_parts(
                    base_ptr.add(prefix.value_length),
                    prefix.value_length,
                    prefix.value_length,
                ),
            )
        };

        Self {
            timestamp: prefix.timestamp,
            key,
            value,
        }
    }
}

#[repr(C)]
pub struct EntryPrefix {
    pub timestamp: Timestamp,
    pub key_length: usize,
    pub value_length: usize,
}

impl EntryPrefix {
    pub fn from_bytes(bytes: [u8; Entry::prefix_length()]) -> Self {
        debug_assert_eq!(std::mem::size_of::<EntryPrefix>(), Entry::prefix_length());
        unsafe { mem::transmute(bytes) }
    }
}

/// Dispatch key to different shards. Called "sharding a key".
///
/// Input type is a reference to a key in `Bytes` and output is which shard this
/// key belones to.
pub type ShardingKeyFn = Arc<dyn Fn(&Bytes) -> usize + Send + Sync>;
