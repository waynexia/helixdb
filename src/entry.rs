use core::time;
use std::io::Write;
use std::mem::{self, size_of};
use std::sync::Arc;

pub type Bytes = Vec<u8>;
pub type Timestamp = i64;

/// Binary format:
///
/// | timestamp (8B) | key size (8B) | value size (8B) | key (key size) | value (value size) |
///
/// C representation is needed to converting `(&ts, &key)` to `&(ts, key)`.
#[derive(Debug, PartialEq, Eq, Clone)]
#[repr(C)]
pub struct Entry {
    pub timestamp: Timestamp,
    pub key: Bytes,
    pub value: Bytes,
}

impl Entry {
    /// `value` will be consumed
    pub fn encode(&mut self) -> Bytes {
        // key, value, key size, value size, timestamp
        let len = self.key.len() + self.value.len() + size_of::<u64>() * 3;
        let mut buf = Vec::with_capacity(len);
        buf.extend_from_slice(&self.timestamp.to_le_bytes());
        buf.extend_from_slice(&self.key.len().to_le_bytes());
        buf.extend_from_slice(&self.value.len().to_le_bytes());
        buf.append(&mut self.key.clone());
        buf.append(&mut self.value);

        buf
    }

    pub const fn prefix_length() -> usize {
        8 * 3
    }

    pub fn decode(mut bytes: Bytes, prefix: &EntryPrefix) -> Self {
        debug_assert_eq!(bytes.len(), prefix.key_length + prefix.value_length);

        let key = bytes.drain(..prefix.key_length).collect();
        let value = bytes;

        Self {
            timestamp: prefix.timestamp,
            key,
            value,
        }
    }

    /// # Unsafe
    /// - Purpose: make a `&(A, B)` over a `&Self{A, B, C}`
    /// - Safety: `Entry` is qualified with `repr(C)`, memory layout is ensured to be
    /// the same with `((A, B), C)`.
    /// - Alternative: maybe no alternative.
    pub fn time_key(&self) -> &(Timestamp, Bytes) {
        unsafe {
            let p_entry = self as *const Entry;
            let p_ts_key = p_entry as *const (Timestamp, Bytes);
            &*p_ts_key
        }
    }
}

impl From<(Timestamp, Bytes, Bytes)> for Entry {
    fn from(input: (Timestamp, Bytes, Bytes)) -> Entry {
        let (timestamp, key, value) = input;
        Entry {
            timestamp,
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

    pub fn offload_length(&self) -> usize {
        self.key_length + self.value_length
    }
}
