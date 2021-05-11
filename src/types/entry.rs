use std::convert::TryInto;
use std::mem;
use std::ops::Range;

use flatbuffers::FlatBufferBuilder;

pub type Bytes = Vec<u8>;
pub type Timestamp = i64;
pub type ThreadId = u64;
/// Mono-increase identifier to level files. Starts from 1.
/// Level id `0` stands for Rick level.
pub type LevelId = u64;

/// Wrapper struct over protos::Entry.
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
    pub fn encode(&self) -> Bytes {
        let mut fbb = FlatBufferBuilder::new();

        let timestamp = protos::Timestamp::new(self.timestamp);
        let key_bytes = fbb.create_vector_direct(&self.key);
        let value_bytes = fbb.create_vector_direct(&self.value);

        let entry = protos::Entry::create(
            &mut fbb,
            &protos::EntryArgs {
                timestamp: Some(&timestamp),
                key: Some(key_bytes),
                value: Some(value_bytes),
            },
        );

        fbb.finish(entry, None);
        fbb.finished_data().to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Self {
        let fb_entry = flatbuffers::get_root::<protos::Entry<'_>>(bytes);

        Self {
            timestamp: fb_entry.timestamp().unwrap().timestamp(),
            key: fb_entry.key().unwrap().to_vec(),
            value: fb_entry.value().unwrap().to_vec(),
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

/// Describe a encoded [Entry]'s buffer.
pub struct EntryMeta {
    pub length: u64,
}

impl EntryMeta {
    pub fn new(length: u64) -> Self {
        Self { length }
    }

    pub const fn meta_size() -> usize {
        mem::size_of::<Self>()
    }

    pub fn encode(&self) -> [u8; 8] {
        self.length.to_le_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Self {
        Self {
            length: u64::from_le_bytes(bytes.try_into().unwrap()),
        }
    }
}

// todo: replace with std::ops::Range.
#[derive(Debug, Copy, Clone)]
pub struct TimeRange {
    start: Timestamp,
    end: Timestamp,
}

impl TimeRange {
    /// Is `self` containing given timestamp.
    pub fn contains(&self, ts: Timestamp) -> bool {
        self.start <= ts && self.end >= ts
    }

    pub fn range(&self) -> Range<Timestamp> {
        Range {
            start: self.start,
            end: self.end,
        }
    }

    pub fn start(&self) -> Timestamp {
        self.start
    }

    pub fn end(&self) -> Timestamp {
        self.end
    }
}

impl From<(Timestamp, Timestamp)> for TimeRange {
    fn from(tuple: (Timestamp, Timestamp)) -> TimeRange {
        Self {
            start: tuple.0,
            end: tuple.1,
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn entry_codec() {
        let entry = Entry {
            timestamp: 1000,
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        };

        let bytes = entry.encode();

        assert_eq!(entry, Entry::decode(&bytes));
    }

    #[test]
    fn time_range_contains() {
        let range = TimeRange::from((0, 10));

        assert_eq!(false, range.contains(-1));
        assert_eq!(true, range.contains(0));
        assert_eq!(true, range.contains(5));
        assert_eq!(true, range.contains(10));
        assert_eq!(false, range.contains(101));
    }
}
