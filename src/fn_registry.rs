use std::collections::{HashMap, VecDeque};
use std::convert::TryInto;
use std::sync::Arc;

use crate::entry::{Bytes, Timestamp};
use crate::error::{HelixError, Result};

/// Custom compaction function. This will be called when compacting L0
/// files to L1.
///
/// The inputs are key, [(timestamp, values),].
pub type CompressFn = Arc<dyn Fn(Bytes, Vec<(Timestamp, Bytes)>) -> Bytes + Send + Sync>;

/// The inputs are key and compressed bytes.
/// Output is [(timestamp, values),]
pub type DecompressFn = Arc<dyn Fn(Bytes, Bytes) -> Vec<(Timestamp, Bytes)> + Send + Sync>;

/// `UDCF` stands for "User Defined Compress Function".
/// Includes compress and decompress implementation.
#[derive(Clone)]
pub struct UDCF {
    name: String,
    compress_fn: CompressFn,
    decompress_fn: DecompressFn,
}

impl UDCF {
    pub fn new(name: String, compress_fn: CompressFn, decompress_fn: DecompressFn) -> Self {
        Self {
            name,
            compress_fn,
            decompress_fn,
        }
    }

    pub fn compress(&self) -> CompressFn {
        self.compress_fn.clone()
    }

    pub fn decompress(&self) -> DecompressFn {
        self.decompress_fn.clone()
    }
}

/// Determine compress function based on key.
///
/// This will be called on each key that going to be compressed.
pub type CompressDispatchFn = Arc<dyn Fn(&Bytes) -> &str + Send + Sync>;

/// Dispatch key to different shards. Called "sharding a key".
///
/// Input type is a reference to a key in `Bytes` and output is which shard this
/// key belongs to.
pub type ShardingKeyFn = Arc<dyn Fn(&Bytes) -> usize + Send + Sync>;

pub struct FnRegistry {
    sharding_key_fn: ShardingKeyFn,
    dispatch_fn: CompressDispatchFn,
    compress_functions: HashMap<String, UDCF>,
}

impl FnRegistry {
    pub fn new_noop() -> Self {
        unsafe {
            Self {
                sharding_key_fn: noop_sharding_key_fn(),
                dispatch_fn: noop_dispatch_fn(),
                compress_functions: HashMap::new(),
            }
        }
    }

    pub fn register_udcf(&mut self, udcf: UDCF) {
        self.compress_functions.insert(udcf.name.clone(), udcf);
    }

    pub fn register_dispatch_fn(&mut self, dispatch_fn: CompressDispatchFn) {
        self.dispatch_fn = dispatch_fn;
    }

    pub fn register_sharding_key_fn(&mut self, sharding_key_fn: ShardingKeyFn) {
        self.sharding_key_fn = sharding_key_fn;
    }

    pub fn dispatch_fn(&self) -> CompressDispatchFn {
        self.dispatch_fn.clone()
    }

    pub fn udcf(&self, name: &str) -> Result<UDCF> {
        self.compress_functions
            .get(name)
            .cloned()
            .ok_or(HelixError::NotFound)
    }
}

pub fn noop_sharding_key_fn() -> ShardingKeyFn {
    Arc::new(|_| 0)
}

/// Dispatch all keys to [noop_udcf].
pub fn noop_dispatch_fn() -> CompressDispatchFn {
    Arc::new(|_| "noop")
}

/// A No-Op compress function.
///
/// Compress: first put all entries' bytes together. Then followed a block of bytes records
/// each entry's length in u64. The last 8 bytes is how many entries sited.
/// ```
/// | N var-length bytes | N * u64 for length | N as u64 |
/// ```
pub fn noop_udcf() -> UDCF {
    // todo switch to `byteorder` crate.
    let compress_fn: CompressFn = Arc::new(|_key, ts_values| {
        let value_num = ts_values.len() as u64;

        // concat timestamp and value together.
        let ts_values: Vec<Bytes> = ts_values
            .into_iter()
            .map(|(ts, mut value)| {
                let mut ts_bytes = ts.to_le_bytes().to_vec();
                ts_bytes.append(&mut value);
                ts_bytes
            })
            .collect();

        // calculate length for every ts_value's bytes and put them together.
        let mut value_length = Vec::with_capacity(value_num as usize);
        for bytes in &ts_values {
            value_length.append(&mut (bytes.len() as u64).to_le_bytes().to_vec())
        }

        // concat all ts_values, lengths, and number of entries
        let mut concated_value = ts_values.concat();
        concated_value.append(&mut value_length);
        concated_value.append(&mut value_num.to_le_bytes().to_vec());

        concated_value
    });

    const TIMESTAMP_SIZE: usize = std::mem::size_of::<Timestamp>();
    const U64_SIZE: usize = std::mem::size_of::<u64>();
    let decompress_fn: DecompressFn = Arc::new(|_key, raw_values| {
        let mut raw_values: VecDeque<u8> = raw_values.into();
        let mut len = raw_values.len();

        // decode `N`
        let value_num_bytes: Vec<u8> = raw_values.drain(len - U64_SIZE..).collect();
        len -= U64_SIZE;
        let value_num = u64::from_le_bytes(value_num_bytes.try_into().unwrap()) as usize;

        // decode lengths
        let mut value_length = Vec::with_capacity(value_num);
        // start from 1
        for i in 1..=value_num {
            let length_bytes: Vec<u8> = raw_values.drain(len - i * U64_SIZE..).collect();
            let length = u64::from_le_bytes(length_bytes.try_into().unwrap()) as usize;
            value_length.push(length);
        }
        len -= U64_SIZE * value_num;

        // slice values
        let mut values = VecDeque::with_capacity(value_num);
        for length in value_length {
            let mut ts_value_bytes: Vec<u8> = raw_values.drain(len - length..).collect();
            len -= length;
            let value_bytes = ts_value_bytes.drain(TIMESTAMP_SIZE..).collect();
            let timestamp = Timestamp::from_le_bytes(ts_value_bytes.try_into().unwrap());
            values.push_front((timestamp, value_bytes));
        }

        // convert VecDeque to Vec.
        // the decompress procedure is in reverse order.
        values.into()
    });

    UDCF::new("noop".to_string(), compress_fn, decompress_fn)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn noop_udcf_compress_decompress() {
        let udcf = noop_udcf();

        let key = b"key".to_vec();
        let ts_values = vec![
            (1, b"value1".to_vec()),
            (2, b"value2".to_vec()),
            (3, b"value3".to_vec()),
            (4, b"value1".to_vec()),
            (5, b"value3".to_vec()),
            (6, b"value2".to_vec()),
        ];

        let compressed = udcf.compress()(key.clone(), ts_values.clone());
        let decompressed = udcf.decompress()(key, compressed);

        assert_eq!(ts_values, decompressed);
    }
}
