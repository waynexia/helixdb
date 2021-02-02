use std::fs::{DirEntry, File};
use std::io::{Read, Seek, SeekFrom, Write};

use crate::entry::{Bytes, Entry, EntryPrefix, Timestamp};
use crate::error::Result;

pub struct IOWorker {
    curr_l0_file: File,
}

impl IOWorker {
    /// Returns vector of (timestamp, key, entry's position).
    pub fn write(&mut self, entries: Vec<Entry>) -> Result<Vec<(Timestamp, Bytes, ValuePosition)>> {
        let mut positions = Vec::with_capacity(entries.len());
        let mut file_length = self.curr_l0_file.seek(SeekFrom::End(0))?;

        let path = "placeholder".to_string();
        for mut entry in entries {
            let bytes = entry.encode();
            let wrote = self.curr_l0_file.write(&bytes)? as u64;
            positions.push((
                entry.timestamp,
                entry.key,
                ValuePosition::new(path.clone(), file_length),
            ));
            file_length += wrote;
        }

        Ok(positions)
    }

    pub fn read(&self, position: &ValuePosition) -> Result<Option<Entry>> {
        let mut file = File::open(position.path.clone())?;
        file.seek(SeekFrom::Start(position.offset))?;

        let mut prefix_buf = [0; Entry::prefix_length()];
        let read_length = file.read(&mut prefix_buf)?;
        debug_assert_eq!(read_length, Entry::prefix_length());

        let prefix = EntryPrefix::from_bytes(prefix_buf);
        let offload_length = prefix.key_length + prefix.value_length;

        let mut offload_buf = Vec::with_capacity(offload_length);
        let read_length = file.read(&mut offload_buf)?;
        debug_assert_eq!(read_length, offload_length);

        Ok(Some(Entry::decode(offload_buf, prefix)))
    }
}

#[derive(Debug)]
pub struct ValuePosition {
    path: String,
    offset: u64,
}

impl ValuePosition {
    fn new(path: String, offset: u64) -> Self {
        Self { path, offset }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use tempfile::tempfile;

    #[test]
    fn io_worker_init() {
        let temp_file = tempfile().unwrap();
        let mut worker = IOWorker {
            curr_l0_file: temp_file,
        };

        let entry = Entry {
            timestamp: 0,
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        };

        let (_, _, pos) = worker.write(vec![entry.clone()]).unwrap().pop().unwrap();

        println!("pos: {:?}", pos);

        assert_eq!(worker.read(&pos).unwrap().unwrap(), entry);
    }
}
