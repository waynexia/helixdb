use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::entry::{Bytes, Entry, EntryPrefix, Timestamp};
use crate::error::Result;
use crate::index::MemIndex;

/// Handles to entries in rick (level 0).
pub struct Rick {
    file: File,
}

impl Rick {
    /// Returns vector of (timestamp, key, entry's offset) to update index.
    ///
    /// `sync()` will be called before returning.
    pub fn write(&mut self, entries: Vec<Entry>) -> Result<Vec<(Timestamp, Bytes, u64)>> {
        let mut positions = Vec::with_capacity(entries.len());
        let mut file_length = self.file.seek(SeekFrom::End(0))?;

        for mut entry in entries {
            let bytes = entry.encode();
            let wrote = self.file.write(&bytes)? as u64;
            positions.push((entry.timestamp, entry.key, file_length));
            file_length += wrote;
        }

        self.sync()?;

        Ok(positions)
    }

    /// Read from a offset.
    ///
    /// Entry not found will be return as a error.
    ///
    /// Maybe verify key here?
    pub fn read(&mut self, offset: u64) -> Result<Entry> {
        self.file.seek(SeekFrom::Start(offset))?;

        let mut prefix_buf = [0; Entry::prefix_length()];
        let read_length = self.file.read(&mut prefix_buf)?;
        debug_assert_eq!(read_length, Entry::prefix_length());

        let prefix = EntryPrefix::from_bytes(prefix_buf);
        let offload_length = prefix.offload_length();

        let mut offload_buf = vec![0; offload_length];
        let read_length = self.file.read(&mut offload_buf)?;
        debug_assert_eq!(read_length, offload_length);

        Ok(Entry::decode(offload_buf, &prefix))
    }

    /// Read all keys
    pub fn key_list(&mut self) -> Result<Vec<Bytes>> {
        self.file.seek(SeekFrom::Start(0))?;

        let mut keys = vec![];
        let mut prefix_buf = [0; Entry::prefix_length()];

        while self.file.read(&mut prefix_buf).is_ok() {
            let prefix = EntryPrefix::from_bytes(prefix_buf);
            let offload_length = prefix.offload_length();
            let mut offload_buf = vec![0; offload_length];
            let _ = self
                .file
                .read(&mut offload_buf)
                .expect("fail to read offload");
            let entry = Entry::decode(offload_buf, &prefix);
            keys.push(entry.key);
        }

        Ok(keys)
    }

    pub fn entry_list(&mut self) -> Result<Vec<Entry>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut entries = vec![];
        let mut prefix_buf = [0; Entry::prefix_length()];

        while self.file.read(&mut prefix_buf).is_ok() {
            let prefix = EntryPrefix::from_bytes(prefix_buf);
            let offload_length = prefix.offload_length();
            let mut offload_buf = vec![0; offload_length];
            let _ = self
                .file
                .read(&mut offload_buf)
                .expect("fail to read offload");
            let entry = Entry::decode(offload_buf, &prefix);
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Scan this rick file and construct its memindex
    pub fn construct_index(&mut self) -> Result<MemIndex> {
        // todo: handle incomplete entry. maybe add a "safe point" record?
        self.file.seek(SeekFrom::Start(0))?;

        let mut indices = BTreeMap::new();
        let mut prefix_buf = [0; Entry::prefix_length()];
        let mut offset = 0;

        while self.file.read(&mut prefix_buf).is_ok() {
            let prefix = EntryPrefix::from_bytes(prefix_buf);
            let offload_length = prefix.offload_length();
            let mut offload_buf = vec![0; offload_length];
            let read_length = self
                .file
                .read(&mut offload_buf)
                .expect("fail to read offload");
            let entry = Entry::decode(offload_buf, &prefix);
            indices.insert((prefix.timestamp, entry.key), offset as u64);
            offset += Entry::prefix_length() + read_length;
        }

        let mem_index = MemIndex { index: indices };
        Ok(mem_index)
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync_all()?;

        Ok(())
    }
}

impl From<File> for Rick {
    fn from(file: File) -> Self {
        Self { file }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::file::file_manager::{FileManager, FileType};

    use tempfile::tempdir;

    #[test]
    fn rick_read_write() {
        let base_dir = tempdir().unwrap();
        let file_manager = FileManager::with_base_dir(base_dir.path()).unwrap();
        let (rick_file, _) = file_manager.create(FileType::Rick).unwrap();
        let mut rick = Rick::from(rick_file);

        let entry = Entry {
            timestamp: 1,
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        };
        rick.write(vec![entry.clone()]).unwrap();

        let read_entry = rick.read(0).unwrap();
        assert_eq!(entry, read_entry);
    }
}
