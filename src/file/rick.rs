use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::{collections::BTreeMap, usize};

use crate::error::Result;
use crate::index::MemIndex;
use crate::types::{Bytes, Entry, EntryMeta, Timestamp};

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

        // todo: does encode each entries separately bring too much overhead?
        for entry in entries {
            let bytes = entry.encode();
            let length = EntryMeta::new(bytes.len() as u64).encode();
            let mut wrote = self.file.write(&length)?;
            wrote += self.file.write(&bytes)?;
            positions.push((entry.timestamp, entry.key, file_length));
            file_length += wrote as u64;
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

        let mut meta_buf = [0; EntryMeta::meta_size()];
        let read_length = self.file.read(&mut meta_buf)?;
        debug_assert_eq!(read_length, EntryMeta::meta_size());
        let meta = EntryMeta::decode(&meta_buf);

        let offload_length = meta.length as usize;
        let mut offload_buf = vec![0; offload_length];
        let read_length = self.file.read(&mut offload_buf)?;
        debug_assert_eq!(read_length, offload_length);

        Ok(Entry::decode(&offload_buf))
    }

    /// Read all keys
    pub fn key_list(&mut self) -> Result<Vec<Bytes>> {
        self.file.seek(SeekFrom::Start(0))?;

        let mut keys = vec![];
        let mut prefix_buf = [0; EntryMeta::meta_size()];

        while self.file.read(&mut prefix_buf).is_ok() {
            let meta = EntryMeta::decode(&prefix_buf);
            let offload_length = meta.length as usize;
            let mut offload_buf = vec![0; offload_length];
            let _ = self
                .file
                .read(&mut offload_buf)
                .expect("fail to read offload");
            let entry = Entry::decode(&offload_buf);
            keys.push(entry.key);
        }

        Ok(keys)
    }

    pub fn entry_list(&mut self) -> Result<Vec<Entry>> {
        self.file.seek(SeekFrom::Start(0))?;
        const META_SIZE: usize = EntryMeta::meta_size();
        let mut entries = vec![];
        let mut prefix_buf = [0; META_SIZE];

        while let Ok(META_SIZE) = self.file.read(&mut prefix_buf) {
            let meta = EntryMeta::decode(&prefix_buf);
            let offload_length = meta.length as usize;
            let mut offload_buf = vec![0; offload_length];
            let _ = self
                .file
                .read(&mut offload_buf)
                .expect("fail to read offload");
            let entry = Entry::decode(&offload_buf);
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Scan this rick file and construct its memindex
    pub fn construct_index(&mut self) -> Result<MemIndex> {
        // todo: handle incomplete entry.
        self.file.seek(SeekFrom::Start(0))?;

        let mut indices = BTreeMap::new();
        let mut prefix_buf = [0; EntryMeta::meta_size()];
        let mut offset = 0;

        while self.file.read(&mut prefix_buf).is_ok() {
            let meta = EntryMeta::decode(&prefix_buf);
            let offload_length = meta.length as usize;
            let mut offload_buf = vec![0; offload_length];
            let read_length = self
                .file
                .read(&mut offload_buf)
                .expect("fail to read offload");
            let entry = Entry::decode(&offload_buf);
            indices.insert((entry.timestamp, entry.key), offset as u64);
            offset += EntryMeta::meta_size() + read_length;
        }

        let mem_index = MemIndex { index: indices };
        Ok(mem_index)
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync_all()?;

        Ok(())
    }

    /// Drop all entries.
    pub fn clean(&mut self) -> Result<()> {
        self.file.set_len(0)?;
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
