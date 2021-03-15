use std::{collections::BTreeMap, usize};

use crate::error::Result;
use crate::index::MemIndex;
use crate::io::File;
use crate::types::{Bytes, Entry, EntryMeta, Timestamp};

/// Handles to entries in rick (level 0).
///
/// Every shard will only have up to one rick file at any time.
pub struct Rick {
    file: File,
}

impl Rick {
    /// Returns vector of (timestamp, key, entry's offset) to update index.
    ///
    /// `sync()` will be called before returning.
    pub async fn write(&mut self, entries: Vec<Entry>) -> Result<Vec<(Timestamp, Bytes, u64)>> {
        let mut positions = Vec::with_capacity(entries.len());
        let file_length = self.file.size().await?;

        let mut buf = vec![];
        for entry in entries {
            let bytes = entry.encode();
            let length = EntryMeta::new(bytes.len() as u64).encode();
            let buf_len_before = buf.len() as u64;
            buf.extend_from_slice(&length);
            buf.extend_from_slice(&bytes);
            positions.push((entry.timestamp, entry.key, file_length + buf_len_before));
        }
        self.file.write(buf, file_length).await?;
        self.sync().await?;

        Ok(positions)
    }

    /// Read from a offset.
    ///
    /// Entry not found will be return as a error.
    ///
    /// Maybe verify key here?
    pub async fn read(&mut self, offset: u64) -> Result<Entry> {
        let meta_buf = self
            .file
            .read(offset, EntryMeta::meta_size() as u64)
            .await?;
        let meta = EntryMeta::decode(&meta_buf);

        let offload_buf = self
            .file
            .read(offset + EntryMeta::meta_size() as u64, meta.length)
            .await?;

        Ok(Entry::decode(&offload_buf))
    }

    /// Read all keys
    pub async fn key_lest(&mut self) -> Result<Vec<Bytes>> {
        let file_size = self.file.size().await?;
        let contents = self.file.read(0, file_size).await?;
        let mut index = 0;

        let mut keys = vec![];

        loop {
            let prefix_buf = &contents[index..index + EntryMeta::meta_size()];
            index += EntryMeta::meta_size();
            let meta = EntryMeta::decode(prefix_buf);
            let offload_length = meta.length as usize;
            let offload_buf = &contents[index..index + offload_length];
            index += offload_length;
            let entry = Entry::decode(offload_buf);
            keys.push(entry.key);

            if index >= file_size as usize {
                break;
            }
        }

        Ok(keys)
    }

    pub async fn entry_list(&mut self) -> Result<Vec<Entry>> {
        let file_size = self.file.size().await?;
        let contents = self.file.read(0, file_size).await?;
        let mut index = 0;

        let mut entries = vec![];

        loop {
            let prefix_buf = &contents[index..index + EntryMeta::meta_size()];
            index += EntryMeta::meta_size();
            let meta = EntryMeta::decode(prefix_buf);
            let offload_length = meta.length as usize;
            let offload_buf = &contents[index..index + offload_length];
            index += offload_length;
            let entry = Entry::decode(offload_buf);
            entries.push(entry);

            if index >= file_size as usize {
                break;
            }
        }

        Ok(entries)
    }

    /// Scan this rick file and construct its memindex
    pub async fn construct_index(&mut self) -> Result<MemIndex> {
        let file_size = self.file.size().await?;
        let contents = self.file.read(0, file_size).await?;
        let mut index = 0;

        let mut indices = BTreeMap::new();
        let mut offset = 0;

        loop {
            let prefix_buf = &contents[index..index + EntryMeta::meta_size()];
            index += EntryMeta::meta_size();
            let meta = EntryMeta::decode(prefix_buf);
            let offload_length = meta.length as usize;
            let offload_buf = &contents[index..index + offload_length];
            index += offload_length;
            let entry = Entry::decode(offload_buf);

            indices.insert((entry.timestamp, entry.key), offset as u64);
            offset += EntryMeta::meta_size() + offload_length;

            if index >= file_size as usize {
                break;
            }
        }

        let mem_index = MemIndex { index: indices };
        Ok(mem_index)
    }

    pub async fn sync(&self) -> Result<()> {
        self.file.sync().await?;

        Ok(())
    }

    /// Drop all entries.
    pub async fn clean(&mut self) -> Result<()> {
        self.file.truncate(0).await?;
        self.file.sync().await?;

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
    use crate::file::file_manager::FileManager;

    use glommio::LocalExecutor;
    use tempfile::tempdir;

    #[test]
    fn rick_read_write() {
        let ex = LocalExecutor::default();

        ex.run(async {
            let base_dir = tempdir().unwrap();
            let file_manager = FileManager::with_base_dir(base_dir.path()).unwrap();
            let mut rick = Rick::from(file_manager.open_rick(1).await.unwrap());

            let entry = Entry {
                timestamp: 1,
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            };
            rick.write(vec![entry.clone()]).await.unwrap();

            let read_entry = rick.read(0).await.unwrap();
            assert_eq!(entry, read_entry);
        });
    }
}
