use futures_util::future::try_join_all;
use std::{collections::BTreeMap, usize};

use crate::error::Result;
use crate::index::MemIndex;
use crate::io::File;
use crate::types::{Bytes, Entry, EntryMeta, Offset, RickSuperBlock, Timestamp};

/// Handles to entries in rick (level 0).
///
/// Every shard will only have up to one rick file at any time.
///
/// (above is out-of-date)
///
/// Rick file may contains "hole" due to garbage collection.
/// It will have a [RickSuperBlock] at the very beginning (offset 0)
/// which can tell where the legal range is.
///
/// Rick can be either ordered or disordered, dependents on which level
/// it sites.
pub struct Rick {
    file: File,
    sb: RickSuperBlock,
}

impl Rick {
    pub async fn open(file: File) -> Result<Self> {
        let sb = Self::read_super_block(&file).await?;

        Ok(Self { file, sb })
    }

    /// Returns vector of (timestamp, key, entry's offset) to update index.
    ///
    /// `sync()` will be called before return.
    pub async fn append(&mut self, entries: Vec<Entry>) -> Result<Vec<(Timestamp, Bytes, u64)>> {
        let mut positions = Vec::with_capacity(entries.len());
        let file_length = self.sb.legal_offset_end;

        // construct binary buffer.
        let mut buf = vec![];
        for entry in entries {
            let bytes = entry.encode();
            let length = EntryMeta::new(bytes.len() as u64).encode();
            let buf_len_before = buf.len() as u64;
            buf.extend_from_slice(&length);
            buf.extend_from_slice(&bytes);
            positions.push((entry.timestamp, entry.key, file_length + buf_len_before));
        }

        // write to file
        let new_file_length = file_length + buf.len() as u64;
        self.file.write(buf, file_length).await?;

        // update super block and sync
        self.sb.legal_offset_end = new_file_length;
        self.sync_super_block().await?;
        self.sync().await?;

        Ok(positions)
    }

    /// Read from a offset.
    ///
    /// Entry not found will be return as a error.
    ///
    /// Maybe verify key here?
    pub async fn read(&self, offset: u64) -> Result<Entry> {
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

    /// Reads offsets.
    // todo: this might be refined by batching io.
    pub async fn reads(&mut self, offsets: Vec<u64>) -> Result<Vec<Entry>> {
        let mut futures = Vec::with_capacity(offsets.len());
        for offset in offsets {
            futures.push(self.read(offset));
        }

        try_join_all(futures).await
    }

    /// Read all keys
    pub async fn key_list(&mut self) -> Result<Vec<Bytes>> {
        let contents = self
            .file
            .read(self.start(), self.start() - self.end())
            .await?;
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

            if index >= self.sb.legal_offset_end as usize {
                break;
            }
        }

        Ok(keys)
    }

    pub async fn entry_list(&mut self) -> Result<Vec<Entry>> {
        let contents = self
            .file
            .read(self.start(), self.start() - self.end())
            .await?;
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

            if index >= self.sb.legal_offset_end as usize {
                break;
            }
        }

        Ok(entries)
    }

    /// Scan this rick file and construct its memindex
    pub async fn construct_index(&mut self) -> Result<MemIndex> {
        let contents = self
            .file
            .read(self.start(), self.start() - self.end())
            .await?;
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

            if index >= self.sb.legal_offset_end as usize {
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
    #[deprecated]
    pub async fn clean(&mut self) -> Result<()> {
        self.file.truncate(0).await?;
        self.file.sync().await?;

        Ok(())
    }

    /// Read super block from the first 4KB block of file.
    /// And if file is empty a new super block will be created.
    async fn read_super_block(file: &File) -> Result<RickSuperBlock> {
        // check whether super block exist.
        let file_length = file.size().await?;
        if file_length == 0 {
            // create super block and write it to file.
            let sb = RickSuperBlock {
                // todo: make it a parameter.
                is_ordered: false,
                legal_offset_start: RickSuperBlock::Length as u64,
                legal_offset_end: RickSuperBlock::Length as u64,
            };

            let buf = sb.encode();
            file.write(buf, 0).await?;

            Ok(sb)
        } else {
            // otherwise read from head.
            let buf = file.read(0, RickSuperBlock::Length as u64).await?;
            let sb = RickSuperBlock::decode(&buf);

            Ok(sb)
        }
    }

    // todo: check crash consistency.
    async fn sync_super_block(&self) -> Result<()> {
        let buf = self.sb.encode();
        self.file.write(buf, 0).await?;

        Ok(())
    }

    /// Get rick's start offset
    #[inline]
    pub fn start(&self) -> Offset {
        self.sb.legal_offset_start
    }

    /// Get rick's end offset.
    #[inline]
    pub fn end(&self) -> Offset {
        self.sb.legal_offset_end
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::file::file_manager::FileManager;

    use glommio::LocalExecutor;
    use tempfile::tempdir;

    #[test]
    fn new_super_block() {
        let ex = LocalExecutor::default();

        ex.run(async {
            let base_dir = tempdir().unwrap();
            let file_manager = FileManager::with_base_dir(base_dir.path()).unwrap();
            let rick_file = file_manager.open_rick(1).await.unwrap();
            let mut rick = Rick::open(rick_file).await.unwrap();

            assert_eq!(RickSuperBlock::Length, rick.start() as usize);
            assert_eq!(RickSuperBlock::Length, rick.end() as usize);

            // write something
            let entry = Entry {
                timestamp: 1,
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            };
            rick.append(vec![entry.clone()]).await.unwrap();
            let new_rick_end = rick.end();
            assert_ne!(RickSuperBlock::Length, rick.end() as usize);

            // close and open again
            drop(rick);
            let rick_file = file_manager.open_rick(1).await.unwrap();
            let rick = Rick::open(rick_file).await.unwrap();

            assert_eq!(RickSuperBlock::Length, rick.start() as usize);
            assert_eq!(new_rick_end, rick.end());
        });
    }

    #[test]
    fn read_write_one_entry() {
        let ex = LocalExecutor::default();

        ex.run(async {
            let base_dir = tempdir().unwrap();
            let file_manager = FileManager::with_base_dir(base_dir.path()).unwrap();
            let rick_file = file_manager.open_rick(1).await.unwrap();
            let mut rick = Rick::open(rick_file).await.unwrap();

            let entry = Entry {
                timestamp: 1,
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            };
            rick.append(vec![entry.clone()]).await.unwrap();

            let read_entry = rick.read(RickSuperBlock::Length as u64).await.unwrap();
            assert_eq!(entry, read_entry);
        });
    }
}
