use std::collections::BTreeMap;
use std::time::Instant;
use std::usize;

use glommio::Local;
use tracing::trace;

use crate::error::Result;
use crate::index::MemIndex;
use crate::io::File;
use crate::types::{
    Bytes,
    Entry,
    EntryMeta,
    Offset,
    RickSuperBlock,
    TimeRange,
    Timestamp,
    ValueFormat,
};
use crate::util::check_bytes_length;

/// Handles to entries in rick (level 0).
///
/// Every shard will only have up to one rick file at any time.
///
/// (above is out-of-date)
///
/// Rick file may contains "hole" due to garbage collection.
/// It will have a [RickSuperBlock] at the very beginning (offset 0)
/// contains two pointers "start" and "end" (start < end)
/// which can tell where the legal range is. The start pointer should
/// points to a record's beginning. The start pointer is pushed by GC procedure
/// and end pointer is pushed by both `append()` method and GC procedure.
///
/// Rick can be either ordered or disordered, dependents on which level
/// it sites.
pub struct Rick {
    file: File,
    sb: RickSuperBlock,
}

impl Rick {
    /// Open a `Rick` from given file.
    ///
    /// Optional parameter `value_format` will be used to initialize a rick
    /// file. If the rick file is not empty it will be ignored. If `None` is
    /// provided, the `value_format` field in super block will be set to
    /// default value, which is `RawValue`.
    pub async fn open(file: File, value_format: Option<ValueFormat>) -> Result<Self> {
        let sb = Self::read_super_block(&file, value_format).await?;

        Ok(Self { file, sb })
    }

    /// Returns vector of (timestamp, key, entry's offset) to update index.
    ///
    /// `sync()` will be called before return.
    ///
    /// Once this method return, this `append` operation is considered finished
    /// on rick file. Even if it crashed before returned indices are
    /// persist.
    ///
    /// Encoding format: | payload length (u64) | payload |
    // todo: is it necessary to return inserted timestamp and key?
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
        check_bytes_length(&meta_buf, EntryMeta::meta_size())?;
        let meta = EntryMeta::decode(&meta_buf);

        let offload_buf = self
            .file
            .read(offset + EntryMeta::meta_size() as u64, meta.length)
            .await?;
        check_bytes_length(&offload_buf, meta.length as usize)?;

        Ok(Entry::decode(&offload_buf))
    }

    /// Reads offsets.
    // todo: this might be refined by batching io.
    pub async fn reads(&mut self, mut offsets: Vec<u64>) -> Result<Vec<Entry>> {
        // fast pass
        if offsets.len() < 2 {
            return match offsets.first() {
                Some(offset) => Ok(vec![self.read(*offset).await?]),
                None => Ok(vec![]),
            };
        }

        let now = Instant::now();
        trace!("[rick] start reads {} entries", offsets.len());

        offsets.sort_unstable();
        let min = *offsets.first().unwrap();
        let max = offsets.remove(offsets.len() - 1);
        let bytes = self.file.read(min, max - min).await?;
        let mut entries_iter = Self::decode_entries(&bytes)?.into_iter().peekable();
        let mut entries = Vec::with_capacity(offsets.len() + 1);

        trace!(
            "[rick] read and decode takes {:?} ms",
            now.elapsed().as_millis()
        );

        // filter decoded entries via given offsets
        for offset in &offsets {
            while entries_iter.peek().unwrap().1 + min != *offset {
                entries_iter.next();
            }
            entries.push(entries_iter.next().unwrap().0);
        }

        trace!("[rick] filter takes {:?} ms", now.elapsed().as_millis());

        // read the last offset
        entries.push(self.read(max).await?);

        Ok(entries)
    }

    pub fn is_compressed(&self) -> bool {
        self.sb.value_format == ValueFormat::CompressedValue
    }

    /// Scan this rick file and construct its memindex
    ///
    /// Generally, Rick file will couple with a persisted index file SSTable.
    /// Except those new ricks that memindex is not flushed to disk yet.
    pub async fn construct_index(&self) -> Result<MemIndex> {
        let contents = self
            .file
            .read(self.start(), self.end() - self.start())
            .await?;
        let mut index = 0;

        let mut indices = BTreeMap::new();
        let mut offset = RickSuperBlock::LENGTH;

        while index < contents.len() {
            let prefix_buf = &contents[index..index + EntryMeta::meta_size()];
            index += EntryMeta::meta_size();
            let meta = EntryMeta::decode(prefix_buf);
            let offload_length = meta.length as usize;
            let offload_buf = &contents[index..index + offload_length];
            index += offload_length;
            let entry = Entry::decode(offload_buf);

            indices.insert((entry.timestamp, entry.key), offset as u64);
            offset += EntryMeta::meta_size() + offload_length;
        }

        let mem_index = MemIndex::from_existing(indices);
        Ok(mem_index)
    }

    pub async fn sync(&self) -> Result<()> {
        self.file.sync().await?;

        Ok(())
    }

    pub async fn close(self) -> Result<()> {
        self.file.close().await?;

        Ok(())
    }

    /// Recycle entries in given `range` by free them using `fallocate` syscall.
    ///
    /// The general procedure would be like:
    /// - Traverse some entries from "start", for each entry
    ///     - suit in `range`, should be recycle.
    ///     - not suit, query index (if have) whether it is legal. Put it into
    ///     "need rewrite" buffer if is, and discard if not.
    /// - Acquire write lock and write those "need rewrite" to the end of file.
    /// Then update index (if have) and sync index (if need).
    /// - Sync super block to update "start" and "end" pointer to make above
    ///   change
    /// visible. After this the write l ock can be released.
    /// - Recycle space occupied by those offset is smaller than "start"
    ///   pointer.
    pub async fn garbage_collect(&self, range: TimeRange) -> Result<()> {
        // yield control to executor.
        Local::yield_if_needed().await;

        todo!()
    }

    // This is a temporary work around. Should be replaced by `garbage_collect()`
    // above.
    pub async fn clean(&mut self) -> Result<()> {
        // mark as illegal
        self.sb.legal_offset_start = self.sb.legal_offset_end;
        self.sync_super_block().await
    }

    pub async fn push_legal_offset_start(&mut self, new_offset_start: Offset) -> Result<()> {
        if new_offset_start <= self.sb.legal_offset_start {
            return Ok(());
        }

        self.sb.legal_offset_start = new_offset_start;
        self.sync_super_block().await
    }

    pub fn get_legal_offset_end(&self) -> Offset {
        self.sb.legal_offset_end
    }

    /// Read super block from the first 4KB block of file.
    /// And if file is empty a new super block will be created.
    ///
    /// `value_format` only works when initializing rick file.
    /// Default value is `RawValue`.
    async fn read_super_block(
        file: &File,
        value_format: Option<ValueFormat>,
    ) -> Result<RickSuperBlock> {
        // check whether super block exist.
        let file_length = file.size().await?;
        if file_length == 0 {
            let value_format = value_format.unwrap_or(ValueFormat::RawValue);
            // create super block and write it to file.
            let sb = RickSuperBlock {
                // todo: make it a parameter.
                is_ordered: false,
                legal_offset_start: RickSuperBlock::LENGTH as u64,
                legal_offset_end: RickSuperBlock::LENGTH as u64,
                // todo: make it a parameter.
                value_format,
                align_timestamp: 0,
            };

            let buf = sb.encode();
            file.write(buf, 0).await?;

            Ok(sb)
        } else {
            // otherwise read from head.
            let buf = file.read(0, RickSuperBlock::LENGTH as u64).await?;
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

    /// Decode to entries and the offset over input bytes.
    // todo: let `construct_index()` use this
    fn decode_entries(contents: &[u8]) -> Result<Vec<(Entry, u64)>> {
        let mut index = 0;
        let mut offset = 0;
        let mut entries = vec![];

        while index < contents.len() {
            let prefix_buf = &contents[index..index + EntryMeta::meta_size()];
            index += EntryMeta::meta_size();
            let meta = EntryMeta::decode(prefix_buf);
            let offload_length = meta.length as usize;
            let offload_buf = &contents[index..index + offload_length];
            index += offload_length;
            let entry = Entry::decode(offload_buf);
            entries.push((entry, offset as u64));

            offset += EntryMeta::meta_size() + offload_length;
        }

        Ok(entries)
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

    pub fn get_align_ts(&self) -> Timestamp {
        self.sb.align_timestamp
    }

    pub async fn set_align_ts(&mut self, ts: Timestamp) -> Result<()> {
        self.sb.align_timestamp = ts;
        self.sync_super_block().await
    }
}

#[cfg(test)]
mod test {
    use glommio::LocalExecutor;
    use tempfile::tempdir;

    use super::*;
    use crate::file::file_manager::FileManager;

    #[test]
    fn new_super_block() {
        let ex = LocalExecutor::default();

        ex.run(async {
            let base_dir = tempdir().unwrap();
            let file_manager = FileManager::with_base_dir(base_dir.path()).unwrap();
            let rick_file = file_manager.open_rick(1).await.unwrap();
            let mut rick = Rick::open(rick_file, None).await.unwrap();

            assert_eq!(RickSuperBlock::LENGTH, rick.start() as usize);
            assert_eq!(RickSuperBlock::LENGTH, rick.end() as usize);

            // write something
            let entry = Entry {
                timestamp: 1,
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            };
            rick.append(vec![entry.clone()]).await.unwrap();
            let new_rick_end = rick.end();
            assert_ne!(RickSuperBlock::LENGTH, rick.end() as usize);

            // close and open again
            drop(rick);
            let rick_file = file_manager.open_rick(1).await.unwrap();
            let rick = Rick::open(rick_file, None).await.unwrap();

            assert_eq!(RickSuperBlock::LENGTH, rick.start() as usize);
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
            let mut rick = Rick::open(rick_file, None).await.unwrap();

            let entry = Entry {
                timestamp: 1,
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            };
            rick.append(vec![entry.clone()]).await.unwrap();

            let read_entry = rick.read(RickSuperBlock::LENGTH as u64).await.unwrap();
            assert_eq!(entry, read_entry);
        });
    }

    #[test]
    fn reconstruct_memindex() {
        let ex = LocalExecutor::default();

        ex.run(async {
            let base_dir = tempdir().unwrap();
            let file_manager = FileManager::with_base_dir(base_dir.path()).unwrap();
            let rick_file = file_manager.open_rick(1).await.unwrap();
            let mut rick = Rick::open(rick_file, None).await.unwrap();

            let entries = vec![
                // one key with three timestamps.
                (1, b"key1".to_vec(), b"value".to_vec()).into(),
                (2, b"key1".to_vec(), b"value".to_vec()).into(),
                (3, b"key1".to_vec(), b"value".to_vec()).into(),
                // overwrite
                (1, b"key2".to_vec(), b"value1".to_vec()).into(),
                (1, b"key2".to_vec(), b"value2".to_vec()).into(),
            ];
            rick.append(entries.clone()).await.unwrap();

            let memindex = rick.construct_index().await.unwrap();

            assert_eq!(3, *memindex.user_keys.get(&b"key1".to_vec()).unwrap());
            assert_eq!(1, *memindex.user_keys.get(&b"key2".to_vec()).unwrap());

            for index in memindex.into_iter() {
                rick.read(index.1).await.unwrap();
            }
        });
    }

    #[test]
    fn rick_reads_method() {
        let ex = LocalExecutor::default();

        ex.run(async {
            let base_dir = tempdir().unwrap();
            let file_manager = FileManager::with_base_dir(base_dir.path()).unwrap();
            let rick_file = file_manager.open_rick(1).await.unwrap();
            let mut rick = Rick::open(rick_file, None).await.unwrap();

            let mut entries = vec![
                (1, b"key1".to_vec(), b"value".to_vec()).into(),
                (2, b"key1".to_vec(), b"value".to_vec()).into(),
                (3, b"key1".to_vec(), b"value".to_vec()).into(),
                (1, b"key2".to_vec(), b"value".to_vec()).into(),
                (2, b"key2".to_vec(), b"value".to_vec()).into(),
                (3, b"key2".to_vec(), b"value".to_vec()).into(),
                (1, b"key3".to_vec(), b"value".to_vec()).into(),
                (2, b"key3".to_vec(), b"value".to_vec()).into(),
                (3, b"key3".to_vec(), b"value".to_vec()).into(),
            ];
            let mut offsets: Vec<u64> = rick
                .append(entries.clone())
                .await
                .unwrap()
                .into_iter()
                .map(|item| item.2)
                .collect();

            // all entries
            let reads_result = rick.reads(offsets.clone()).await.unwrap();
            assert_eq!(entries, reads_result);

            // eliminate some in the middle
            entries.remove(entries.len() / 2);
            offsets.remove(offsets.len() / 2);
            let reads_result = rick.reads(offsets.clone()).await.unwrap();
            assert_eq!(entries, reads_result);
        });
    }
}
