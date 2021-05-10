use std::sync::Arc;

use crate::context::Context;
use crate::error::{HelixError, Result};
use crate::file::Rick;
use crate::index::MemIndex;
use crate::io::File;
use crate::table::TableReadHandle;
use crate::types::sstable::{BlockInfo, BlockType, IndexBlockEntry, SSTableSuperBlock};
use crate::types::{Bytes, LevelId, Offset, ThreadId, Timestamp};
use crate::util::{check_bytes_length, decode_u64, encode_u64};

pub struct SSTable {
    file: File,
    sb: SSTableSuperBlock,
}

impl SSTable {
    pub async fn open(file: File) -> Result<Self> {
        let sb = Self::read_super_block(&file).await?;

        Ok(Self { file, sb })
    }

    pub async fn into_read_handle(self, ctx: Arc<Context>) -> Result<TableReadHandle> {
        // read index block
        let index_blocks = self.sb.get_block_info(BlockType::IndexBlock);
        if index_blocks.is_empty() {
            return Err(HelixError::NotFound);
        }
        let mut indices = vec![];
        for block in index_blocks {
            let block_buf = self.file.read(block.offset, block.length).await?;
            check_bytes_length(&block_buf, block.length as usize)?;
            let memindex = IndexBlockReader::read(block_buf)?;
            indices.push(memindex);
        }
        // todo: merge multi mem-indices
        let memindex = indices.pop().unwrap();

        // open rick file
        let rick_file = ctx
            .file_manager
            .open_vlog(self.sb.thread_id, self.sb.level_id)
            .await?;
        let rick = Rick::open(rick_file, None).await?;

        let handle = TableReadHandle::new(memindex, self, rick, ctx);
        Ok(handle)
    }

    /// Read super block from the first 4KB block of file.
    /// And if file is empty a new super block will be created.
    // todo: duplicate code with `Rick::read_super_block()`
    async fn read_super_block(file: &File) -> Result<SSTableSuperBlock> {
        // check whether super block exist.
        let file_length = file.size().await?;
        if file_length == 0 {
            // create super block and write it to file.
            let sb = SSTableSuperBlock {
                // todo: which default value?
                thread_id: 0,
                level_id: 0,
                blocks: vec![],
            };

            let buf = sb.encode();
            file.write(buf, 0).await?;

            Ok(sb)
        } else {
            // otherwise read from head.
            let buf = file.read(0, SSTableSuperBlock::Length as u64).await?;
            let sb = SSTableSuperBlock::decode(&buf);

            Ok(sb)
        }
    }
}

pub struct TableBuilder {
    thread_id: ThreadId,
    level_id: LevelId,
    file: File,
    block_buffer: Bytes,
    blocks: Vec<BlockInfo>,
    tail_offset: Offset,
}

impl TableBuilder {
    /// Start to build table.
    pub fn begin(thread_id: ThreadId, level_id: LevelId, file: File) -> Self {
        Self {
            thread_id,
            level_id,
            file,
            block_buffer: vec![],
            blocks: vec![],
            tail_offset: SSTableSuperBlock::Length as u64,
        }
    }

    pub fn add_block(&mut self, block_builder: impl BlockBuilder) {
        let (block_type, mut block_data) = block_builder.finish();

        let block_size = block_data.len() as u64;
        self.blocks.push(BlockInfo {
            block_type,
            offset: self.tail_offset,
            length: block_size,
        });
        self.block_buffer.append(&mut block_data);
        self.tail_offset += block_size;
    }

    /// Consume this builder to build a SSTable.
    pub async fn finish(self) -> Result<()> {
        // write super block
        let sb = SSTableSuperBlock {
            thread_id: self.thread_id,
            level_id: self.level_id,
            blocks: self.blocks,
        };
        self.file.write(sb.encode(), 0).await?;

        debug_assert_eq!(
            self.block_buffer.len(),
            self.tail_offset as usize - SSTableSuperBlock::Length
        );
        // write other blocks
        // todo: finish this in one write req
        self.file
            .write(self.block_buffer, SSTableSuperBlock::Length as u64)
            .await?;

        self.file.sync().await?;

        Ok(())
    }
}

pub trait BlockBuilder {
    fn finish(self) -> (BlockType, Bytes);
}

pub struct IndexBlockBuilder {
    entry_buffer: Bytes,
}

impl IndexBlockBuilder {
    pub fn new() -> Self {
        Self {
            entry_buffer: vec![],
        }
    }

    pub fn from_memindex() -> Self {
        todo!()
    }

    pub fn add_entry(&mut self, key: &Bytes, timestamp: Timestamp, offset: Offset) {
        let index_entry = IndexBlockEntry {
            value_offset: offset,
            timestamp,
            key: key.to_owned(),
        };
        let mut entry_bytes = index_entry.encode();
        let bytes_len = entry_bytes.len() as u64;

        self.entry_buffer.append(&mut encode_u64(bytes_len));
        self.entry_buffer.append(&mut entry_bytes);
    }
}

impl BlockBuilder for IndexBlockBuilder {
    fn finish(self) -> (BlockType, Bytes) {
        (BlockType::IndexBlock, self.entry_buffer)
    }
}

pub trait BlockReader {
    type Output;

    fn read(_: Bytes) -> Result<Self::Output>;
}

pub struct IndexBlockReader {}

impl BlockReader for IndexBlockReader {
    type Output = MemIndex;

    fn read(mut data: Bytes) -> Result<MemIndex> {
        let mut memindex = MemIndex::default();

        // todo: benchmark this
        while !data.is_empty() {
            // read length
            let length_buf: Vec<_> = data.drain(..std::mem::size_of::<u64>()).collect();
            check_bytes_length(&length_buf, std::mem::size_of::<u64>())?;
            let length = decode_u64(&length_buf) as usize;

            // read index entry
            let data_buf: Vec<_> = data.drain(..length).collect();
            check_bytes_length(&data_buf, length)?;
            let index_entry = IndexBlockEntry::decode(&data_buf);

            memindex.insert((
                index_entry.timestamp,
                index_entry.key,
                index_entry.value_offset,
            ))?;
        }

        Ok(memindex)
    }
}

#[cfg(test)]
mod test {
    use glommio::LocalExecutor;
    use tempfile::tempdir;

    use super::*;
    use crate::file::FileManager;
    use crate::fn_registry::FnRegistry;

    #[test]
    fn index_block_builder_and_reader() {
        let ex = LocalExecutor::default();
        ex.run(async {
            let mut builder = IndexBlockBuilder::new();

            builder.add_entry(&b"key1".to_vec(), 1, 3);
            builder.add_entry(&b"key2".to_vec(), 1, 10);

            let (block_type, bytes) = builder.finish();
            assert_eq!(BlockType::IndexBlock, block_type);

            let memindex = IndexBlockReader::read(bytes).unwrap();
            assert_eq!(memindex.get(&(1, b"key1".to_vec())).unwrap(), Some(3));
            assert_eq!(memindex.get(&(1, b"key2".to_vec())).unwrap(), Some(10));
        });
    }

    #[test]
    fn simple_table_builder() {
        let ex = LocalExecutor::default();
        ex.run(async {
            let base_dir = tempdir().unwrap();
            let file_manager = FileManager::with_base_dir(base_dir.path()).unwrap();
            let ctx = Arc::new(Context {
                file_manager,
                fn_registry: FnRegistry::new_noop(),
            });
            let mut table_builder =
                TableBuilder::begin(1, 1, ctx.file_manager.open_sstable(1, 1).await.unwrap());
            let mut index_bb = IndexBlockBuilder::new();

            let indices = vec![
                (b"key1".to_vec(), 1, 1),
                (b"key2key2".to_vec(), 2, 2),
                (b"key333".to_vec(), 3, 3),
            ];

            for index in &indices {
                index_bb.add_entry(&index.0, index.1, index.2);
            }
            table_builder.add_block(index_bb);
            table_builder.finish().await.unwrap();

            let table_handle = SSTable::open(ctx.file_manager.open_sstable(1, 1).await.unwrap())
                .await
                .unwrap()
                .into_read_handle(ctx)
                .await
                .unwrap();

            for index in indices {
                assert_eq!(
                    table_handle.get_offset(&(index.1, index.0)).unwrap(),
                    Some(index.2)
                );
            }
            assert_eq!(
                table_handle
                    .get_offset(&(233, b"not exist".to_vec()))
                    .unwrap(),
                None
            );
        });
    }
}
