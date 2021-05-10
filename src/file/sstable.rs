use std::collections::HashMap;
use std::convert::TryInto;
use std::mem::size_of;
use std::sync::Arc;

use crate::context::Context;
use crate::error::Result;
use crate::file::VLog;
use crate::index::MemIndex;
use crate::io::File;
use crate::table::{SSTableHandle, TableIterator};
use crate::types::sstable::{BlockInfo, BlockType, IndexBlockEntry, SSTableSuperBlock};
use crate::types::{Bytes, LevelId, Offset, ThreadId, Timestamp};
use crate::util::{decode_u64, encode_u64};

pub struct SSTable {
    file: File,
    sb: SSTableSuperBlock,
}

impl SSTable {
    #[deprecated]
    pub fn new(file: File, tid: ThreadId, level_id: LevelId) -> Self {
        todo!()
        // Self {
        //     file,
        //     tid,
        //     level_id,
        // }
    }

    pub async fn open(file: File) -> Result<Self> {
        let sb = Self::read_super_block(&file).await?;

        Ok(Self { file, sb })
    }

    #[deprecated]
    pub fn metadata(&self) -> Result<TableMeta> {
        Ok(TableMeta {
            start_timestamp: 0,
            end_timestamp: 0,
        })
    }

    /// Get read handle of SSTable.
    pub async fn handle(&mut self, ctx: Arc<Context>) -> Result<SSTableHandle> {
        let (index, raw_entry_positions) = self.read_table().await?;
        let vlog = VLog::from(
            ctx.file_manager
                .open_vlog(self.sb.thread_id, self.sb.level_id)
                .await?,
        );

        Ok(SSTableHandle::new(
            ctx,
            index,
            raw_entry_positions,
            vlog,
            self.metadata()?,
        ))
    }

    /// Construct a iterator on SSTable.
    ///
    /// Todo: use trait in std::iter.
    ///
    /// Todo: avoid read all data?
    // todo: fix clippy needless_lifetimes?
    pub async fn iterator<'ctx>(&mut self, ctx: &'ctx Context) -> Result<TableIterator<'ctx>> {
        let (index, raw_entry_positions) = self.read_table().await?;
        let vlog = VLog::from(
            ctx.file_manager
                .open_vlog(self.sb.thread_id, self.sb.level_id)
                .await?,
        );

        TableIterator::try_new(index, raw_entry_positions, self.metadata()?, ctx, vlog).await
    }

    // todo: remove this temp method.
    /// out-param: index, raw_entry_positions.
    #[allow(clippy::type_complexity)]
    #[deprecated]
    async fn read_table(&mut self) -> Result<(HashMap<Bytes, usize>, Vec<(Bytes, u64, u64)>)> {
        let mut table_index = HashMap::new();
        const PREFIX_LENGTH: usize = 3 * size_of::<u64>();
        const PREFIX_UNIT_LENGTH: usize = size_of::<u64>();

        // get data block size
        let data_block_size = self.file.size().await?;

        // make key-value index
        let mut raw_entry_positions = vec![];
        let contents = self.file.read(0, data_block_size).await?;
        let mut index = 0;

        loop {
            let prefix_buf = &contents[index..index + PREFIX_LENGTH];
            index += PREFIX_LENGTH;
            let value_offset =
                u64::from_le_bytes(prefix_buf[0..PREFIX_UNIT_LENGTH].try_into().unwrap());
            let value_size = u64::from_le_bytes(
                prefix_buf[PREFIX_UNIT_LENGTH..PREFIX_UNIT_LENGTH * 2]
                    .try_into()
                    .unwrap(),
            );
            let key_length = u64::from_le_bytes(
                prefix_buf[PREFIX_UNIT_LENGTH * 2..PREFIX_UNIT_LENGTH * 3]
                    .try_into()
                    .unwrap(),
            ) as usize;

            let key_buf = contents[index..index + key_length].to_owned();
            index += key_length;
            table_index.insert(key_buf.clone(), raw_entry_positions.len());
            raw_entry_positions.push((key_buf, value_offset, value_size));

            if index >= data_block_size as usize {
                break;
            }
        }

        Ok((table_index, raw_entry_positions))
    }

    async fn load_index_block(&self) -> Result<MemIndex> {
        // let index_block_bytes = self.
        todo!()
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
            tail_offset: 0,
        }
    }

    /// in-params: key, [(offset, size),]
    ///
    /// format: | value offset (u64) | value size (u64) | key size (u64) | key |
    #[deprecated]
    pub fn add_entries(&mut self, keys: Vec<Bytes>, offsets: Vec<(u64, u64)>) {
        let mut entries = Vec::with_capacity(keys.len());
        for (mut key, (offset, size)) in keys.into_iter().zip(offsets.into_iter()) {
            let mut bytes = vec![];
            let key_size = key.len() as u64;
            bytes.append(&mut offset.to_le_bytes().to_vec());
            bytes.append(&mut size.to_le_bytes().to_vec());
            bytes.append(&mut key_size.to_le_bytes().to_vec());
            bytes.append(&mut key);

            entries.append(&mut bytes);
        }

        self.block_buffer.append(&mut entries);
    }

    pub fn add_block(&mut self, block_builder: impl BlockBuilder) {
        let (block_type, block_data) = block_builder.finish();

        let block_size = block_data.len() as u64;
        self.blocks.push(BlockInfo {
            block_type,
            offset: self.tail_offset,
            length: block_size,
        });
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

#[deprecated]
#[derive(Debug)]
pub struct TableMeta {
    start_timestamp: Timestamp,
    end_timestamp: Timestamp,
}

impl TableMeta {
    pub fn start(&self) -> Timestamp {
        self.start_timestamp
    }

    pub fn end(&self) -> Timestamp {
        self.end_timestamp
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

            let keys = vec![b"key1".to_vec(), b"key2key2".to_vec(), b"key333".to_vec()];
            let offsets = vec![(1, 1), (2, 2), (3, 3)];
            table_builder.add_entries(keys.clone(), offsets.clone());
            table_builder.finish().await.unwrap();

            let table_handle = SSTable::open(ctx.file_manager.open_sstable(1, 1).await.unwrap())
                .await
                .unwrap()
                .handle(ctx)
                .await
                .unwrap();

            for (key, offset) in keys.into_iter().zip(offsets.into_iter()) {
                assert_eq!(table_handle.get_raw(&key).unwrap(), offset);
            }
            assert_eq!(table_handle.get_raw(&b"not exist".to_vec()), None);
        });
    }
}
