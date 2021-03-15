use std::collections::HashMap;
use std::convert::TryInto;
use std::mem::size_of;
use std::sync::Arc;

use crate::context::Context;
use crate::error::Result;
use crate::file::VLog;
use crate::io::File;
use crate::table::{SSTableHandle, TableIterator};
use crate::types::{Bytes, LevelId, ThreadId, Timestamp};

pub struct SSTable {
    file: File,
    tid: ThreadId,
    level_id: LevelId,
}

impl SSTable {
    pub fn new(file: File, tid: ThreadId, level_id: LevelId) -> Self {
        Self {
            file,
            tid,
            level_id,
        }
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
        let vlog = VLog::from(ctx.file_manager.open_vlog(self.tid, self.level_id).await?);

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
        let vlog = VLog::from(ctx.file_manager.open_vlog(self.tid, self.level_id).await?);

        TableIterator::try_new(index, raw_entry_positions, self.metadata()?, ctx, vlog).await
    }

    // todo: remove this temp method.
    /// out-param: index, raw_entry_positions.
    #[allow(clippy::type_complexity)]
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
}

pub struct TableBuilder {
    file: File,
    entry_buffer: Bytes,
}

// todo: need to load the whole sstable file to read?
impl TableBuilder {
    /// in-params: key, [(offset, size),]
    ///
    /// format: | value offset (u64) | value size (u64) | key size (u64) | key |
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

        self.entry_buffer.append(&mut entries);
    }

    /// Consume this builder to build a SSTable.
    pub async fn finish(self) -> Result<()> {
        // write data block
        let data_block_size = self.entry_buffer.len() as u64;
        self.file.write(self.entry_buffer, 0).await?;

        // todo: other blocks

        self.file.sync().await?;

        Ok(())
    }
}

impl From<File> for TableBuilder {
    fn from(file: File) -> Self {
        Self {
            file,
            entry_buffer: vec![],
        }
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
                TableBuilder::from(ctx.file_manager.open_sstable(1, 1).await.unwrap());

            let keys = vec![b"key1".to_vec(), b"key2key2".to_vec(), b"key333".to_vec()];
            let offsets = vec![(1, 1), (2, 2), (3, 3)];
            table_builder.add_entries(keys.clone(), offsets.clone());
            table_builder.finish().await.unwrap();

            let table_handle =
                SSTable::new(ctx.file_manager.open_sstable(1, 1).await.unwrap(), 1, 1)
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
