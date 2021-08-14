use std::sync::Arc;

use crate::context::Context;
use crate::error::Result;
use crate::file::{Rick, SSTable};
use crate::index::MemIndex;
#[cfg(test)]
use crate::types::Offset;
use crate::types::{Bytes, Entry, LevelId, ThreadId, Timestamp};

#[derive(Hash, PartialEq, Eq)]
pub struct TableIdentifier {
    pub tid: ThreadId,
    pub lid: LevelId,
}

impl From<(ThreadId, LevelId)> for TableIdentifier {
    fn from(ids: (ThreadId, LevelId)) -> Self {
        Self {
            tid: ids.0,
            lid: ids.1,
        }
    }
}

/// Provides read methods to SSTable.
///
/// If wants to modify a sstable should upgrade to a writable handle
/// (unimplemented).
pub struct TableReadHandle {
    memindex: MemIndex,
    sstable: SSTable,
    rick: Rick,
    ctx: Arc<Context>,
}

impl TableReadHandle {
    pub fn new(memindex: MemIndex, sstable: SSTable, rick: Rick, ctx: Arc<Context>) -> Self {
        Self {
            memindex,
            sstable,
            rick,
            ctx,
        }
    }

    pub async fn get(&self, time_key: &(Timestamp, Bytes)) -> Result<Option<Entry>> {
        let offset = if self.is_compressed() {
            let mut align_time_key = time_key.clone();
            align_time_key.0 = self.rick.get_align_ts();
            self.memindex.get(&align_time_key)?
        } else {
            self.memindex.get(time_key)?
        };
        if let Some(offset) = offset {
            Ok(Some(self.rick.read(offset).await?))
        } else {
            Ok(None)
        }
    }

    /// Upgrade to writeable handle.
    pub fn upgrade() -> ! {
        todo!()
    }

    pub fn is_compressed(&self) -> bool {
        self.rick.is_compressed()
    }

    // For test case.
    /// Get value's offset in rick file.
    #[cfg(test)]
    pub fn get_offset(&self, time_key: &(Timestamp, Bytes)) -> Result<Option<Offset>> {
        self.memindex.get(time_key)
    }

    fn decompress_entry(&self, key: &[u8], data: &[u8]) -> Result<Vec<(Timestamp, Bytes)>> {
        self.ctx.fn_registry.decompress_entries(key, data)
    }
}
