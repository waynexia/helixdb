use flatbuffers::FlatBufferBuilder;

use super::{Bytes, LevelId, Offset, ThreadId, Timestamp};

/// Enumeration of blocks' type
pub type BlockType = protos::BlockType;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) struct BlockInfo {
    pub block_type: BlockType,
    pub offset: Offset,
    pub length: u64,
}

impl BlockInfo {
    pub fn to_generated_type(&self) -> protos::BlockInfo {
        let offset = protos::Offset::new(self.offset);

        protos::BlockInfo::new(self.block_type, &offset, self.length)
    }
}

impl From<protos::BlockInfo> for BlockInfo {
    fn from(fb_info: protos::BlockInfo) -> BlockInfo {
        Self {
            block_type: fb_info.block_type(),
            offset: fb_info.offset().offset(),
            length: fb_info.length(),
        }
    }
}

/// Will be padded to 4096 bytes.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SSTableSuperBlock {
    pub thread_id: ThreadId,
    pub level_id: LevelId,
    pub blocks: Vec<BlockInfo>,
}

impl SSTableSuperBlock {
    pub const LENGTH: usize = 4096;

    pub fn encode(&self) -> Bytes {
        let mut fbb = FlatBufferBuilder::new();

        let thread_id = protos::ThreadId::new(self.thread_id);
        let level_id = protos::LevelId::new(self.level_id);
        fbb.start_vector::<protos::BlockInfo>(self.blocks.len());
        for info in &self.blocks {
            fbb.push(info.to_generated_type());
        }
        let blocks = fbb.end_vector::<protos::BlockInfo>(self.blocks.len());
        let blocks = protos::SSTableSuperBlock::create(
            &mut fbb,
            &protos::SSTableSuperBlockArgs {
                thread_id: Some(&thread_id),
                level_id: Some(&level_id),
                blocks: Some(blocks),
            },
        );

        fbb.finish(blocks, None);
        let mut padding_bytes = fbb.finished_data().to_vec();

        // the un-padding bytes should shorter than 4096 otherwise it will be truncated.
        debug_assert_eq!(true, padding_bytes.len() <= Self::LENGTH);
        // padding it. Flatbuffers has the information about payload's length, so tailing
        // zero doesn't matter.
        padding_bytes.resize(Self::LENGTH, 0);
        padding_bytes
    }

    pub fn decode(bytes: &[u8]) -> Self {
        if bytes.is_empty() {
            return Self {
                blocks: vec![],
                thread_id: 0,
                level_id: 0,
            };
        }

        let fb_blocks = flatbuffers::get_root::<protos::SSTableSuperBlock<'_>>(bytes);
        let thread_id = fb_blocks.thread_id().unwrap().id();
        let level_id = fb_blocks.level_id().unwrap().id();
        let blocks = fb_blocks
            .blocks()
            .unwrap()
            .to_owned()
            .into_iter()
            .map(BlockInfo::from)
            .collect();

        Self {
            thread_id,
            level_id,
            blocks,
        }
    }

    /// Get blocks info of given block type. There may have many blocks with
    /// the same types (but not tested yet. this is a todo).
    pub fn get_block_info(&self, block_type: BlockType) -> Vec<BlockInfo> {
        let mut result = vec![];
        for block in &self.blocks {
            if block.block_type == block_type {
                result.push(*block);
            }
        }

        result
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct IndexBlockEntry {
    pub value_offset: Offset,
    pub timestamp: Timestamp,
    pub key: Bytes,
}

impl IndexBlockEntry {
    pub fn encode(&self) -> Bytes {
        let mut fbb = FlatBufferBuilder::new();

        let value_offset = protos::Offset::new(self.value_offset);
        let timestamp = protos::Timestamp::new(self.timestamp);
        let key_bytes = fbb.create_vector_direct(&self.key);

        let entry = protos::IndexBlockEntry::create(
            &mut fbb,
            &protos::IndexBlockEntryArgs {
                value_offset: Some(&value_offset),
                timestamp: Some(&timestamp),
                key: Some(key_bytes),
            },
        );

        fbb.finish(entry, None);
        fbb.finished_data().to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Self {
        let fb_entry = flatbuffers::get_root::<protos::IndexBlockEntry<'_>>(bytes);

        Self {
            value_offset: fb_entry.value_offset().unwrap().offset(),
            timestamp: fb_entry.timestamp().unwrap().timestamp(),
            key: fb_entry.key().unwrap().to_vec(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sstable_super_block_codec() {
        let block_info = BlockInfo {
            block_type: BlockType::IndexBlock,
            offset: 40960,
            length: 10240,
        };
        let sb = SSTableSuperBlock {
            thread_id: 3,
            level_id: 5,
            blocks: vec![block_info],
        };

        let bytes = sb.encode();
        assert_eq!(bytes.len(), SSTableSuperBlock::LENGTH);
        assert_eq!(sb, SSTableSuperBlock::decode(&bytes));
    }

    #[test]
    fn sstable_index_entry_codec() {
        let entry = IndexBlockEntry {
            value_offset: 40960,
            timestamp: 12345,
            key: b"value".to_vec(),
        };

        let bytes = entry.encode();
        assert_eq!(entry, IndexBlockEntry::decode(&bytes));
    }
}
