use flatbuffers::FlatBufferBuilder;

use super::{Bytes, Timestamp};

pub(crate) type Offset = u64;

pub(crate) type ValueFormat = protos::ValueFormat;

/// [Rick] file's super block.
///
/// The binary representation will be padded to 4KB.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RickSuperBlock {
    pub is_ordered: bool,
    pub legal_offset_start: Offset,
    pub legal_offset_end: Offset,
    // todo: add `version` and `crc` fields
    pub value_format: ValueFormat,
    /// Only valid when value format is `CompressedValue`
    pub align_timestamp: Timestamp,
}

impl RickSuperBlock {
    pub const LENGTH: usize = 4096;

    pub fn encode(&self) -> Bytes {
        let mut fbb = FlatBufferBuilder::new();

        let legal_offset_start = protos::Offset::new(self.legal_offset_start);
        let legal_offset_end = protos::Offset::new(self.legal_offset_end);
        let align_timestamp = protos::Timestamp::new(self.align_timestamp);

        let sb = protos::RickSuperBlock::create(
            &mut fbb,
            &protos::RickSuperBlockArgs {
                is_ordered: self.is_ordered,
                legal_offset_start: Some(&legal_offset_start),
                legal_offset_end: Some(&legal_offset_end),
                value_format: self.value_format,
                align_timestamp: Some(&align_timestamp),
            },
        );

        fbb.finish(sb, None);
        let mut padding_bytes = fbb.finished_data().to_vec();

        // the un-padding bytes should shorter than 4096 otherwise it will be truncated.
        debug_assert!(padding_bytes.len() <= Self::LENGTH);
        // padding it. Flatbuffers has the information about payload's length, so tailing
        // zero doesn't matter.
        padding_bytes.resize(Self::LENGTH, 0);
        padding_bytes
    }

    pub fn decode(bytes: &[u8]) -> Self {
        let fb_sb = flatbuffers::root::<protos::RickSuperBlock<'_>>(bytes).unwrap();
        // let fb_sb = flatbuffers::get_root::<protos::RickSuperBlock<'_>>(bytes);

        Self {
            is_ordered: fb_sb.is_ordered(),
            legal_offset_start: fb_sb.legal_offset_start().unwrap().offset(),
            legal_offset_end: fb_sb.legal_offset_end().unwrap().offset(),
            value_format: fb_sb.value_format(),
            align_timestamp: fb_sb.align_timestamp().unwrap().timestamp(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn rick_super_block_codec() {
        let sb = RickSuperBlock {
            is_ordered: true,
            legal_offset_start: 4096,
            legal_offset_end: 8192,
            value_format: ValueFormat::RawValue,
            align_timestamp: 10086,
        };

        let bytes = sb.encode();
        assert_eq!(bytes.len(), RickSuperBlock::LENGTH);
        assert_eq!(sb, RickSuperBlock::decode(&bytes));
    }
}
