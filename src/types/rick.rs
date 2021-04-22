use flatbuffers::FlatBufferBuilder;

use super::Bytes;

pub type Offset = u64;

/// [Rick] file's super block.
///
/// The binary representation will be padded to 4KB.
#[derive(Debug, PartialEq, Eq)]
pub struct RickSuperBlock {
    pub is_ordered: bool,
    pub legal_offset_start: Offset,
    pub legal_offset_end: Offset,
    // todo: add `version` and `crc` fields
}

impl RickSuperBlock {
    pub const Length: usize = 4096;

    pub fn encode(&self) -> Bytes {
        let mut fbb = FlatBufferBuilder::new();

        let legal_offset_start = protos::Offset::new(self.legal_offset_start);
        let legal_offset_end = protos::Offset::new(self.legal_offset_end);

        let sb = protos::RickSuperBlock::create(
            &mut fbb,
            &protos::RickSuperBlockArgs {
                is_ordered: self.is_ordered,
                legal_offset_start: Some(&legal_offset_start),
                legal_offset_end: Some(&legal_offset_end),
            },
        );

        fbb.finish(sb, None);
        let mut padding_bytes = fbb.finished_data().to_vec();

        // the un-padding bytes should shorter than 4096 otherwise it will be truncated.
        debug_assert_eq!(true, padding_bytes.len() <= 4096);
        // padding it. Flatbuffers has the information about payload's length, so tailing
        // zero doesn't matter.
        padding_bytes.resize(Self::Length, 0);
        padding_bytes
    }

    pub fn decode(bytes: &[u8]) -> Self {
        let fb_sb = flatbuffers::get_root::<protos::RickSuperBlock<'_>>(bytes);

        Self {
            is_ordered: fb_sb.is_ordered(),
            legal_offset_start: fb_sb.legal_offset_start().unwrap().offset(),
            legal_offset_end: fb_sb.legal_offset_end().unwrap().offset(),
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
        };

        let bytes = sb.encode();
        assert_eq!(bytes.len(), RickSuperBlock::Length);

        assert_eq!(sb, RickSuperBlock::decode(&bytes));
    }
}