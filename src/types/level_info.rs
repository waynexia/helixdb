use flatbuffers::{FlatBufferBuilder, Follow};
use std::collections::VecDeque;
use std::sync::RwLock;

use super::{Bytes, LevelId, Timestamp};
use crate::error::Result;

pub struct LevelDesc {
    start: Timestamp,
    end: Timestamp,
    id: LevelId,
}

impl From<protos::LevelDesc> for LevelDesc {
    fn from(fb_desc: protos::LevelDesc) -> LevelDesc {
        let time_range = fb_desc.time_range();
        Self {
            start: time_range.start().timestamp(),
            end: time_range.start().timestamp(),
            id: fb_desc.id().id(),
        }
    }
}

impl LevelDesc {
    pub fn to_generated_type(&self) -> protos::LevelDesc {
        let start = protos::Timestamp::new(self.start);
        let end = protos::Timestamp::new(self.end);
        let time_range = protos::TimeRange::new(&start, &end);

        let id = protos::LevelId::new(self.id);

        let desc = protos::LevelDesc::new(&time_range, &id);

        desc
    }

    #[inline]
    pub fn is_timestamp_match(&self, timestamp: Timestamp) -> bool {
        self.start <= timestamp && timestamp >= self.end
    }
}

// This should be placed in heap? And each thread keeps a copy to it.
pub struct LevelInfo {
    infos: RwLock<VecDeque<LevelDesc>>,
}

impl LevelInfo {
    pub fn encode(&self) -> Bytes {
        let mut fbb = FlatBufferBuilder::new();
        let infos = self.infos.read().unwrap();

        fbb.start_vector::<protos::LevelDesc>(infos.len());
        for desc in &*infos {
            fbb.push(desc.to_generated_type());
        }
        let batch = fbb.end_vector::<protos::LevelDesc>(infos.len());

        fbb.finish(batch, None);
        fbb.finished_data().to_vec()
    }

    pub fn decode(bytes: &Bytes) -> Self {
        // for empty level-info file.
        if bytes.is_empty() {
            return Self {
                infos: RwLock::new(VecDeque::default()),
            };
        }

        let fb_info = flatbuffers::get_root::<protos::LevelInfo<'_>>(bytes);
        let infos = fb_info
            .infos()
            .unwrap()
            .to_owned()
            .into_iter()
            .map(LevelDesc::from)
            .collect();

        Self {
            infos: RwLock::new(infos),
        }
    }

    // todo: use one lock to accomplish following two methods.
    /// find level index.
    pub fn find_level(&self, timestamp: Timestamp) -> Option<usize> {
        let infos = self.infos.read().unwrap();

        // timestamp covered by rick will not present in level-info
        if infos.is_empty() || timestamp < infos[0].start {
            return Some(0);
        }

        for (pos, desc) in infos.iter().enumerate() {
            if desc.is_timestamp_match(timestamp) {
                return Some(pos);
            }
        }

        None
    }

    pub fn get_level_id(&self, timestamp: Timestamp) -> Option<LevelId> {
        let infos = self.infos.read().unwrap();

        // timestamp covered by rick will not present in level-info
        if infos.is_empty() || timestamp < infos[0].start {
            return Some(0);
        }

        for desc in &*infos {
            if desc.is_timestamp_match(timestamp) {
                return Some(desc.id);
            }
        }

        None
    }

    pub fn add_level(&mut self, start: Timestamp, end: Timestamp) -> Result<()> {
        todo!()
    }

    pub fn remove_last_level(&mut self) -> Result<()> {
        todo!()
    }
}
