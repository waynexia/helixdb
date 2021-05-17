use std::collections::VecDeque;
use std::sync::RwLock;

use flatbuffers::FlatBufferBuilder;

use super::{Bytes, LevelId, Timestamp};
use crate::error::Result;
use crate::file::FileManager;

#[derive(Default, PartialEq, Debug, Clone, Copy)]
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
            end: time_range.end().timestamp(),
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

        protos::LevelDesc::new(&time_range, &id)
    }

    #[inline]
    pub fn is_timestamp_match(&self, timestamp: Timestamp) -> bool {
        self.start <= timestamp && timestamp <= self.end
    }
}

// This should be placed in heap? And each thread keeps a copy of it.
#[derive(Debug)]
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

        let infos =
            protos::LevelInfo::create(&mut fbb, &protos::LevelInfoArgs { infos: Some(batch) });

        fbb.finish(infos, None);
        fbb.finished_data().to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Self {
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

    /// Give a timestamp and find the level suits it.
    ///
    /// Rick entries' timestamp will not present in level-info.
    /// Thus if given timestamp is larger than the biggest timestamp recorded by
    /// this level-info, `Some(0)` will be returned. `0` is a special [LevelId]
    /// stands for Rick level.
    pub fn get_level_id(&self, timestamp: Timestamp) -> Option<LevelId> {
        let infos = self.infos.read().unwrap();

        // timestamp covered by rick will not present in level-info
        if infos.is_empty() || timestamp > infos[0].end {
            return Some(0);
        }

        for desc in &*infos {
            if desc.is_timestamp_match(timestamp) {
                return Some(desc.id);
            }
        }

        None
    }

    /// Return new level id.
    pub async fn add_level(
        &mut self,
        start: Timestamp,
        end: Timestamp,
        file_manager: &FileManager,
    ) -> Result<LevelId> {
        let mut new_desc = LevelDesc { start, end, id: 0 };

        let mut infos = self.infos.write().unwrap();
        let next_id = infos.front().map_or_else(|| 1, |desc| desc.id + 1);
        new_desc.id = next_id;

        infos.push_front(new_desc);
        drop(infos);

        self.sync(file_manager).await?;

        Ok(next_id)
    }

    pub async fn remove_last_level(&mut self, file_manager: &FileManager) -> Result<()> {
        self.infos.write().unwrap().pop_back();

        self.sync(file_manager).await
    }

    /// Sync file infos to disk. Requires read lock.
    async fn sync(&self, file_manager: &FileManager) -> Result<()> {
        let bytes = self.encode();
        file_manager.sync_level_info(bytes).await?;

        Ok(())
    }

    #[cfg(test)]
    fn new(descriptions: Vec<LevelDesc>) -> Self {
        let infos = VecDeque::from(descriptions);

        Self {
            infos: RwLock::new(infos),
        }
    }
}

#[cfg(test)]
mod test {

    use glommio::LocalExecutor;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn level_desc_codec() {
        let desc = LevelDesc {
            start: 21,
            end: 40,
            id: 4,
        };
        let info = LevelInfo::new(vec![desc]);

        let bytes = info.encode();
        let info = LevelInfo::decode(&bytes);
        let infos: Vec<_> = info.infos.read().unwrap().iter().copied().collect();

        assert_eq!(vec![desc], infos);
    }

    #[test]
    fn add_level() {
        let ex = LocalExecutor::default();
        ex.run(async {
            let base_dir = tempdir().unwrap();
            let file_manager = FileManager::with_base_dir(base_dir.path()).unwrap();

            let mut info = LevelInfo::new(vec![]);
            info.add_level(0, 9, &file_manager).await.unwrap();
            info.add_level(10, 19, &file_manager).await.unwrap();
            info.add_level(20, 29, &file_manager).await.unwrap();
            drop(info);

            let info = file_manager.open_level_info().await.unwrap();
            let infos: Vec<_> = info.infos.read().unwrap().iter().copied().collect();
            let expected = vec![
                LevelDesc {
                    start: 0,
                    end: 9,
                    id: 1,
                },
                LevelDesc {
                    start: 10,
                    end: 19,
                    id: 2,
                },
                LevelDesc {
                    start: 20,
                    end: 29,
                    id: 3,
                },
            ];

            assert_eq!(infos, expected);
        });
    }
}
