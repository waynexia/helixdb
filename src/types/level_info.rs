use std::collections::VecDeque;

use flatbuffers::FlatBufferBuilder;

use super::{Bytes, LevelId, Timestamp};
use crate::error::Result;
use crate::file::FileManager;

#[derive(Default, PartialEq, Eq, Debug, Clone, Copy)]
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
    pub fn as_generated_type(&self) -> protos::LevelDesc {
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

/// Metadata of every levels. Is a array-like container of [LevelDesc].
///
/// [LevelDesc] is arranged from old (smaller timestamp) to new
/// (larger timestamp).
#[derive(Debug, PartialEq, Eq)]
pub struct LevelInfo {
    // todo: remove RwLock
    infos: VecDeque<LevelDesc>,
}

impl LevelInfo {
    pub fn encode(&self) -> Bytes {
        let mut fbb = FlatBufferBuilder::new();

        fbb.start_vector::<protos::LevelDesc>(self.infos.len());
        for desc in &self.infos {
            fbb.push(desc.as_generated_type());
        }
        let batch = fbb.end_vector::<protos::LevelDesc>(self.infos.len());

        let infos =
            protos::LevelInfo::create(&mut fbb, &protos::LevelInfoArgs { infos: Some(batch) });

        fbb.finish(infos, None);
        fbb.finished_data().to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Self {
        // for empty level-info file.
        if bytes.is_empty() {
            return Self {
                infos: VecDeque::default(),
            };
        }

        let fb_info = flatbuffers::root::<protos::LevelInfo<'_>>(bytes).unwrap();
        let infos = fb_info
            .infos()
            .unwrap()
            .to_owned()
            .into_iter()
            .rev() // `fbb.push()` in encode reversed the order
            .map(LevelDesc::from)
            .collect();

        Self { infos }
    }

    /// Give a timestamp and find the level suits it.
    ///
    /// Rick entries' timestamp will not present in level-info.
    /// Thus if given timestamp is larger than the biggest timestamp recorded by
    /// this level-info, `Some(0)` will be returned. `0` is a special [LevelId]
    /// stands for Rick level.
    pub fn get_level_id(&self, timestamp: Timestamp) -> Option<LevelId> {
        // timestamp covered by rick will not present in level-info
        if self.infos.is_empty() || timestamp > self.infos.back().unwrap().end {
            return Some(0);
        }

        for desc in &self.infos {
            if desc.is_timestamp_match(timestamp) {
                return Some(desc.id);
            }
        }

        None
    }

    /// Return new level id.
    crate async fn add_level(
        &mut self,
        start: Timestamp,
        end: Timestamp,
        file_manager: &FileManager,
    ) -> Result<LevelId> {
        let mut new_desc = LevelDesc { start, end, id: 0 };

        let next_id = self.infos.back().map_or_else(|| 1, |desc| desc.id + 1);
        new_desc.id = next_id;
        self.infos.push_back(new_desc);
        self.sync(file_manager).await?;

        Ok(next_id)
    }

    crate async fn remove_last_level(&mut self, file_manager: &FileManager) -> Result<()> {
        self.infos.pop_front();

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

        Self { infos }
    }
}

#[cfg(test)]
mod test {

    use glommio::LocalExecutor;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn level_desc_codec() {
        let infos = LevelInfo::new(vec![
            LevelDesc {
                start: 21,
                end: 40,
                id: 4,
            },
            LevelDesc {
                start: 100,
                end: 200,
                id: 8,
            },
        ]);

        let bytes = infos.encode();
        let decoded = LevelInfo::decode(&bytes);

        assert_eq!(decoded, infos);
    }

    #[test]
    fn add_level() {
        let ex = LocalExecutor::default();
        ex.run(async {
            let base_dir = tempdir().unwrap();
            let file_manager = FileManager::with_base_dir(base_dir.path(), 1).unwrap();

            let mut info = LevelInfo::new(vec![]);
            info.add_level(0, 9, &file_manager).await.unwrap();
            info.add_level(10, 19, &file_manager).await.unwrap();
            info.add_level(20, 29, &file_manager).await.unwrap();
            drop(info);

            let info = file_manager.open_level_info().await.unwrap();
            let infos: Vec<_> = info.infos.iter().copied().collect();
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
