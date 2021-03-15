use crate::error::Result;
use crate::file::FileManager;
use crate::types::{LevelId, Timestamp};

#[deprecated]
pub struct LevelInfoHandle {}

impl LevelInfoHandle {
    pub fn new(file_manager: &FileManager) -> Self {
        todo!()
    }

    pub fn num_levels(&self) -> usize {
        todo!()
    }

    pub fn find_level(&self, timestamp: Timestamp) -> Option<usize> {
        todo!()
    }

    pub fn get_level_range(&self, level_number: usize) -> Option<(Timestamp, Timestamp)> {
        todo!()
    }

    pub fn get_level_id(&self, timestamp: Timestamp) -> Option<LevelId> {
        todo!()
    }

    pub fn add_level(&mut self, start: Timestamp, end: Timestamp) -> Result<()> {
        todo!()
    }

    pub fn remove_last_level(&mut self) -> Result<()> {
        todo!()
    }
}
