use std::fs::File;
use std::path::{Path, PathBuf};

use crate::error::Result;

pub enum FileType {
    Rick,
    VLog,
    SSTable,
    Manifist,
}

pub struct FileManager {
    base_dir: PathBuf,
}

impl FileManager {
    pub fn with_base_dir<P: AsRef<Path>>(base_dir: P) -> Result<Self> {
        todo!()
    }

    pub fn create(&self, ty: FileType) -> Result<File> {
        todo!()
    }

    pub fn remove<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        todo!()
    }
}
