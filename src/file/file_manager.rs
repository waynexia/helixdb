use std::fs;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::io::File;
use crate::types::{Bytes, LevelId, LevelInfo, ThreadId};

const COMMON_FILE_PREFIX: &str = "helix";
const COMMON_FILE_EXTENSION: &str = "hlx";
const BINARY_FILE_EXTENSION: &str = "bin";

const LEVEL_INFO_FILENAME: &str = "LEVEL_INFO";

pub enum FileType {
    Rick,
    VLog,
    SSTable,
    Manifest,
    Others(String),
}

impl FileType {
    fn file_name_desc(&self) -> &str {
        match self {
            FileType::Rick => "rick",
            FileType::VLog => "vlog",
            FileType::SSTable => "sst",
            FileType::Manifest => "manifest",
            FileType::Others(name) => name,
        }
    }
}

pub enum OtherType {
    /// Timestamp range of each level.
    LevelInfo,
    /// Thread Identifier.
    TId,
}

pub struct FileManager {
    base_dir: PathBuf,
}

impl FileManager {
    pub fn with_base_dir<P: AsRef<Path>>(base_dir: P) -> Result<Self> {
        fs::create_dir_all(base_dir.as_ref())?;

        Ok(Self {
            base_dir: base_dir.as_ref().to_path_buf(),
        })
    }

    // might deprecate this.
    /// Create files in `Others` type. Like `LEVEL_INFO`.
    async fn create_others(&self, filename: String) -> Result<(File, String)> {
        let filename = self.base_dir.join(filename);
        let file = File::open(filename.clone()).await?;
        let filename = filename.into_os_string().into_string().unwrap();

        Ok((file, filename))
    }

    /// Initialize / recover manager's state from manifest file.
    fn init() -> Result<()> {
        todo!()
    }

    pub async fn open_rick(&self, tid: ThreadId) -> Result<File> {
        let filename = self
            .base_dir
            .join(format!("{}-{}.{}", "rick", tid, BINARY_FILE_EXTENSION));

        Ok(File::open(filename).await?)
    }

    pub async fn open_sstable(&self, tid: ThreadId, level_id: LevelId) -> Result<File> {
        let filename = self.base_dir.join(format!(
            "{}-{}-{}.{}",
            "sst", tid, level_id, BINARY_FILE_EXTENSION,
        ));

        Ok(File::open(filename).await?)
    }

    pub async fn open_vlog(&self, tid: ThreadId, level_id: LevelId) -> Result<File> {
        let filename = self.base_dir.join(format!(
            "{}-{}-{}.{}",
            "vlog", tid, level_id, BINARY_FILE_EXTENSION,
        ));

        Ok(File::open(filename).await?)
    }

    /// Open or create [LevelInfo].
    pub async fn open_level_info(&self) -> Result<LevelInfo> {
        let filename = self.base_dir.join(LEVEL_INFO_FILENAME);
        let file = File::open(filename).await?;

        // read all
        let size = file.size().await?;
        let buf = file.read(0, size).await?;

        let level_info = LevelInfo::decode(&buf);
        Ok(level_info)
    }

    // todo: correct this.
    /// Refresh (overwrite) level info file.
    pub async fn sync_level_info(&self, bytes: Bytes) -> Result<()> {
        let filename = self.base_dir.join(LEVEL_INFO_FILENAME);
        let file = File::open(filename).await?;

        file.write(bytes, 0).await?;
        file.sync().await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use glommio::LocalExecutor;
    use tempfile::tempdir;

    #[test]
    fn new_file_manager() {
        let ex = LocalExecutor::default();
        ex.run(async {
            let base_dir = tempdir().unwrap();

            let file_manager = FileManager::with_base_dir(base_dir.path()).unwrap();
            let _ = file_manager.open_rick(1).await.unwrap();
            assert_eq!(base_dir.path().read_dir().unwrap().count(), 1);
        });
    }
}
