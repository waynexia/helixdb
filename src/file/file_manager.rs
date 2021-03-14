use std::fs::{self, remove_file, File};
// use std::fs;
use std::io::Read;
use std::io::Write;
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::Result;
// use crate::io::File;
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

    /// filename is consist of general prefix, file type and creating timestamp.
    /// For example, `helix-manifest-160000000.hlx`
    #[deprecated]
    pub async fn create(&self, ty: FileType) -> Result<(File, String)> {
        if let FileType::Others(filename) = ty {
            return self.create_others(filename).await;
        }

        let filename = self.base_dir.join(format!(
            "{}-{}-{}.{}",
            COMMON_FILE_PREFIX,
            ty.file_name_desc(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            COMMON_FILE_EXTENSION
        ));

        let file = File::with_options()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(filename.clone())?;
        let filename = filename.into_os_string().into_string().unwrap();

        Ok((file, filename))
    }

    // might deprecate this.
    /// Create files in `Others` type. Like `LEVEL_INFO`.
    async fn create_others(&self, filename: String) -> Result<(File, String)> {
        let filename = self.base_dir.join(filename);
        let file = File::with_options()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(filename.clone())?;
        let filename = filename.into_os_string().into_string().unwrap();

        Ok((file, filename))
    }

    pub async fn remove<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        remove_file(path)?;

        Ok(())
    }

    pub async fn open_<P: AsRef<Path>>(&self, filename: P) -> Result<File> {
        Ok(File::with_options()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(filename)?)
    }

    /// open or create required file.
    pub async fn open(&self, tid: ThreadId, ty: FileType) -> Result<File> {
        todo!()
    }

    /// Initialize / recover manager's state from manifest file.
    fn init() -> Result<()> {
        todo!()
    }

    pub async fn open_sstable(&self, tid: ThreadId, level_id: LevelId) -> Result<File> {
        let filename = self.base_dir.join(format!(
            "{}-{}-{}.{}",
            "sst", tid, level_id, BINARY_FILE_EXTENSION,
        ));

        Ok(File::with_options()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(filename)?)
    }

    // todo: remove vlog_name in return value
    pub async fn open_vlog(&self, tid: ThreadId, level_id: LevelId) -> Result<(File, String)> {
        let filename = self.base_dir.join(format!(
            "{}-{}-{}.{}",
            "vlog", tid, level_id, BINARY_FILE_EXTENSION,
        ));

        Ok((
            File::with_options()
                .read(true)
                .write(true)
                .truncate(false)
                .create(true)
                .open(filename.clone())?,
            filename.into_os_string().into_string().unwrap(),
        ))
    }

    /// Open or create [LevelInfo].
    pub async fn open_level_info(&self) -> Result<LevelInfo> {
        let filename = self.base_dir.join(LEVEL_INFO_FILENAME);
        let mut file = File::with_options()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(filename)?;

        // read all
        let mut buf = vec![];
        file.seek(SeekFrom::Start(0))?;
        file.read_to_end(&mut buf)?;

        let level_info = LevelInfo::decode(&buf);
        Ok(level_info)
    }

    // todo: correct this.
    /// Refresh (overwrite) level info file.
    pub async fn sync_level_info(&self, bytes: &Bytes) -> Result<()> {
        let filename = self.base_dir.join(LEVEL_INFO_FILENAME);
        let mut file = File::with_options()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(filename)?;

        file.write_all(bytes)?;
        file.sync_all()?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use tempfile::tempdir;

    #[tokio::test]
    async fn new_file_manager() {
        let base_dir = tempdir().unwrap();

        let file_manager = FileManager::with_base_dir(base_dir.path()).unwrap();
        let _ = file_manager.create(FileType::Manifest).await.unwrap();
        assert_eq!(base_dir.path().read_dir().unwrap().count(), 1);
    }
}
