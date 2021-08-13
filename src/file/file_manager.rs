use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::warn;

use crate::error::Result;
use crate::io::File;
use crate::types::{Bytes, LevelId, LevelInfo, ThreadId};
use crate::util::{AssertSend, AssertSync};

const COMMON_FILE_PREFIX: &str = "helix";
const COMMON_FILE_EXTENSION: &str = "hlx";
const BINARY_FILE_EXTENSION: &str = "bin";

const LEVEL_INFO_FILENAME: &str = "LEVEL_INFO";

crate enum FileType {
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

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
crate enum FileNo {
    LevelInfo,
    Rick(LevelId),
    SSTable(LevelId),
}

impl FileNo {
    fn name(&self) -> String {
        match self {
            FileNo::LevelInfo => "LEVEL_INFO".to_string(),
            FileNo::Rick(l_id) => format!("rick-{}.{}", l_id, BINARY_FILE_EXTENSION),
            FileNo::SSTable(l_id) => format!("sst-{}.{}", l_id, BINARY_FILE_EXTENSION),
        }
    }
}

crate enum OtherType {
    /// Timestamp range of each level.
    LevelInfo,
    /// Thread Identifier.
    TId,
}

#[derive(Clone)]
struct RawFilePtr(Rc<File>);

unsafe impl Send for RawFilePtr {}
unsafe impl Sync for RawFilePtr {}

/// Proxy for all file open/create operations.
///
/// It will keep opened file until a explicit garbage collection. So others
/// needn't to close file.
crate struct FileManager {
    base_dir: PathBuf,
    // todo: GC. maybe do it when outdating some level.
    fd_pool: Arc<Mutex<HashMap<(ThreadId, FileNo), RawFilePtr>>>,
}

impl AssertSync for FileManager {}
impl AssertSend for FileManager {}

impl FileManager {
    pub fn with_base_dir<P: AsRef<Path>>(base_dir: P, shards: usize) -> Result<Self> {
        fs::create_dir_all(base_dir.as_ref())?;

        // check dir
        let dir_num = fs::read_dir(base_dir.as_ref())?
            .map(|dir| Ok(dir?.file_type()?.is_dir()))
            .collect::<Result<Vec<_>>>()?
            .len();
        if dir_num != shards {
            warn!(
                "Detected {} folder in {:?}, which isn't equal to given shard number {}",
                dir_num,
                base_dir.as_ref().to_str(),
                shards
            );
        }

        // create sub dir
        for id in 0..shards {
            match fs::create_dir(base_dir.as_ref().join(id.to_string())) {
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                other => other?,
            }
        }

        Ok(Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            fd_pool: Arc::default(),
        })
    }

    crate async fn open(&self, tid: ThreadId, file_no: FileNo) -> Result<Rc<File>> {
        if let Some(file_ptr) = self.fd_pool.lock().await.get(&(tid, file_no)) {
            return Ok(file_ptr.0.clone());
        }

        let name = file_no.name();
        let path = self.base_dir.join(tid.to_string()).join(name);
        let file = Rc::new(File::open(path).await?);
        let cache_file = file.clone();
        self.fd_pool
            .lock()
            .await
            .insert((tid, file_no), RawFilePtr(cache_file));

        Ok(file)
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
        file.close().await?;

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
        file.close().await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::os::unix::io::AsRawFd;

    use glommio::LocalExecutor;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn new_file_manager() {
        let ex = LocalExecutor::default();
        let base_dir = tempdir().unwrap();
        let file_manager = FileManager::with_base_dir(base_dir.path(), 1).unwrap();

        ex.run(async {
            let _ = file_manager.open_rick(1).await.unwrap();
            assert_eq!(base_dir.path().read_dir().unwrap().count(), 1);
        });
    }

    #[test]
    fn reopen_file() {
        let ex = LocalExecutor::default();
        let base_dir = tempdir().unwrap();
        let file_manager = FileManager::with_base_dir(base_dir.path(), 1).unwrap();

        ex.run(async {
            let info_file = file_manager.open(0, FileNo::LevelInfo).await.unwrap();
            let first_fd = info_file.as_raw_fd();

            drop(info_file);
            let info_file = file_manager.open(0, FileNo::LevelInfo).await.unwrap();
            let second_fd = info_file.as_raw_fd();

            assert_eq!(first_fd, second_fd);
        });
    }
}
