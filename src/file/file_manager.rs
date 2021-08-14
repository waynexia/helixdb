use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::warn;

use crate::error::{HelixError, Result};
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
    crate fn with_base_dir<P: AsRef<Path>>(base_dir: P, shards: usize) -> Result<Self> {
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

    /// Close files not used by others, i.e., strong count is 1.
    ///
    /// # Notice
    /// As [FileManager] is shared between all shards, it keep all files that
    /// should not be visible to other shards. Trying to close with wrong `tid`
    /// is undefined behavior.
    crate async fn close_some(&self, tid: ThreadId) -> Result<()> {
        let free_list = self
            .fd_pool
            .lock()
            .await
            .drain_filter(|(thread_id, _), file| {
                *thread_id == tid && Rc::strong_count(&file.0) == 1
            })
            .collect::<Vec<_>>();

        for (_, file) in free_list {
            match Rc::try_unwrap(file.0) {
                Ok(file) => file.close().await?,
                Err(file) => {
                    return Err(HelixError::Unreachable(
                        "Going to close a file which is still referenced".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    crate async fn open_sstable(&self, tid: ThreadId, level_id: LevelId) -> Result<File> {
        let filename = self.base_dir.join(format!(
            "{}-{}-{}.{}",
            "sst", tid, level_id, BINARY_FILE_EXTENSION,
        ));

        Ok(File::open(filename).await?)
    }

    crate async fn open_vlog(&self, tid: ThreadId, level_id: LevelId) -> Result<File> {
        let filename = self.base_dir.join(format!(
            "{}-{}-{}.{}",
            "vlog", tid, level_id, BINARY_FILE_EXTENSION,
        ));

        Ok(File::open(filename).await?)
    }

    /// Open or create [LevelInfo].
    crate async fn open_level_info(&self) -> Result<LevelInfo> {
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
    crate async fn sync_level_info(&self, bytes: Bytes) -> Result<()> {
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
        let file_manager = FileManager::with_base_dir(base_dir.path(), 8).unwrap();

        ex.run(async {
            assert_eq!(base_dir.path().read_dir().unwrap().count(), 8);
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
