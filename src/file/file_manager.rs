use std::fs::{self, remove_file, File};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::Result;

const COMMON_FILE_PREFIX: &str = "helix";
const COMMON_FILE_EXTENSION: &str = "hlx";

pub enum FileType {
    Rick,
    VLog,
    SSTable,
    Manifest,
}

impl FileType {
    fn file_name_desc(&self) -> &str {
        match self {
            FileType::Rick => "rick",
            FileType::VLog => "vlog",
            FileType::SSTable => "sst",
            FileType::Manifest => "manifest",
        }
    }
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
    pub fn create(&self, ty: FileType) -> Result<(File, String)> {
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

    pub fn remove<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        remove_file(path)?;

        Ok(())
    }

    pub fn open<P: AsRef<Path>>(&self, filename: P) -> Result<File> {
        Ok(File::with_options()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(filename)?)
    }

    /// Initialize / recover manager's state from manifest file.
    fn init() -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn init_file_manager() {
        let base_dir = tempdir().unwrap();

        let file_manager = FileManager::with_base_dir(base_dir.path()).unwrap();
        let _ = file_manager.create(FileType::Manifest).unwrap();
        assert_eq!(base_dir.path().read_dir().unwrap().count(), 1);
    }
}
