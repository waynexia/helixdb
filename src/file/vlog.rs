use std::convert::TryFrom;
use std::sync::Arc;

// todo: use `bytes` crate.
use crate::error::{HelixError, Result};
use crate::io::File;
use crate::types::Bytes;

#[derive(Clone)]
pub struct VLog {
    file: Arc<File>,
}

impl VLog {
    pub async fn get(&self, offset: u64, length: u64) -> Result<Bytes> {
        let buf = self.file.read(offset, length).await?;
        Ok(buf)
    }
}

impl From<File> for VLog {
    fn from(file: File) -> Self {
        Self {
            file: Arc::new(file),
        }
    }
}

pub struct ValueLogBuilder {
    file: File,
    file_size: u64,
}

impl ValueLogBuilder {
    /// return offset and length
    pub async fn add_entry(&mut self, value: Bytes) -> Result<(u64, u64)> {
        let wrote_length = value.len() as u64;
        let file_size = self.file.size().await?;
        self.file.write(value, file_size).await?;
        let prev_size = self.file_size;
        self.file_size += wrote_length;

        Ok((prev_size, wrote_length))
    }
}

impl TryFrom<File> for ValueLogBuilder {
    type Error = HelixError;

    fn try_from(file: File) -> Result<Self> {
        Ok(Self { file, file_size: 0 })
    }
}

#[cfg(test)]
mod test {
    use glommio::LocalExecutor;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn vlog_builder() {
        let ex = LocalExecutor::default();
        ex.run(async {
            let temp_dir = tempdir().unwrap();
            let vlog_file_path = temp_dir.path().join("vlog_file");
            let vlog_file = File::open(vlog_file_path.clone()).await.unwrap();
            let mut vlog_builder = ValueLogBuilder::try_from(vlog_file).unwrap();

            let (offset1, size1) = vlog_builder
                .add_entry(b"some values".to_vec())
                .await
                .unwrap();
            let (offset2, size2) = vlog_builder
                .add_entry(b"some others".to_vec())
                .await
                .unwrap();
            drop(vlog_builder);

            let vlog_file = File::open(vlog_file_path).await.unwrap();
            let vlog = VLog::from(vlog_file);
            assert_eq!(
                vlog.get(offset1, size1).await.unwrap(),
                b"some values".to_vec()
            );
            assert_eq!(
                vlog.get(offset2, size2).await.unwrap(),
                b"some others".to_vec()
            );
        });
    }
}
