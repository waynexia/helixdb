use glommio::io::DmaFile;
use glommio::ByteSliceMutExt;
use std::path::Path;

use crate::error::Result;
use crate::types::Bytes;

pub struct File(DmaFile);

use std::ops::Deref;

impl File {
    /// Open or create on given path.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<File> {
        Ok(File(DmaFile::open(path).await?))
    }

    pub async fn read(&self, offset: u64, size: u64) -> Result<Bytes> {
        let read_result = self.0.read_at(offset, size as usize).await?;

        Ok(read_result.to_vec())
    }

    pub async fn write(&self, bytes: Bytes, offset: u64) -> Result<()> {
        let mut buf = self.0.alloc_dma_buffer(bytes.len());
        buf.as_bytes_mut().write_at(0, &bytes);

        self.0.write_at(buf, offset).await?;

        Ok(())
    }

    pub async fn sync(&self) -> Result<()> {
        self.0.fdatasync().await?;

        Ok(())
    }
}
