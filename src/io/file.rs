use glommio::io::{DmaFile, DmaOpenOptions};
use glommio::ByteSliceMutExt;
use std::path::Path;

use crate::error::Result;
use crate::types::Bytes;

pub struct File(DmaFile);

// todo: check these. required by async trait `Iterator`
unsafe impl Send for File {}
unsafe impl Sync for File {}

impl File {
    /// Open or create on given path.
    #[inline]
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<File> {
        let file = DmaOpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .await?;

        Ok(File(file))
    }

    #[inline]
    pub async fn read(&self, offset: u64, size: u64) -> Result<Bytes> {
        let read_result = self.0.read_at(offset, size as usize).await?;

        // todo: remove this copy
        Ok(read_result.to_vec())
    }

    #[inline]
    pub async fn write(&self, bytes: Bytes, offset: u64) -> Result<()> {
        let mut buf = self.0.alloc_dma_buffer(bytes.len());
        buf.as_bytes_mut().write_at(0, &bytes);

        self.0.write_at(buf, offset).await?;

        Ok(())
    }

    #[inline]
    pub async fn sync(&self) -> Result<()> {
        self.0.fdatasync().await?;

        Ok(())
    }

    #[inline]
    pub async fn size(&self) -> Result<u64> {
        Ok(self.0.file_size().await?)
    }

    #[inline]
    pub async fn truncate(&self, size: u64) -> Result<()> {
        self.0.truncate(size).await?;

        Ok(())
    }
}
