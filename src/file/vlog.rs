use std::convert::TryFrom;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

// todo: use `bytes` crate.
use crate::error::{HelixError, Result};
use crate::types::Bytes;

#[derive(Clone)]
pub struct VLog {
    file: Arc<Mutex<File>>,
}

impl VLog {
    pub fn get(&self, offset: u64, length: u64) -> Result<Bytes> {
        let mut file = self.file.lock().unwrap();
        let mut buf = Vec::with_capacity(length as usize);
        buf.resize_with(length as usize, Default::default);
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buf)?;
        Ok(buf)
    }
}

impl From<File> for VLog {
    fn from(file: File) -> Self {
        Self {
            file: Arc::new(Mutex::new(file)),
        }
    }
}

pub struct ValueLogBuilder {
    file: File,
    file_size: u64,
}

impl ValueLogBuilder {
    /// return offset and length
    pub fn add_entry(&mut self, value: Bytes) -> Result<(u64, u64)> {
        let wrote_length = self.file.write(&value)? as u64;
        let prev_size = self.file_size;
        self.file_size += wrote_length;

        Ok((prev_size, wrote_length))
    }
}

impl TryFrom<File> for ValueLogBuilder {
    type Error = HelixError;

    fn try_from(mut file: File) -> Result<Self> {
        file.seek(SeekFrom::Start(0))?;
        Ok(Self { file, file_size: 0 })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use tempfile::tempfile;

    #[test]
    fn vlog_builder() {
        let vlog_file = tempfile().unwrap();
        let mut vlog_builder = ValueLogBuilder::try_from(vlog_file.try_clone().unwrap()).unwrap();

        let (offset1, size1) = vlog_builder.add_entry(b"some values".to_vec()).unwrap();
        let (offset2, size2) = vlog_builder.add_entry(b"some others".to_vec()).unwrap();
        drop(vlog_builder);

        let vlog = VLog::from(vlog_file);
        assert_eq!(vlog.get(offset1, size1).unwrap(), b"some values".to_vec());
        assert_eq!(vlog.get(offset2, size2).unwrap(), b"some others".to_vec());
    }
}
