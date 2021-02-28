use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;

use crate::context::Context;
use crate::entry::{Bytes, Timestamp};
use crate::error::Result;
use crate::table::TableIterator;

pub struct SSTable {
    file: File,
}

impl SSTable {
    pub fn metadata(&self) -> Result<TableMeta> {
        todo!()
    }

    /// Construct a iterator on SSTable.
    ///
    /// Todo: use trait in std::iter.
    ///
    /// Todo: avoid read all data?
    pub fn iterator<'ctx>(&mut self, ctx: &'ctx Context) -> Result<TableIterator<'ctx>> {
        let mut index = HashMap::new();
        const PREFIX_LENGTH: usize = 3 * size_of::<u64>();

        // get vlog filename
        self.file.seek(SeekFrom::End(size_of::<u64>() as i64))?;
        let mut filename_length_buf = Vec::with_capacity(size_of::<u64>());
        self.file.read_exact(&mut filename_length_buf)?;
        let filename_length = u64::from_le_bytes(filename_length_buf.try_into().unwrap());
        let pos = size_of::<u64>() as i64 + filename_length as i64;
        self.file.seek(SeekFrom::End(pos))?;
        let mut filename_buf = Vec::with_capacity(filename_length as usize);
        self.file.read_exact(&mut filename_buf)?;
        let vlog_filename = String::from_utf8(filename_buf).unwrap();

        // get data block size
        let mut read_length = 0;
        let data_block_size = self.file.seek(SeekFrom::End(0))? - pos as u64;
        let data_block_size = data_block_size as usize;
        self.file.seek(SeekFrom::Start(0))?;

        // make key-value index
        let mut raw_entry_positions = vec![];
        while read_length < data_block_size {
            let mut prefix_buf = vec![0; PREFIX_LENGTH];
            self.file.read_exact(&mut prefix_buf)?;
            let value_offset = u64::from_le_bytes(prefix_buf[0..PREFIX_LENGTH].try_into().unwrap());
            let value_size = u64::from_le_bytes(
                prefix_buf[PREFIX_LENGTH..PREFIX_LENGTH * 2]
                    .try_into()
                    .unwrap(),
            );
            let key_length = u64::from_le_bytes(
                prefix_buf[PREFIX_LENGTH * 2..PREFIX_LENGTH * 3]
                    .try_into()
                    .unwrap(),
            ) as usize;

            let mut key_buf = Vec::with_capacity(key_length);
            self.file.read_exact(&mut key_buf)?;
            read_length += PREFIX_LENGTH * 3 + key_length;

            index.insert(key_buf.clone(), raw_entry_positions.len());
            raw_entry_positions.push((key_buf, value_offset, value_size));
        }

        TableIterator::try_new(
            index,
            raw_entry_positions,
            self.metadata()?,
            vlog_filename,
            ctx,
        )
    }
}

impl From<File> for SSTable {
    fn from(file: File) -> Self {
        Self { file }
    }
}

pub struct TableBuilder {
    file: File,
    entry_buffer: Bytes,
}

// todo: need to load the whole sstable file to read?
impl TableBuilder {
    /// in-params: key, [(offset, size),]
    ///
    /// format: | value offset (u64) | value size (u64) | key size (u64) | key |
    pub fn add_entries(&mut self, keys: Vec<Bytes>, offsets: Vec<(u64, u64)>) {
        let mut entries = Vec::with_capacity(keys.len());
        for (mut key, (offset, size)) in keys.into_iter().zip(offsets.into_iter()) {
            let mut bytes = vec![];
            let key_size = key.len() as u64;
            bytes.append(&mut offset.to_le_bytes().to_vec());
            bytes.append(&mut size.to_le_bytes().to_vec());
            bytes.append(&mut key_size.to_le_bytes().to_vec());
            bytes.append(&mut key);

            entries.append(&mut bytes);
        }

        self.entry_buffer.append(&mut entries);
    }

    /// Consume this builder to build a SSTable.
    pub fn finish(mut self, vlog_filename: String) -> Result<()> {
        // write data block
        let data_block_size = self.entry_buffer.len();
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&self.entry_buffer)?;

        // vlog filename
        let filename_length = vlog_filename.len() as u64;
        self.file.write_all(&vlog_filename.into_bytes())?;
        self.file.write_all(&filename_length.to_le_bytes())?;

        // todo: other blocks

        Ok(())
    }
}

impl From<File> for TableBuilder {
    fn from(file: File) -> Self {
        Self {
            file,
            entry_buffer: vec![],
        }
    }
}

#[derive(Debug)]
pub struct TableMeta {
    start_timestamp: Timestamp,
    end_timestamp: Timestamp,
}

impl TableMeta {
    pub fn start(&self) -> Timestamp {
        self.start_timestamp
    }

    pub fn end(&self) -> Timestamp {
        self.end_timestamp
    }
}
