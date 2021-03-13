use std::fs::{DirEntry, File};
use std::io::{Read, Seek, SeekFrom, Write};

use crate::error::Result;
use crate::level::Levels;
use crate::types::{Bytes, Entry, Timestamp};

pub struct IOWorker {
    levels: Levels,
}

impl IOWorker {}
