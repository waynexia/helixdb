use std::fs::{DirEntry, File};
use std::io::{Read, Seek, SeekFrom, Write};

use crate::error::Result;
use crate::types::{Bytes, Entry, Timestamp};

pub struct IOWorker {
    curr_l0_file: File,
}

impl IOWorker {}
