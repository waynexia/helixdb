use std::fs::{DirEntry, File};
use std::io::{Read, Seek, SeekFrom, Write};

use crate::entry::{Bytes, Entry, Timestamp};
use crate::error::Result;

pub struct IOWorker {
    curr_l0_file: File,
}

impl IOWorker {}
