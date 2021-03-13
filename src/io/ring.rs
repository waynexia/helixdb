use io_uring::opcode::{Read, Write};
use io_uring::types::Fixed;
use io_uring::IoUring;
use std::collections::HashMap;
use std::os::unix::prelude::RawFd;
use std::sync::Mutex;

use super::context::RingContext;
use super::task::RingFuture;
use crate::error::Result;

/// IoUring interface.
pub struct Ring {
    io_uring: IoUring,
    fd_cache: FdCache,
}

impl Ring {
    pub fn try_new(entries: u32) -> Result<Self> {
        let io_uring = IoUring::builder().build(entries)?;

        Ok(Self {
            io_uring,
            fd_cache: FdCache::default(),
        })
    }

    pub fn read(
        &self,
        fd: RawFd,
        offset: i64,
        buf: &mut [u8],
        _ctx: &RingContext,
    ) -> RingFuture<Result<()>> {
        let fd_index = self.fd_cache.get_or_register(fd);
        let read_entry = Read::new(Fixed(fd_index), buf.as_mut_ptr(), buf.len() as u32)
            .offset(offset)
            .build();

        // unsafe {
        //     self.io_uring.submission().push(&read_entry);
        // }

        todo!()
    }
}

/// Refer to io-uring::`Submitter`'s `register_files()` and `unregister_files()`
#[derive(Default)]
struct FdCache {
    /// [RawFd] to IoUring internal fd cache's index.
    cache: Mutex<HashMap<RawFd, u32>>,
    /// Vacancy caused by unregistering files.
    vacancies: Vec<u32>,
    max_index: u32,
}

impl FdCache {
    fn get_or_register(&self, fd: RawFd) -> u32 {
        todo!()
    }

    fn unregister(&mut self, fd: RawFd) {
        todo!()
    }
}
