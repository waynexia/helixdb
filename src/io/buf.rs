//! Sites structs interact with OS.

use std::sync::Arc;

/// When submitting a task to io_uring, the buffer provides to OS should
/// always be valid. So it will be leaked temporary to avoid future is cancelled
/// before completion.
// todo: consider Box::leak.
pub struct IoBuf {
    buf: Arc<Vec<u8>>,
}

impl IoBuf {
    /// # Safety
    /// This should be called exactly once.
    pub unsafe fn on_submit(mut self) -> Self {
        let ptr = Arc::into_raw(self.buf);
        Arc::increment_strong_count(ptr);
        self.buf = Arc::from_raw(ptr);
        self
    }

    /// # Safety
    /// This should be called exactly once.
    pub unsafe fn on_complet(mut self) -> Self {
        let ptr = Arc::into_raw(self.buf);
        Arc::decrement_strong_count(ptr);
        self.buf = Arc::from_raw(ptr);
        self
    }
}

impl AsRef<[u8]> for IoBuf {
    fn as_ref(&self) -> &[u8] {
        &self.buf
    }
}

impl AsMut<[u8]> for IoBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        Arc::get_mut(&mut self.buf).unwrap()
    }
}
