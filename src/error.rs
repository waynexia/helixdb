use std::io;

use thiserror::Error;

use crate::io_worker::Task;
use crate::types::Entry;

pub type Result<T> = std::result::Result<T, HelixError>;

#[derive(Error, Debug)]
pub enum HelixError {
    #[error("IO error {0}")]
    IO(#[from] io::Error),
    #[error("Glommio error {0}")]
    Glommio(#[from] glommio::GlommioError<()>),
    #[error("Common HelixDB error")]
    Common,
    #[error("Element not found")]
    NotFound,
    #[error("Task dropped")]
    Dropped(#[from] tokio::sync::oneshot::error::RecvError),
    #[error("Failed to send due to Helix is stopped")]
    Stopped(#[from] tokio::sync::mpsc::error::SendError<Task>),
    #[error("Operation {0} is poisoned")]
    Poisoned(String),
    // todo: review this usage.
    #[error("Internal channel disconnected")]
    Disconnected(#[from] tokio::sync::mpsc::error::SendError<Vec<Entry>>),
    #[error("Incompatible length or size, expect {0}, got {1}")]
    IncompatibleLength(usize, usize),
    #[error("Helix is closed")]
    Closed,
}
