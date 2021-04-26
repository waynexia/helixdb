use std::io;

use thiserror::Error;

use crate::io_worker::Task;

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
    #[error("Helix stopped")]
    Stopped(#[from] tokio::sync::mpsc::error::SendError<Task>),
    #[error("Operation {0} is poisoned")]
    Poisoned(String),
}
