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
    #[error("common HelixDB error")]
    Common,
    #[error("element not found")]
    NotFound,
    #[error("task dropped")]
    Dropped(#[from] tokio::sync::oneshot::error::RecvError),
    #[error("helix stopped")]
    Stopped(#[from] tokio::sync::mpsc::error::SendError<Task>),
}
