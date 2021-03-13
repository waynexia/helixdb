use std::io;
use thiserror::Error;

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
}
