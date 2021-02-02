//! HelixDB is a KV-Engine designed for time-series data.

#![feature(vec_into_raw_parts)]

mod db;
mod entry;
mod error;
mod file_manager;
mod index;
mod io_worker;
mod level;
mod rick;

pub use db::*;
