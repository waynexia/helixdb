//! HelixDB is a KV-Engine designed for time-series data.

#![feature(with_options)]
#![feature(vec_into_raw_parts)]
#![feature(new_uninit)]

mod context;
mod db;
mod error;
mod file;
mod fn_registry;
mod index;
mod io_worker;
mod iterator;
mod level;
mod table;
mod types;

pub use db::*;
