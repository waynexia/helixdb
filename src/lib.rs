//! HelixDB is a KV-Engine designed for time-series data.

#![feature(with_options)]
#![feature(vec_into_raw_parts)]
#![feature(trait_alias)]
#![feature(new_uninit)]

mod blocks;
mod cache;
mod context;
mod db;
mod error;
mod file;
mod fn_registry;
mod index;
mod io;
mod io_worker;
mod iterator;
mod level;
mod table;
mod types;

pub use db::*;
