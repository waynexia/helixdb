//! HelixDB is a KV-Engine designed for time-series data.

#![feature(with_options)]
#![feature(vec_into_raw_parts)]
#![feature(trait_alias)]
#![feature(async_closure)]
#![feature(btree_retain)]
#![feature(new_uninit)]

#[deprecated]
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
pub mod iterator;
mod level;
pub mod option;
mod table;
mod types;
mod util;

pub use db::*;
pub use fn_registry::FnRegistry;
pub use level::{SimpleTimestampReviewer, TimestampAction, TimestampReviewer};
pub use types::{Entry, TimeRange};
pub use util::{Comparator, NoOrderComparator};
