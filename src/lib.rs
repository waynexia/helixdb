//! HelixDB is a KV-Engine designed for time-series data.

#![feature(with_options)]
#![feature(vec_into_raw_parts)]
#![feature(trait_alias)]
#![feature(async_closure)]
#![feature(btree_retain)]
#![feature(new_uninit)]
// todo: open these lints
#![allow(dead_code)]
#![allow(unused_variables)]

/// Unwrap `Option` under the `Result<Option<T>>` return type requirement.
/// The `None` case will early return with `Ok(None)`.
///
/// # Example
/// *Notice this macro is not exported via "`#[macro_export]`" so the following
/// example will not be run as a test case.*
/// ```
/// # #![feature(never_type)]
/// # #[macro_use] extern crate helixdb;
/// fn return_ok_none() -> Result<Option<usize>, !> {
///     let val: Option<usize> = None;
///     ok_unwrap!(val);
///     panic!("should have returned");
/// }
///
/// # fn container() -> Result<Option<()>, !> {
/// let val = ok_unwrap!(Some(0usize));
/// assert_eq!(val, 0usize);
/// assert_eq!(return_ok_none(), Ok(None));
/// #    Ok(None)
/// # }
///
/// # let _ = container();
/// ```
macro_rules! ok_unwrap {
    ($e:expr) => {
        match $e {
            Some(thing) => thing,
            None => return Ok(None),
        }
    };
}

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
pub use util::{Comparator, LexicalComparator, NoOrderComparator};
