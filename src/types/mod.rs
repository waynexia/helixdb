//! Wrapper over all generated types / structs. And implements
//! their utilities.
//!
//! `protos` dependency should only present in this mod.

mod entry;
mod level_info;
mod rick;
pub mod sstable;

pub use entry::{Bytes, BytesRef, Entry, EntryMeta, LevelId, ThreadId, TimeRange, Timestamp};
pub use level_info::{LevelDesc, LevelInfo};
pub(crate) use rick::{Offset, RickSuperBlock, ValueFormat};

// todo: maybe make a trait `Generated` or sth.
// contains `encode()`, `decode()`, `to_generated_type()`.
