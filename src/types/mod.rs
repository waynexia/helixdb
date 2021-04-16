//! Wrapper over all generated types / structs. And implements
//! their utilities.
//!
//! `protos` dependency should only present in this mod.

mod entry;
mod level_info;
mod rick;

pub use entry::{Bytes, Entry, EntryMeta, ThreadId, TimeRange, Timestamp};
pub use level_info::{LevelDesc, LevelId, LevelInfo};
pub use rick::{Offset, RickSuperBlock};

// todo: maybe make a trait `Generated` or sth.
// contains `encode()`, `decode()`, `to_generated_type()`.
