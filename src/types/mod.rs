//! Wrapper over all generated types / structs. And implements
//! their utilities.
//!
//! `protos` dependency should only present in this mod.

mod entry;
mod level_info;

pub use entry::{Bytes, Entry, EntryMeta, ThreadId, Timestamp};
pub use level_info::{LevelDesc, LevelId, LevelInfo};

// todo: maybe make a trait `Generated` or sth.
// contains `encode()`, `decode()`, `to_generated_type()`.
