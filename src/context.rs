use crate::file::FileManager;
use crate::fn_registry::FnRegistry;

pub struct Context {
    crate file_manager: FileManager,
    pub fn_registry: FnRegistry,
}
