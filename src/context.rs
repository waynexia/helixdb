use crate::file::FileManager;
use crate::fn_registry::FnRegistry;

pub struct Context {
    pub file_manager: FileManager,
    pub fn_registry: FnRegistry,
}
