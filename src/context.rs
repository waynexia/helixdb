use crate::file::FileManager;
use crate::fn_registry::FnRegistry;

pub struct Context {
    pub fn_registry: FnRegistry,
    pub(crate) file_manager: FileManager,
}
