use crate::disk::manager::Manager;
use std::sync::Arc;

pub struct DBServer {
    pub file_manager: Arc<Manager>,
}

impl DBServer {
    pub fn new_with_params(options: DBServerOptions) -> Self {
        Self {
            file_manager: Arc::new(Manager::new(options.directory, options.block_size)),
        }
    }
}

#[derive(Default)]
pub struct DBServerOptions {
    directory: String,
    block_size: u64,
}

impl DBServerOptions {
    pub fn directory(mut self, dir: String) -> Self {
        self.directory = dir;
        self
    }

    pub fn block_size(mut self, block_size: u64) -> Self {
        self.block_size = block_size;
        self
    }
}
