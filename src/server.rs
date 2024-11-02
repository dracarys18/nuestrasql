use crate::{disk::manager::Manager, log::manager::LogManager};
use std::sync::{Arc, Mutex};

pub struct DBServer {
    pub file_manager: Arc<Manager>,
    pub log_manager: Arc<Mutex<LogManager>>,
}

impl DBServer {
    pub fn new_with_params(options: DBServerOptions) -> std::io::Result<Self> {
        let fm = Arc::new(Manager::new(options.directory, options.block_size));

        Ok(Self {
            file_manager: fm.clone(),
            log_manager: Arc::new(Mutex::new(LogManager::new(
                fm.clone(),
                "wal.log".to_string(),
            )?)),
        })
    }
    pub fn log_manager(&self) -> Arc<Mutex<LogManager>> {
        self.log_manager.clone()
    }

    pub fn file_manager(&self) -> Arc<Manager> {
        self.file_manager.clone()
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
