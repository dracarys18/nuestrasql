use crate::{
    bufferpool::pool::BufferPoolManager, disk::manager::Manager, error::DbResult,
    log::manager::LogManager, tx::Transactions,
};
use std::sync::{Arc, Mutex};

pub struct DBServer {
    pub file_manager: Arc<Manager>,
    pub log_manager: Arc<Mutex<LogManager>>,
    pub buffer_manager: Arc<Mutex<BufferPoolManager>>,
}

impl DBServer {
    pub fn new_with_params(options: DBServerOptions) -> std::io::Result<Self> {
        let fm = Arc::new(Manager::new(options.directory, options.block_size));
        let log_manager = Arc::new(Mutex::new(LogManager::new(
            fm.clone(),
            "wal.log".to_string(),
        )?));

        Ok(Self {
            file_manager: fm.clone(),
            log_manager: log_manager.clone(),
            buffer_manager: Arc::new(Mutex::new(BufferPoolManager::new(
                fm.clone(),
                log_manager.clone(),
                options.pool_size as u32,
            ))),
        })
    }
    pub fn log_manager(&self) -> Arc<Mutex<LogManager>> {
        self.log_manager.clone()
    }

    pub fn buffer_manager(&self) -> Arc<Mutex<BufferPoolManager>> {
        self.buffer_manager.clone()
    }

    pub fn file_manager(&self) -> Arc<Manager> {
        self.file_manager.clone()
    }

    pub fn new_tx(&self) -> DbResult<Transactions> {
        Transactions::new(
            self.file_manager.clone(),
            self.buffer_manager.clone(),
            self.log_manager.clone(),
        )
    }
}

#[derive(Default)]
pub struct DBServerOptions {
    directory: String,
    block_size: u64,
    pool_size: usize,
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

    pub fn pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = pool_size;
        self
    }
}
