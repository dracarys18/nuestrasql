use super::log_record::{LogOperation, RecordLog};
use crate::{
    storage::{disk::page::Page, log::manager::LogManager, tx},
    utils::safe_lock::SafeLock,
};

use std::sync::{Arc, Mutex};

pub struct Checkpoint {}

impl Checkpoint {
    pub fn new() -> Self {
        Self {}
    }

    pub fn write_to_log(lm: Arc<Mutex<LogManager>>) -> std::io::Result<u32> {
        let mut page = Page::new(4);

        page.set_int(0, LogOperation::Checkpoint as i32);
        lm.safe_lock().append(page.contents())
    }
}

impl RecordLog for Checkpoint {
    fn undo(&self, _tx: &mut tx::Transactions) -> crate::error::DbResult<()> {
        Ok(())
    }

    fn op(&self) -> LogOperation {
        LogOperation::Checkpoint
    }
    fn tx_number(&self) -> i32 {
        -1
    }
}

impl std::fmt::Display for Checkpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<CHECKPOINT>")
    }
}
