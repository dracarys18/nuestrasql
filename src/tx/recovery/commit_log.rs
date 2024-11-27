use super::log_record::{LogOperation, RecordLog};
use crate::{disk::page::Page, log::manager::LogManager, utils::safe_lock::SafeLock};

use std::sync::{Arc, Mutex};

pub struct CommitLog {
    txnum: i32,
}

impl CommitLog {
    pub fn new(mut page: Page) -> Self {
        let tpos = 4;
        Self {
            txnum: page.get_int(tpos),
        }
    }

    pub fn write_to_log(lm: Arc<Mutex<LogManager>>, tx_num: i32) -> std::io::Result<u32> {
        let mut page = Page::new(2 * 4);

        page.set_int(0, LogOperation::Commit as i32);
        page.set_int(4, tx_num);

        lm.safe_lock().append(page.contents())
    }
}

impl RecordLog for CommitLog {
    fn undo(&self, _tx: &mut crate::tx::Transactions) -> crate::error::DbResult<()> {
        Ok(())
    }

    fn op(&self) -> LogOperation {
        LogOperation::Commit
    }
    fn tx_number(&self) -> i32 {
        self.txnum
    }
}

impl std::fmt::Display for CommitLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<COMMIT {} >", self.txnum)
    }
}
