use crate::{
    consts::INTEGER_BYTES, storage::disk::page::Page, storage::log::manager::LogManager,
    storage::tx::Transactions, utils::safe_lock::SafeLock,
};

use super::log_record::{LogOperation, RecordLog};

use std::sync::{Arc, Mutex};

pub struct Rollback {
    txnum: i32,
}

impl Rollback {
    pub fn new(mut page: Page) -> Self {
        Self {
            txnum: page.get_int(INTEGER_BYTES),
        }
    }

    pub fn write_to_log(lm: Arc<Mutex<LogManager>>, tx_num: i32) -> std::io::Result<u32> {
        let mut page = Page::new(2 * INTEGER_BYTES as u64);
        page.set_int(0, LogOperation::Rollback as i32);
        page.set_int(INTEGER_BYTES, tx_num);

        lm.safe_lock().append(page.contents())
    }
}

impl RecordLog for Rollback {
    fn op(&self) -> LogOperation {
        LogOperation::Rollback
    }

    fn tx_number(&self) -> i32 {
        self.txnum
    }

    fn undo(&self, _tx: &mut Transactions) -> crate::error::DbResult<()> {
        Ok(())
    }
}

impl std::fmt::Display for Rollback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<ROLLBACK {} >", self.txnum)
    }
}
