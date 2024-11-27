use crate::{
    disk::{block::Block, page::Page},
    error::DbResult,
    log::manager::LogManager,
    tx::Transactions,
    utils::safe_lock::SafeLock,
};
use std::sync::{Arc, Mutex};

use super::log_record::{LogOperation, RecordLog};

/// Saves the log in the write-ahead-log in the following format
///
/// SETSTRING [TRANSACTION NUM] [FILE NAME] [BLOCK NUM] [OFFSET] [VALUE]
///
/// - Operation: Operation that has been performed by the database like SetInt, SetString
/// - Transaction number: A unique number given to the transaction
/// - File name: Name of the file database just changed
/// - Block number: If the file is divided into N equally sized blocks, the block database modified
/// - Offset: Offset from the start of the block which database changed
/// - Value: The value being written to log
pub struct SetStringRecord {
    txnum: i32,
    val: String,
    block: Block,
    offset: u32,
}

impl SetStringRecord {
    pub fn new(mut p: Page) -> Self {
        let tpos = 4;
        let txnum = p.get_int(tpos);
        let fpos = tpos + 4;

        let file_name = p.get_string(fpos);
        let bpos = fpos + Page::max_len(file_name.len());
        let blk_num = p.get_int(bpos);

        let opos = bpos + 4;
        let offset = p.get_int(opos);
        let vpos = opos + 4;
        let val = p.get_string(vpos);

        Self {
            txnum,
            block: Block::new(file_name, blk_num as u64),
            val,
            offset: offset as u32,
        }
    }

    pub fn write_to_log(
        lm: Arc<Mutex<LogManager>>,
        tx_num: i32,
        block: Block,
        offset: u32,
        val: String,
    ) -> std::io::Result<u32> {
        let trpos = 4;
        let fpos = trpos + 4;
        let bpos = fpos + Page::max_len(block.filename().len());
        let opos = bpos + 4;
        let vpos = opos + 4;

        let reclen = vpos + Page::max_len(val.len());

        let mut page = Page::new(reclen as u64);

        page.set_int(0, LogOperation::SetString as i32);
        page.set_int(trpos, tx_num);
        page.set_string(fpos, block.filename().to_string());
        page.set_int(bpos, block.num() as i32);
        page.set_int(opos, offset as i32);
        page.set_string(vpos, val);

        lm.safe_lock().append(page.contents())
    }
}

impl RecordLog for SetStringRecord {
    fn op(&self) -> LogOperation {
        LogOperation::SetString
    }

    fn tx_number(&self) -> i32 {
        self.txnum
    }

    fn undo(&self, tx: &mut Transactions) -> DbResult<()> {
        tx.pin(&self.block)?;
        tx.set_string(&self.block, self.offset, self.val.clone(), false)?;
        tx.unpin(&self.block)?;
        Ok(())
    }
}

impl std::fmt::Display for SetStringRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<SETSTRING {} {} {} {} >",
            self.txnum, self.block, self.offset, self.val
        )
    }
}
