use super::log_record::{LogOperation, RecordLog};
use crate::{
    disk::{block::Block, page::Page},
    log::manager::LogManager,
    utils::safe_lock::SafeLock,
};
use std::sync::{Arc, Mutex};

pub struct SetIntRecord {
    offset: u32,
    val: i32,
    txnum: i32,
    block: Block,
}

impl SetIntRecord {
    pub fn new(mut p: Page) -> Self {
        let txnum_pos = 4;
        let txnum = p.get_int(txnum_pos);
        let file_name_pos = txnum_pos + 4;

        let file_name = p.get_string(file_name_pos);
        let blk_num_pos = file_name_pos + Page::max_len(file_name.len());
        let blk_num = p.get_int(blk_num_pos);

        let offset_pos = blk_num_pos + 4;
        let offset = p.get_int(offset_pos);
        let value_pos = offset_pos + 4;
        let val = p.get_int(value_pos);

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
        val: i32,
    ) -> std::io::Result<u32> {
        let txnum_pos = 4;
        let filename_pos = txnum_pos + 4;
        let blknum_pos = filename_pos + Page::max_len(block.filename().len());
        let offset_pos = blknum_pos + 4;
        let value_pos = offset_pos + 4;

        let mut page = Page::new((value_pos + 4) as u64);

        page.set_int(0, LogOperation::SetInt as i32);
        page.set_int(txnum_pos, tx_num);
        page.set_string(filename_pos, block.filename().to_string());
        page.set_int(blknum_pos, block.num() as i32);
        page.set_int(offset_pos, offset as i32);
        page.set_int(value_pos, val);

        lm.safe_lock().append(page.contents())
    }
}

impl RecordLog for SetIntRecord {
    fn tx_number(&self) -> i32 {
        self.txnum
    }
    fn op(&self) -> LogOperation {
        LogOperation::SetInt
    }
    fn undo(&self, tx: &mut crate::tx::Transactions) -> crate::error::DbResult<()> {
        tx.pin(&self.block)?;
        tx.set_int(&self.block, self.offset, self.val, false)?;
        tx.unpin(&self.block)?;
        Ok(())
    }
}

impl std::fmt::Display for SetIntRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<SETINT {} {} {} {} >",
            self.txnum, self.block, self.offset, self.val
        )
    }
}
