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
        let tpos = 4;
        let txnum = p.get_int(tpos);
        let fpos = tpos + 4;

        let file_name = p.get_string(fpos);
        let bpos = fpos + Page::max_len(file_name.len());
        let blk_num = p.get_int(bpos);

        let opos = bpos + 4;
        let offset = p.get_int(opos);
        let vpos = opos + 4;
        let val = p.get_int(vpos);

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
        let trpos = 4;
        let fpos = trpos + 4;
        let bpos = fpos + Page::max_len(block.filename().len());
        let opos = bpos + 4;
        let vpos = opos + 4;

        let mut page = Page::new((vpos + 4) as u64);

        page.set_int(0, LogOperation::SetInt as i32);
        page.set_int(trpos, tx_num);
        page.set_string(fpos, block.filename().to_string());
        page.set_int(bpos, block.num() as i32);
        page.set_int(opos, offset as i32);
        page.set_int(vpos, val);

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
