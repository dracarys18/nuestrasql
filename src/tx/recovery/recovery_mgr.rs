use super::log_record::{create_log_record, LogOperation};
use crate::{
    bufferpool::{buffer::Buffer, pool::BufferPoolManager},
    log::manager::LogManager,
    tx::recovery::{
        checkpoint::Checkpoint, commit_log::CommitLog, rollback::Rollback, set_int::SetIntRecord,
        set_string::SetStringRecord,
    },
    tx::Transactions,
    utils::safe_lock::SafeLock,
};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct RecoveryManager {
    lm: Arc<Mutex<LogManager>>,
    bm: Arc<Mutex<BufferPoolManager>>,
    tx: Option<Arc<Mutex<Transactions>>>,
    txnum: i32,
}

impl RecoveryManager {
    pub fn new(txnum: i32, lm: Arc<Mutex<LogManager>>, bm: Arc<Mutex<BufferPoolManager>>) -> Self {
        Self {
            tx: None,
            lm,
            bm,
            txnum,
        }
    }

    pub fn init_transaction(self, tx: Arc<Mutex<Transactions>>) -> Self {
        Self {
            tx: Some(tx.clone()),
            ..self
        }
    }

    pub fn commit(&mut self) -> std::io::Result<()> {
        self.bm.safe_lock().flush_all(self.txnum)?;

        let lsn = CommitLog::write_to_log(self.lm.clone(), self.txnum)?;

        self.lm.safe_lock().flush(lsn)?;
        Ok(())
    }

    pub fn rollback(&mut self) -> crate::error::DbResult<()> {
        self.do_rollback()?;

        self.bm.safe_lock().flush_all(self.txnum)?;
        let lsn = Rollback::write_to_log(self.lm.clone(), self.txnum)?;

        self.lm.safe_lock().flush(lsn)?;
        Ok(())
    }

    pub fn recover(&mut self) -> crate::error::DbResult<()> {
        self.do_recover()?;

        self.bm.safe_lock().flush_all(self.txnum)?;
        let lsn = Checkpoint::write_to_log(self.lm.clone())?;

        self.lm.safe_lock().flush(lsn)?;
        Ok(())
    }

    pub fn set_int(
        &mut self,
        buff: &mut Buffer,
        offset: u32,
        _new_val: i32,
    ) -> std::io::Result<u32> {
        let oldval = buff.contents().get_int(offset as usize);
        let block = buff
            .blocks()
            .ok_or(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Blocks not found",
            ))?
            .clone();

        SetIntRecord::write_to_log(self.lm.clone(), self.txnum, block, offset, oldval)
    }

    pub fn set_string(
        &mut self,
        buff: &mut Buffer,
        offset: u32,
        _new_val: &str,
    ) -> std::io::Result<u32> {
        let oldval = buff.contents().get_string(offset as usize);
        let block = buff
            .blocks()
            .ok_or(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Blocks not found",
            ))?
            .clone();

        SetStringRecord::write_to_log(self.lm.clone(), self.txnum, block, offset, oldval)
    }

    fn do_rollback(&mut self) -> crate::error::DbResult<()> {
        let iter = self.lm.safe_lock().iter()?;

        for bytes in iter {
            let rec = create_log_record(bytes);

            if rec.tx_number() == self.txnum {
                if rec.op().eq(&LogOperation::Start) {
                    return Ok(());
                }
                rec.undo(
                    &mut self
                        .tx
                        .as_ref()
                        .expect("Transactions was not initialized")
                        .safe_lock(),
                )?;
            }
        }

        Ok(())
    }

    fn do_recover(&mut self) -> crate::error::DbResult<()> {
        let mut finished_txs = Vec::<i32>::new();

        for byte in self.lm.safe_lock().iter()? {
            let rec = create_log_record(byte);

            if rec.op().eq(&LogOperation::Checkpoint) {
                return Ok(());
            }

            if rec.op().eq(&LogOperation::Commit) || rec.op().eq(&LogOperation::Rollback) {
                finished_txs.push(rec.tx_number());
            } else if finished_txs.contains(&rec.tx_number()) {
                rec.undo(
                    &mut self
                        .tx
                        .as_ref()
                        .expect("Transactions was not initialized")
                        .safe_lock(),
                )?;
            }
        }

        Ok(())
    }
}
