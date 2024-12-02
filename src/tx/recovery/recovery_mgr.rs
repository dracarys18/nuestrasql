use super::{
    log_record::{create_log_record, LogOperation},
    start_log::StartLog,
};
use crate::{
    bufferpool::{buffer::Buffer, pool::BufferPoolManager},
    error::DbResult,
    log::manager::LogManager,
    tx::recovery::{
        checkpoint::Checkpoint, commit_log::CommitLog, rollback::Rollback, set_int::SetIntRecord,
        set_string::SetStringRecord,
    },
    tx::Transactions,
    utils::safe_lock::SafeLock,
};
use std::sync::{Arc, Mutex};

/// Recovery manager uses transaction log records to recover the database to the previous state it was before a shutdown or a crash.
///
/// ## For the undo stage:
/// For each log record, if the current log record is a commit, then add it to the list of committed transactions. Otherwise, if it’s a rollback, then add it to the rolled-back transactions list.
/// Check the subsequent log for the commit and rollback, then add either commit or rollback.
///
/// ## For the redo stage:
/// If the operation is an update, then add it to the committed list and update the new value again in the database.
/// If a record was actually written to disk and there was no commit log for the same, then the recovery manager has to roll back a committed transaction.
/// This actually violates the Durability property of ACID, so the database has to first write the committed log and flush it to the disk, then update the record.
/// Each update log record contains the new and old value for the record. The new value is for redo and the old value is for undo.
///
/// Stage 1 of the recovery manager checks if all the operations done by the database were committed or not.
/// If it was not, it undoes the transactions. Stage 2 of the recovery manager redoes all the committed transactions,
/// since it cannot tell if the record was actually written or not. Stage 1 has to read the log from the last entry, and stage 2 has to read it from the beginning.
///
/// ## Undo-Only recovery:
/// The approach listed above is idempotent, meaning no matter how many times you run the change, it will result in the same behavior.
/// This is good, but it leads to higher levels of disk writes. To solve this, one idea is to flush the bytes to disk before writing the commit record in the log.
/// Now, the recovery algorithm is modified as follows:
/// 1. Flush the commit transaction’s modified buffers to disk.
/// 2. Write a commit record to the log.
/// 3. Flush the log page containing the commit record.
///
/// ## Redo-Only Recovery:
/// Redo-only recovery can be achieved by not flushing the data to disk unless it’s committed.
/// But to achieve this, the database should keep a buffer pinned for all the modifications a transaction is doing, which makes it a very risky choice.
///
/// ** This recovery manager uses undo only recovery **
///
#[derive(Clone)]
pub struct RecoveryManager {
    lm: Arc<Mutex<LogManager>>,
    bm: Arc<Mutex<BufferPoolManager>>,
    tx: Option<Arc<Mutex<Transactions>>>,
    txnum: i32,
}

impl RecoveryManager {
    /// Creates an object for recovery manager, txnum should be incremented
    /// before passing it here
    pub fn new(
        txnum: i32,
        lm: Arc<Mutex<LogManager>>,
        bm: Arc<Mutex<BufferPoolManager>>,
    ) -> DbResult<Self> {
        let rmr = Self {
            tx: None,
            lm: lm.clone(),
            bm,
            txnum,
        };
        StartLog::write_to_log(lm.clone(), txnum)?;
        Ok(rmr)
    }

    /// HACK: This had to be used since there's a cyclic dependency
    /// between Transactions and RecoveryManager
    pub fn init_transaction(self, tx: Arc<Mutex<Transactions>>) -> Self {
        Self {
            tx: Some(tx.clone()),
            ..self
        }
    }

    /// Flushes the current transaction to disk and writes the commit
    /// log to the log file
    pub fn commit(&mut self) -> std::io::Result<()> {
        self.bm.safe_lock().flush_all(self.txnum)?;

        let lsn = CommitLog::write_to_log(self.lm.clone(), self.txnum)?;

        self.lm.safe_lock().flush(lsn)?;
        Ok(())
    }

    /// Rolls back the data and flushes into the disk and flushes
    /// the data into the disk
    pub fn rollback(&mut self) -> crate::error::DbResult<()> {
        self.do_rollback()?;

        self.bm.safe_lock().flush_all(self.txnum)?;
        let lsn = Rollback::write_to_log(self.lm.clone(), self.txnum)?;

        self.lm.safe_lock().flush(lsn)?;
        Ok(())
    }

    /// This follows undo-only recovery i.e it does undo on
    /// Transactions which is not commited/rollbacked before the database
    /// crash or exited
    pub fn recover(&mut self) -> crate::error::DbResult<()> {
        self.do_recover()?;

        self.bm.safe_lock().flush_all(self.txnum)?;
        let lsn = Checkpoint::write_to_log(self.lm.clone())?;

        self.lm.safe_lock().flush(lsn)?;
        Ok(())
    }

    /// Sets the log record for a set_int operation with old value
    /// written to the log file for recovery
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

    /// Sets the log record for a set_string operation with old value
    /// written to the log file for recovery
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

    /// Iterates over the log file with pointer at the end and moving
    /// towards the beginning of the file. All the transactions are logged with
    /// `Start` marker so the iterator keeps iterating until the marker is found
    /// and undoes whatever that was done
    fn do_rollback(&mut self) -> crate::error::DbResult<()> {
        let iter = self.lm.safe_lock().iter()?;
        let block_size = iter.block_size();

        for mut bytes in iter {
            bytes.resize(block_size as usize, 0);

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

    /// Iterates over the log file with pointer at the end and moving
    /// towards the beginning of the file. All the recovery operation is marked
    /// with `Checkpoint` marker so once the checkpoint is found it comes out of the loop
    ///
    /// If the Transactions are not committed or rolled back it reverses the change
    fn do_recover(&mut self) -> crate::error::DbResult<()> {
        let mut finished_txs = Vec::<i32>::new();
        let iter = self.lm.safe_lock().iter()?;
        let block_size = iter.block_size();

        for mut byte in iter {
            byte.resize(block_size as usize, 0);

            let rec = create_log_record(byte);

            if rec.op().eq(&LogOperation::Checkpoint) {
                return Ok(());
            }

            if rec.op().eq(&LogOperation::Commit) || rec.op().eq(&LogOperation::Rollback) {
                finished_txs.push(rec.tx_number());
            } else if !finished_txs.contains(&rec.tx_number()) {
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
