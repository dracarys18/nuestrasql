use crate::{
    error::{DbError, DbResult},
    storage::disk::block::Block,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Maintains the Lock status for a Block, Granularity of the lock is on a single block
pub struct LockTable(HashMap<Block, i32>);

impl LockTable {
    /// Max time till you can keep a transaction waiting
    const MAX_TIME: u32 = 10000_u32;

    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Shared lock can be obtained by on or more transactions, usually for reading
    /// When you acquire the shared lock you cannot acquire exclusive lock and vise versa
    ///
    /// We first check if the block already has an exclusive lock if so we park the current
    /// thread till `MAX_TIME` milliseconds. If the block still has an exclusive lock then
    /// the `LockAborted` error is returned
    pub fn slock(&mut self, blk: &Block) -> DbResult<()> {
        let time = Instant::now();

        while self.has_x_lock(blk) && !Self::did_wait_too_long(time) {
            std::thread::park_timeout(Duration::from_millis(Self::MAX_TIME as u64));
        }

        if self.has_x_lock(blk) {
            return Err(DbError::LockAborted);
        }

        let lock = self.get_lock(blk);
        self.0.insert(blk.clone(), lock + 1);

        Ok(())
    }

    /// Exclusive lock can only be obtained by a single transaction, any transaction writing the
    /// data should acquire an exclusive lock before committing
    ///
    /// We first check if there's any Shared lock already for the current block if so we wait till
    /// `MAX_TIME` milliseconds, If the block is still ShareLocked then the `LockAborted` error is
    /// returned
    pub fn xlock(&mut self, blk: &Block) -> DbResult<()> {
        let time = Instant::now();

        while self.has_slock(blk) && !Self::did_wait_too_long(time) {
            std::thread::park_timeout(Duration::from_millis(Self::MAX_TIME as u64));
        }

        if self.has_slock(blk) {
            return Err(DbError::LockAborted);
        }

        self.0.insert(blk.clone(), -1);
        Ok(())
    }

    /// If the lock was a shared lock, Lock value will be decremented on the lock table
    /// if the transaction trying unlock is the last transaction holding the lock on the
    /// block then the waiting threads are notified to wake
    pub fn unlock(&mut self, blk: &Block) {
        let val = self.get_lock(blk);

        if val > 1 {
            self.0.insert(blk.clone(), val - 1);
        } else {
            self.0.remove(blk);
            std::thread::current().unpark();
        }
    }

    fn get_lock(&self, blk: &Block) -> i32 {
        *self.0.get(blk).unwrap_or(&0)
    }

    fn has_x_lock(&self, blk: &Block) -> bool {
        self.get_lock(blk) < 0
    }

    fn has_slock(&self, blk: &Block) -> bool {
        self.get_lock(blk) > 1
    }
    fn did_wait_too_long(start_time: Instant) -> bool {
        start_time.elapsed() > Duration::from_millis(Self::MAX_TIME as u64)
    }
}
