mod test;

use super::locks::lock_table::LockTable;
use crate::{disk::block::Block, error::DbResult, utils::safe_lock::SafeLock};
use once_cell::sync::Lazy;
use std::{collections::HashMap, sync::Mutex};

/// A shared global lock table to be used by multiple concurrent Transactions
pub static LOCK_TABLE: Lazy<Mutex<LockTable>> = Lazy::new(|| Mutex::new(LockTable::new()));

#[derive(PartialEq, Clone)]
enum LockTypes {
    Shared,
    Exclusive,
}

impl std::fmt::Display for LockTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Shared => write!(f, "S"),
            Self::Exclusive => write!(f, "X"),
        }
    }
}

/// Every transaction will have a seperate concurrency manager
///
/// It tracks the kind of locks the transaction holds for a block
#[derive(Clone)]
pub struct ConcurrencyManager {
    locks: HashMap<Block, LockTypes>,
}

impl ConcurrencyManager {
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
        }
    }
    /// Tries to acquire Slock if there was no lock held by the
    /// transaction previously on the block
    pub fn slock(&mut self, block: &Block) -> DbResult<()> {
        if !self.locks.contains_key(block) {
            LOCK_TABLE.safe_lock().slock(block)?;
            self.locks.insert(block.clone(), LockTypes::Shared);
        }

        Ok(())
    }

    /// Tries to acquire XLock if there was no Xlock held
    /// previously by the transaction, And if there was Slock
    /// held by the transaction, it upgrades the lock to XLock
    pub fn xlock(&mut self, block: &Block) -> DbResult<()> {
        if !self.has_x_lock(block) {
            self.slock(block)?;
            LOCK_TABLE.safe_lock().xlock(block)?;
            self.locks.insert(block.clone(), LockTypes::Exclusive);
        }
        Ok(())
    }

    /// Release all the locks acquired by the transaction on a block
    pub fn release(&mut self) -> DbResult<()> {
        for blk in self.locks.keys() {
            LOCK_TABLE.safe_lock().unlock(blk);
        }

        self.locks.clear();
        Ok(())
    }

    fn has_x_lock(&self, block: &Block) -> bool {
        self.locks
            .get(block)
            .map(|l| l.eq(&LockTypes::Exclusive))
            .unwrap_or_default()
    }
}
