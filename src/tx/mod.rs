mod bufferlist;
mod concurrency;
mod locks;
mod recovery;
mod test;

use crate::{
    bufferpool::pool::BufferPoolManager,
    disk::{block::Block, manager::Manager},
    error::{DbError, DbResult},
    log::manager::LogManager,
    tx::recovery::recovery_mgr::RecoveryManager,
    utils::safe_lock::SafeLock,
};
use std::sync::{atomic::AtomicU32, Arc, Mutex};

static END_OF_FILE: u64 = u64::MAX;

static NEXT_TRANSACTION_ID: AtomicU32 = AtomicU32::new(0);

fn next_transaction_id() -> u32 {
    NEXT_TRANSACTION_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

#[derive(Clone)]
pub struct Transactions {
    file_mgr: Arc<Manager>,
    concurrency: concurrency::ConcurrencyManager,
    bm: Arc<Mutex<BufferPoolManager>>,
    txnum: u32,
    buffer: bufferlist::BufferList,
    recovery_mgr: RecoveryManager,
}

impl Transactions {
    pub fn new(
        fm: Arc<Manager>,
        bm: Arc<Mutex<BufferPoolManager>>,
        lm: Arc<Mutex<LogManager>>,
    ) -> DbResult<Self> {
        let txnum = next_transaction_id();

        let mut txn = Self {
            file_mgr: fm.clone(),
            concurrency: concurrency::ConcurrencyManager::new(),
            bm: bm.clone(),
            txnum,
            recovery_mgr: RecoveryManager::new(txnum as i32, lm.clone(), bm.clone())?,
            buffer: bufferlist::BufferList::new(bm.clone()),
        };

        txn.init();
        Ok(txn)
    }

    pub fn init(&mut self) {
        let txn = Arc::new(Mutex::new(self.clone()));

        let recovery_mgr = self.recovery_mgr.clone();

        self.recovery_mgr = recovery_mgr.clone().init_transaction(txn.clone());
    }

    pub fn txnum(&self) -> u32 {
        self.txnum
    }

    pub fn pin(&mut self, block: &Block) -> DbResult<()> {
        self.buffer.pin(block)
    }

    pub fn unpin(&mut self, block: &Block) -> DbResult<()> {
        self.buffer.unpin(block)
    }

    pub fn commit(&mut self) -> DbResult<()> {
        self.recovery_mgr.commit()?;

        println!("transaction {} committed", self.txnum);
        self.concurrency.release()?;

        self.buffer.unpin_all()?;

        Ok(())
    }

    pub fn rollback(&mut self) -> DbResult<()> {
        self.recovery_mgr.rollback()?;

        println!("transaction {} rolled back", self.txnum);
        self.concurrency.release()?;

        self.buffer.unpin_all()?;

        Ok(())
    }

    pub fn recover(&mut self) -> DbResult<()> {
        self.bm.safe_lock().flush_all(self.txnum as i32)?;
        self.recovery_mgr.recover()?;

        Ok(())
    }

    pub fn get_int(&mut self, block: &Block, offset: u32) -> DbResult<i32> {
        self.concurrency.slock(block)?;

        let buffer = self.buffer.get_buffer(block)?;
        let mut bm = self.bm.safe_lock();
        let buffer = bm.get_buffer_mut(buffer);

        Ok(buffer.contents().get_int(offset as usize))
    }

    pub fn get_string(&mut self, block: &Block, offset: u32) -> DbResult<String> {
        self.concurrency.slock(block)?;

        let buffer = self.buffer.get_buffer(block)?;
        let mut bm = self.bm.safe_lock();
        let buffer = bm.get_buffer_mut(buffer);

        Ok(buffer.contents().get_string(offset as usize))
    }

    pub fn set_string(
        &mut self,
        block: &Block,
        offset: u32,
        val: String,
        ok_to_log: bool,
    ) -> DbResult<()> {
        self.concurrency.xlock(block)?;

        let buffer = self.buffer.get_buffer(block)?;
        let mut bm = self.bm.safe_lock();

        let buffer = bm.get_buffer_mut(buffer);

        let mut lsn: i32 = -1;

        if ok_to_log {
            lsn = self.recovery_mgr.set_string(buffer, offset, &val)? as i32;
        }

        let page = buffer.contents();
        page.set_string(offset as usize, val);
        buffer.set_modified(self.txnum as i32, lsn);
        Ok(())
    }
    pub fn set_int(
        &mut self,
        block: &Block,
        offset: u32,
        val: i32,
        ok_to_log: bool,
    ) -> DbResult<()> {
        self.concurrency.xlock(block)?;

        let buffer = self.buffer.get_buffer(block)?;
        let mut bm = self.bm.safe_lock();

        let buffer = bm.get_buffer_mut(buffer);

        let mut lsn = -1;

        if ok_to_log {
            lsn = self.recovery_mgr.set_int(buffer, offset, val)? as i32;
        }

        let page = buffer.contents();
        page.set_int(offset as usize, val);
        buffer.set_modified(self.txnum as i32, lsn);

        Ok(())
    }

    pub fn append(&mut self, filename: String) -> DbResult<Block> {
        let blk = Block::new(filename.clone(), END_OF_FILE);
        self.concurrency.xlock(&blk)?;

        self.file_mgr.append(&filename).map_err(DbError::IoError)
    }

    pub fn blocksize(&self) -> u64 {
        self.file_mgr.blocksize()
    }
}
