use crate::{
    bufferpool::pool::BufferPoolManager,
    disk::block::Block,
    error::{DbError, DbResult},
    utils::safe_lock::SafeLock,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// A tracker to keep track of pinned buffers of a transaction
#[derive(Clone)]
pub struct BufferList {
    buffers: HashMap<Block, usize>,
    pins: Vec<Block>,
    bgr: Arc<Mutex<BufferPoolManager>>,
}

impl BufferList {
    pub fn new(bgr: Arc<Mutex<BufferPoolManager>>) -> Self {
        Self {
            bgr,
            pins: Vec::new(),
            buffers: HashMap::new(),
        }
    }

    pub fn get_buffer(&self, blk: &Block) -> DbResult<usize> {
        self.buffers.get(blk).cloned().ok_or(DbError::InvalidValue)
    }

    /// Pin the buffer to a block and track it in the pinned buffers
    pub fn pin(&mut self, blk: &Block) -> DbResult<()> {
        let mut bm = self.bgr.safe_lock();
        let buffer = bm.pin(blk.clone())?;

        self.buffers.insert(blk.clone(), buffer);
        self.pins.push(blk.clone());
        Ok(())
    }

    /// Unpin the block from the buffer and remove it from the pinned buffer
    pub fn unpin(&mut self, blk: &Block) -> DbResult<()> {
        let mut bm = self.bgr.safe_lock();
        let buffer = self.get_buffer(blk)?;

        bm.unpin(buffer);
        self.pins.retain(|b| !b.eq(blk));
        self.buffers.remove(blk);

        Ok(())
    }

    /// Unpin all the buffers
    pub fn unpin_all(&mut self) -> DbResult<()> {
        let mut bm = self.bgr.safe_lock();

        for blk in &self.pins {
            let buffer = self.get_buffer(blk)?;
            bm.unpin(buffer);
        }
        self.buffers.clear();
        self.pins.clear();

        Ok(())
    }
}
