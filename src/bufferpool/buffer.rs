use crate::{
    disk::{block::Block, manager::Manager, page::Page},
    log::manager::LogManager,
    utils::safe_lock::SafeLock,
};
use std::sync::{Arc, Mutex};

/// Contents of a single buffer in a buffer pool
pub struct Buffer {
    file_manager: Arc<Manager>,
    log_manager: Arc<Mutex<LogManager>>,
    contents: Page,

    /// If a buffer is assigned to a block this will be Some(block) otherwise None
    block: Option<Block>,
    /// Number of times this buffer has been pinned,
    /// This will help to determine how frequently it's been used
    pins: u64,
    /// Unique id for a single transaction that happens on the disk
    txnum: i32,
    /// LSN of the log that has been written for a particular
    /// transaction that happens on the buffer
    lsn: i32,
}

/// Operate function f over the block, if the block is some execute
/// the function on the block otherwise return an error
fn operate_block(
    block: Option<&Block>,
    mut f: impl FnMut(&Block) -> std::io::Result<()>,
) -> std::io::Result<()> {
    if let Some(block) = block {
        f(block)
    } else {
        Err(std::io::Error::other(
            "Cannot operate on block since it's null",
        ))
    }
}

impl Buffer {
    pub fn new(file_manager: Arc<Manager>, log_manager: Arc<Mutex<LogManager>>) -> Self {
        Self {
            file_manager: file_manager.clone(),
            log_manager,
            contents: Page::new(file_manager.blocksize()),
            block: None,
            pins: 0,
            txnum: -1,
            lsn: -1,
        }
    }

    pub fn contents(&mut self) -> &mut Page {
        &mut self.contents
    }

    pub fn blocks(&self) -> Option<&Block> {
        self.block.as_ref()
    }

    /// Sets the txnum to the supplied one, and if the LSN is less than zero that means
    /// logs were pushed for the buffer modification, so keep it unchanged
    pub fn set_modified(&mut self, txnum: i32, lsn: i32) {
        self.txnum = txnum;

        if lsn >= 0 {
            self.lsn = lsn;
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_tx(&self) -> i32 {
        self.txnum
    }

    pub fn assign_to_block(&mut self, block: Block) -> std::io::Result<()> {
        self.flush()?;
        self.block = Some(block);

        let block_clone = self.block.clone();

        operate_block(block_clone.as_ref(), |b| {
            self.file_manager.read(&b.clone(), &mut self.contents)
        })?;

        self.pins = 0;
        Ok(())
    }

    /// Flush only if there are any modifications in the buffer.
    /// i.e Checks if the txnum is not negative and if it's not flushes the
    /// Page into disk and resets the txnum to -1
    pub fn flush(&mut self) -> std::io::Result<()> {
        if self.txnum >= 0 {
            self.log_manager.safe_lock().flush(self.lsn as u32)?;

            let block_clone = self.block.clone();

            operate_block(block_clone.as_ref(), |b| {
                self.file_manager.write(&b.clone(), &mut self.contents)
            })?;

            self.txnum = -1;
        }
        Ok(())
    }

    pub fn pin(&mut self) {
        self.pins += 1;
    }

    pub fn unpin(&mut self) {
        self.pins -= 1;
    }
}
