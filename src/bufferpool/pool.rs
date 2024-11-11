use crate::{
    disk::{block::Block, manager::Manager},
    log::manager::LogManager,
};
use std::{
    sync::{Arc, Mutex},
    thread,
};

use super::buffer::Buffer;

///
/// The Buffer manager allocates the fixed set of memory pages, called buffer pool.
/// In order to access any disk blocks, the client talks to the buffer manager
///
/// The general algorithm works this way
/// 1. The client asks the buffer manager to "pin" a page from the buffer pool to that block
/// 2. The client accesses the contents of the page as much as it desires
/// 3. When the client is done with the page, it tells the buffer manager to "unpin" it.
///
pub struct BufferPoolManager {
    bufferpool: Vec<Buffer>,
    available: u32,
}

impl BufferPoolManager {
    /// Time in milliseconds which the client can wait for max
    const MAX_TIME: u64 = 10000;

    pub fn new(fm: Arc<Manager>, lm: Arc<Mutex<LogManager>>, buffs: u32) -> Self {
        let buffers = (0..buffs)
            .map(|_| Buffer::new(fm.clone(), lm.clone()))
            .collect::<Vec<Buffer>>();

        Self {
            bufferpool: buffers,
            available: buffs,
        }
    }

    pub fn available(&self) -> u32 {
        self.available
    }

    pub fn get_buffer_mut(&mut self, index: usize) -> &mut Buffer {
        &mut self.bufferpool[index]
    }

    /// Flush all the buffers that is changed by the current txn
    pub fn flush_all(&mut self, txnum: i32) -> std::io::Result<()> {
        for buf in &mut self.bufferpool {
            if buf.modifying_tx() == txnum {
                buf.flush()?;
            }
        }
        Ok(())
    }

    ///
    /// Marks the buffer as unpinned, If the buffer is unpinned then,
    /// It unparks the current thread and allows the waiting clients to use the buffer
    ///
    pub fn unpin(&mut self, buff: usize) {
        let buff = self.get_buffer_mut(buff);
        buff.unpin();

        if !buff.is_pinned() {
            self.available += 1;
            thread::current().unpark();
        }
    }

    ///
    /// The client concurrently pins the current buffer, while it's pinned
    /// the client has the freedom read and modify the contents of the buffer.
    /// If all the buffers in the buffer pool are pinned the client has to wait
    /// until any of them becomes available. If the client has waited too long then
    /// buffer pool manager will return TimeOutError
    ///
    pub fn pin(&mut self, block: Block) -> std::io::Result<usize> {
        let current = std::time::Instant::now();
        let mut buffer = self.try_pin(block.clone());

        while buffer.is_none() && !self.waited_too_long(current) {
            thread::park_timeout(std::time::Duration::from_millis(Self::MAX_TIME));
            buffer = self.try_pin(block.clone());
        }

        buffer.ok_or(std::io::Error::other("Buffer timed out"))
    }

    pub fn find_unpinned(&self) -> Option<usize> {
        self.bufferpool.iter().position(|b| !b.is_pinned())
    }

    ///
    /// Tries to find the existing Buffer which has this block assigned,
    /// Or else finds the buffer which is not pinned yet.
    ///
    fn try_pin(&mut self, block: Block) -> Option<usize> {
        let buffer = self.find_existing(&block).or_else(|| {
            let buf = self.find_unpinned()?;
            self.bufferpool[buf].assign_to_block(block.clone()).ok()?;
            Some(buf)
        })?;

        if !self.bufferpool[buffer].is_pinned() {
            self.available -= 1;
        }

        self.bufferpool[buffer].pin();

        Some(buffer)
    }

    pub fn waited_too_long(&self, started_at: std::time::Instant) -> bool {
        started_at.elapsed() > std::time::Duration::from_millis(Self::MAX_TIME)
    }

    pub fn find_existing(&self, block: &Block) -> Option<usize> {
        self.bufferpool
            .iter()
            .position(|b| b.blocks().eq(&Some(block)))
    }
}
