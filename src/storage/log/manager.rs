use crate::{
    consts::INTEGER_BYTES,
    storage::disk::{block::Block, manager::Manager, page::Page},
};
use std::sync::Arc;

pub struct LogManager {
    // File manager to read/write to file
    fm: Arc<Manager>,
    // A fixed permanent memory Page for the Logfile
    log_page: Page,
    // Block which points to last ever written block of data to Logfile
    block: Block,
    logfile: String,
    last_saved_seq: u32,
    latest_seq: u32,
}

impl LogManager {
    pub fn new(fm: Arc<Manager>, logfile: String) -> std::io::Result<Self> {
        let data = vec![0; fm.blocksize() as usize];

        let mut page = Page::new_with_data(data);

        let size = fm.size(&logfile)?;

        // If the log file is empty append a new block to LogFile otherwise get the last written
        // block to memory
        let block = if size == 0 {
            Self::append_new_block(&mut page, &fm, &logfile)?
        } else {
            let block = Block::new(logfile.clone(), size - 1);
            fm.read(&block, &mut page)?;
            block
        };

        Ok(Self {
            fm,
            block,
            log_page: page,
            logfile,
            last_saved_seq: 0,
            latest_seq: 0,
        })
    }

    /// Append a new block to the Logfile, This is to be used to append the block when the Logfile
    /// is empty
    pub fn append_new_block(page: &mut Page, fm: &Manager, file: &str) -> std::io::Result<Block> {
        let block = fm.append(file)?;

        page.set_int(0, fm.blocksize() as i32);
        fm.write(&block, page)?;
        Ok(block)
    }

    /// Appends the data to the current Page,if there is room otherwise,
    /// Flushes the current Page to disk and allocates a new block to the Page
    pub fn append(&mut self, data: &[u8]) -> std::io::Result<u32> {
        let mut plen = self.log_page.get_int(0);
        let data_size = data.len();

        // 4 bytes to store the length of the page and rest for the data
        let total_bytes_needed = (data_size + INTEGER_BYTES) as i32;

        if plen - total_bytes_needed < INTEGER_BYTES as i32 {
            self.flush_impl(self.latest_seq)?;
            self.block = Self::append_new_block(&mut self.log_page, &self.fm, &self.logfile)?;
            plen = self.log_page.get_int(0);
        }

        // Insert the data at the last
        let insert_pos = plen - total_bytes_needed;
        self.log_page.set_bytes(insert_pos as usize, data);
        self.log_page.set_int(0, insert_pos);

        self.latest_seq += 1;
        Ok(self.latest_seq)
    }

    /// Compare the provided SEQ with the last saved SEQ if it's smaller then write the `logpage to
    /// disk
    pub fn flush(&mut self, log_seq_no: u32) -> std::io::Result<()> {
        if log_seq_no >= self.last_saved_seq {
            self.flush_impl(log_seq_no)?;
        }
        Ok(())
    }

    pub fn iter(&mut self) -> std::io::Result<LogManagerIterator> {
        self.flush_impl(self.last_saved_seq)?;
        LogManagerIterator::new(self.fm.clone(), self.block.clone())
    }
    pub fn flush_impl(&mut self, log_seq_no: u32) -> std::io::Result<()> {
        self.fm.write(&self.block, &mut self.log_page)?;

        self.last_saved_seq = log_seq_no;
        Ok(())
    }
}

/// Log Iterator moves from the last block in reverse order
pub struct LogManagerIterator {
    fm: Arc<Manager>,
    current_pos: usize,
    boundary: usize,
    block: Block,
    page: Page,
}

impl LogManagerIterator {
    pub fn block_size(&self) -> u64 {
        self.fm.blocksize()
    }

    pub fn new(fm: Arc<Manager>, block: Block) -> std::io::Result<Self> {
        let mut this = Self {
            fm: fm.clone(),
            block: block.clone(),
            page: Page::new_with_data(vec![0; fm.blocksize() as usize]),
            boundary: 0,
            current_pos: 0,
        };

        this.move_to_block(&block)?;
        Ok(this)
    }

    fn move_to_block(&mut self, block: &Block) -> std::io::Result<()> {
        self.fm.read(block, &mut self.page)?;
        self.boundary = self.page.get_int(0) as usize;
        self.current_pos = self.boundary;
        Ok(())
    }
}

impl Iterator for LogManagerIterator {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        if !(self.current_pos < self.fm.blocksize() as usize || self.block.num() > 0) {
            return None;
        }

        if self.current_pos == self.fm.blocksize() as usize {
            let block = Block::new(self.block.filename().to_string(), self.block.num() - 1);
            self.block = block.clone();
            self.move_to_block(&block).ok()?;
        }
        let data = self.page.get_bytes(self.current_pos);
        self.current_pos += data.len() + INTEGER_BYTES;

        Some(data.to_vec())
    }
}
