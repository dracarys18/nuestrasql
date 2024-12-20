use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
    sync::{atomic::AtomicU64, Arc},
};

use super::{block::Block, page::Page};

pub struct Manager {
    directory: String,
    blocksize: u64,
    is_new: bool,
    stats: ManagerStats,
}

impl Manager {
    pub fn new(dir: String, blocksize: u64) -> Self {
        let is_new = !fs::exists(&dir).unwrap_or_default();

        if is_new {
            fs::create_dir_all(&dir).ok();
        }

        Self {
            directory: dir,
            blocksize,
            is_new,
            stats: ManagerStats {
                blocks_read: Arc::new(0.into()),
                blocks_writes: Arc::new(0.into()),
            },
        }
    }

    fn get_file(&self, name: &str) -> std::io::Result<fs::File> {
        let filepath = std::path::Path::new(&self.directory).join(name);

        fs::File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(filepath)
    }
    pub fn read(&self, block: &Block, p: &mut Page) -> std::io::Result<()> {
        let mut file = self.get_file(block.filename())?;

        let location = block.num() * self.blocksize;

        file.seek(SeekFrom::Start(location))?;
        self.stats.increment_blocks_read();
        file.read(p.contents()).map(|_| ())
    }

    pub fn write(&self, block: &Block, p: &mut Page) -> std::io::Result<()> {
        let mut file = self.get_file(block.filename())?;

        let location = block.num() * self.blocksize;

        file.seek(SeekFrom::Start(location))?;
        self.stats.increment_blocks_writes();

        file.write_all(p.contents()).map(|_| ())
    }

    pub fn size(&self, file: &str) -> std::io::Result<u64> {
        let file = self.get_file(file)?;
        let total_size = file.metadata()?.len();

        Ok(total_size / self.blocksize)
    }

    pub fn append(&self, file: &str) -> std::io::Result<Block> {
        let blknum = self.size(file)?;

        let block = Block::new(file.to_string(), blknum);
        let data = vec![0; self.blocksize as usize];

        let mut file = self.get_file(file)?;

        file.seek(SeekFrom::Start(self.blocksize * block.num()))?;
        self.stats.increment_blocks_writes();

        file.write(&data).map(|_| block)
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn blocksize(&self) -> u64 {
        self.blocksize
    }
}

struct ManagerStats {
    blocks_read: Arc<AtomicU64>,
    blocks_writes: Arc<AtomicU64>,
}

impl ManagerStats {
    fn increment_blocks_read(&self) {
        self.blocks_read
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn increment_blocks_writes(&self) {
        self.blocks_writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
