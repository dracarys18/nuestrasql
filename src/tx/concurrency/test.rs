#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use crate::{
        bufferpool::pool::BufferPoolManager,
        disk::{block::Block, manager::Manager},
        log::manager::LogManager,
        server::{DBServer, DBServerOptions},
        tx::Transactions,
    };

    #[test]
    fn concurrencytest() {
        let db = DBServer::new_with_params(
            DBServerOptions::default()
                .block_size(400)
                .directory("concurrencytest".to_string())
                .pool_size(8),
        )
        .unwrap();

        let fm = db.file_manager();
        let lm = db.log_manager();
        let bm = db.buffer_manager();

        let fm_a = fm.clone();
        let lm_a = lm.clone();
        let bm_a = bm.clone();
        let handler_a = thread::spawn(move || run_a(fm_a, lm_a, bm_a));

        let fm_b = fm.clone();
        let lm_b = lm.clone();
        let bm_b = bm.clone();
        let handler_b = thread::spawn(move || run_b(fm_b, lm_b, bm_b));

        let fm_c = fm.clone();
        let lm_c = lm.clone();
        let bm_c = bm.clone();
        let handler_c = thread::spawn(move || run_c(fm_c, lm_c, bm_c));

        handler_a.join().unwrap();
        handler_b.join().unwrap();
        handler_c.join().unwrap();

        fs::remove_dir_all("concurrencytest").unwrap();
    }

    fn run_a(fm: Arc<Manager>, lm: Arc<Mutex<LogManager>>, bm: Arc<Mutex<BufferPoolManager>>) {
        let mut tx_a = Transactions::new(fm, bm, lm);
        let blk1 = Block::new("testfile".to_string(), 1);
        let blk2 = Block::new("testfile".to_string(), 2);
        tx_a.pin(&blk1).unwrap();
        tx_a.pin(&blk2).unwrap();

        println!("TX A: request slock 1");
        tx_a.get_int(&blk1, 0).unwrap();

        println!("TX A: receive slock 1");
        thread::sleep(Duration::from_millis(20));

        println!("TX A: request slock 2");
        tx_a.get_int(&blk2, 0).unwrap();

        println!("TX A: receive slock 1");
        tx_a.commit().unwrap();

        println!("TX A: commit");
    }

    fn run_b(fm: Arc<Manager>, lm: Arc<Mutex<LogManager>>, bm: Arc<Mutex<BufferPoolManager>>) {
        let mut tx_b = Transactions::new(fm, bm, lm);
        let blk1 = Block::new("testfile".to_string(), 1);
        let blk2 = Block::new("testfile".to_string(), 2);
        tx_b.pin(&blk1).unwrap();
        tx_b.pin(&blk2).unwrap();

        println!("TX B: request xlock 2");
        tx_b.set_int(&blk2, 0, 0, false).unwrap();

        println!("TX B: receive xlock 2");
        thread::sleep(Duration::from_millis(20));

        println!("TX B: request slock 1");
        tx_b.get_int(&blk1, 0).unwrap();

        println!("TX B: receive slock 1");
        tx_b.commit().unwrap();

        println!("TX B: COMMIT");
    }

    fn run_c(fm: Arc<Manager>, lm: Arc<Mutex<LogManager>>, bm: Arc<Mutex<BufferPoolManager>>) {
        let mut tx_c = Transactions::new(fm, bm, lm);
        let blk1 = Block::new("testfile".to_string(), 1);
        let blk2 = Block::new("testfile".to_string(), 2);
        tx_c.pin(&blk1).unwrap();
        tx_c.pin(&blk2).unwrap();
        thread::sleep(Duration::from_millis(10));

        tx_c.set_int(&blk1, 0, 0, false).unwrap();

        thread::sleep(Duration::from_millis(20));

        tx_c.get_int(&blk2, 0).unwrap();

        tx_c.commit().unwrap();
    }
}
