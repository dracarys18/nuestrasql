#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    use crate::{
        bufferpool::pool::BufferPoolManager,
        disk::{block::Block, manager::Manager, page::Page},
        server::{DBServer, DBServerOptions},
    };

    #[test]
    fn recoverytest() {
        let db = DBServer::new_with_params(
            DBServerOptions::default()
                .block_size(400)
                .directory("recoverytest".to_string())
                .pool_size(8),
        )
        .unwrap();
        let fm = db.file_manager();
        let bm = db.buffer_manager();
        let blk0 = Block::new("testfile".to_string(), 0);
        let blk1 = Block::new("testfile".to_string(), 1);

        for _ in 0..2 {
            if fm.size("testfile").unwrap() == 0 {
                initialize(&db, fm.clone(), &blk0, &blk1);
                modify(&db, fm.clone(), bm.clone(), &blk0, &blk1);
            } else {
                recover(&db, fm.clone(), &blk0, &blk1);
            }
        }
        fs::remove_dir_all("recoverytest").unwrap();
    }

    fn initialize(db: &DBServer, fm: Arc<Manager>, blk0: &Block, blk1: &Block) {
        let mut tx1 = db.new_tx().unwrap();
        let mut tx2 = db.new_tx().unwrap();
        tx1.pin(blk0).unwrap();
        tx2.pin(blk1).unwrap();
        let bytes = 4;
        let mut pos = 0;
        for _ in 0..6 {
            tx1.set_int(blk0, pos, pos as i32, false).unwrap();
            tx2.set_int(blk1, pos, pos as i32, false).unwrap();
            pos += bytes;
        }
        tx1.set_string(blk0, 30, "abc".to_string(), false).unwrap();
        tx2.set_string(blk1, 30, "def".to_string(), false).unwrap();
        tx1.commit().unwrap();
        tx2.commit().unwrap();

        print_values(fm.clone(), "After initialization", blk0, blk1);
        assert_values(
            fm,
            blk0,
            blk1,
            [[0, 0], [4, 4], [8, 8], [12, 12], [16, 16], [20, 20]],
            ["abc", "def"],
        );
    }

    fn modify(
        db: &DBServer,
        fm: Arc<Manager>,
        bm: Arc<Mutex<BufferPoolManager>>,
        blk0: &Block,
        blk1: &Block,
    ) {
        let mut tx3 = db.new_tx().unwrap();
        let mut tx4 = db.new_tx().unwrap();
        tx3.pin(blk0).unwrap();
        tx4.pin(blk1).unwrap();
        let mut pos = 0;
        let bytes = 4;
        for _ in 0..6 {
            tx3.set_int(blk0, pos, pos as i32 + 100, true).unwrap();
            tx4.set_int(blk1, pos, pos as i32 + 100, true).unwrap();
            pos += bytes;
        }
        tx3.set_string(blk0, 30, "uvw".to_string(), true).unwrap();
        tx4.set_string(blk1, 30, "xyz".to_string(), true).unwrap();
        bm.lock().unwrap().flush_all(tx3.txnum() as i32).unwrap();
        bm.lock().unwrap().flush_all(tx4.txnum() as i32).unwrap();

        print_values(fm.clone(), "After modification", blk0, blk1);
        tx3.rollback().unwrap();

        print_values(fm.clone(), "After rollback", blk0, blk1);
    }

    fn print_values(fm: Arc<Manager>, message: &str, blk0: &Block, blk1: &Block) {
        println!("{}", message);
        let mut p0 = Page::new(fm.blocksize());
        let mut p1 = Page::new(fm.blocksize());

        fm.read(blk0, &mut p0).unwrap();
        fm.read(blk1, &mut p1).unwrap();

        let mut pos = 0;

        for _ in 0..6 {
            print!("{} ", p0.get_int(pos));
            print!("{} ", p1.get_int(pos));
            pos += 4;
        }
        print!("{} ", p0.get_string(30));
        print!("{} ", p1.get_string(30));
        println!()
    }
    fn recover(db: &DBServer, fm: Arc<Manager>, blk0: &Block, blk1: &Block) {
        let mut tx = db.new_tx().unwrap();
        tx.recover().unwrap();

        print_values(fm.clone(), "After recover", blk0, blk1);
        assert_values(
            fm,
            blk0,
            blk1,
            [[0, 0], [4, 4], [8, 8], [12, 12], [16, 16], [20, 20]],
            ["abc", "def"],
        );
    }

    fn assert_values(
        fm: Arc<Manager>,
        blk0: &Block,
        blk1: &Block,
        e1: [[i32; 2]; 6],
        e2: [&str; 2],
    ) {
        let mut p0 = Page::new(fm.blocksize());
        let mut p1 = Page::new(fm.blocksize());
        fm.read(blk0, &mut p0).unwrap();
        fm.read(blk1, &mut p1).unwrap();
        let mut pos = 0;
        for item in e1 {
            assert_eq!(p0.get_int(pos), item[0]);
            assert_eq!(p1.get_int(pos), item[1]);
            pos += 4;
        }
        assert_eq!(p0.get_string(30), e2[0]);
        assert_eq!(p1.get_string(30), e2[1]);
    }
}
