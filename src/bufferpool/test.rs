#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{
        disk::{block::Block, page::Page},
        server::{DBServer, DBServerOptions},
    };

    #[test]
    fn bufferfiletest() {
        let db = DBServer::new_with_params(
            DBServerOptions::default()
                .block_size(400)
                .directory("bufftest".to_string())
                .pool_size(8),
        )
        .unwrap();
        let m = db.buffer_manager();
        let mut bm = m.lock().unwrap();
        let blk = Block::new("testfile".to_string(), 2);
        let pos1 = 88;

        let b1 = bm.pin(blk.clone()).unwrap();
        let buf = bm.get_buffer_mut(b1);

        let p1 = buf.contents();
        p1.set_string(pos1, "abcdefghijklm".to_string());
        let size = Page::max_len("abcdefghijklm".len());
        let pos2 = pos1 + size;
        p1.set_int(pos2, 345);
        buf.set_modified(1, 0);
        bm.unpin(b1);

        let b2 = bm.pin(blk.clone()).unwrap();
        let buf2 = bm.get_buffer_mut(b2);
        let p2 = buf2.contents();
        assert_eq!(pos2, 105);
        assert_eq!(p2.get_int(pos2), 345);
        assert_eq!(pos1, 88);
        assert_eq!(p2.get_string(pos1), "abcdefghijklm");
        bm.unpin(b2);

        fs::remove_dir_all("bufftest").unwrap();
    }
}
