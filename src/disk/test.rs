#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{
        disk::{block::Block, page::Page},
        server::{DBServer, DBServerOptions},
    };

    #[test]
    fn filetest() {
        let db = DBServer::new_with_params(
            DBServerOptions::default()
                .directory("db".to_string())
                .block_size(400),
        )
        .unwrap();
        let fm = db.file_manager();

        let blk = Block::new("testfile".to_string(), 2);
        let mut p1 = Page::new(fm.blocksize());
        let pos1 = 88;
        p1.set_string(pos1, "abcdefghijklm".to_string());
        let size = Page::max_len("abcdefghijklm".len());
        let pos2 = pos1 + size;
        p1.set_int(pos2, 345);
        fm.write(&blk, &mut p1).unwrap();

        let mut p2 = Page::new(fm.blocksize());
        fm.read(&blk, &mut p2).unwrap();

        assert_eq!(105, pos2);
        assert_eq!(345, p2.get_int(pos2));
        assert_eq!(88, pos1);
        assert_eq!("abcdefghijklm", p2.get_string(pos1).unwrap());

        fs::remove_dir_all("db").unwrap();
    }
}
