#[cfg(test)]
mod tests {
    use std::{
        fs,
        iter::zip,
        sync::{Arc, Mutex},
    };

    use crate::{
        disk::page::Page,
        log::manager::LogManager,
        server::{DBServer, DBServerOptions},
    };

    #[test]
    fn logtest() {
        let options = DBServerOptions::default()
            .directory(String::from("logtest"))
            .block_size(400);

        let db = DBServer::new_with_params(options).unwrap();
        let lm = db.log_manager();

        assert_log_records(lm.clone(), Vec::new());
        create_records(lm.clone(), 1, 35);
        assert_log_records(lm.clone(), (1..=35).rev().collect());
        create_records(lm.clone(), 36, 70);
        lm.lock().unwrap().flush(65).unwrap();
        assert_log_records(lm, (1..=70).rev().collect());

        fs::remove_dir_all("logtest").unwrap();
    }

    fn assert_log_records(lm: Arc<Mutex<LogManager>>, expected: Vec<i32>) {
        let iter = lm.lock().unwrap().iter().unwrap();
        for (rec, exp) in zip(iter, expected) {
            let mut p = Page::new_with_data(rec);
            let s = p.get_string(0);
            let npos = Page::max_len(s.len());
            let val = p.get_int(npos);

            assert_eq!(format!("record{}", exp), s);
            assert_eq!(exp + 100, val);
        }
    }

    fn create_records(lm: Arc<Mutex<LogManager>>, start: usize, end: usize) {
        for i in start..=end {
            let s = format!("{}{}", "record", i);
            let rec = create_log_record(s.as_str(), i + 100);
            let lsn = lm.lock().unwrap().append(&rec).unwrap();
            assert_eq!(i, lsn as usize);
        }
    }

    fn create_log_record(s: &str, n: usize) -> Vec<u8> {
        let spos = 0;
        let npos = Page::max_len(s.len());
        let b: Vec<u8> = vec![0; npos + 4];
        let mut p = Page::new_with_data(b);
        p.set_string(spos, s.to_string());
        p.set_int(npos, n as i32);
        p.contents().to_vec()
    }
}
