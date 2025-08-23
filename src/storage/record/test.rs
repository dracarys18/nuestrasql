#[cfg(test)]
mod tests {
    use std::fs;

    use rand::{distr::Uniform, prelude::Distribution};

    use crate::{
        common::slot::Slot,
        server::{DBServer, DBServerOptions},
        storage::record::{layout::Layout, record_page::RecordPage, schema::Schema},
    };

    #[test]
    fn recordtest() {
        let options = DBServerOptions::default()
            .directory(String::from("recordtest"))
            .block_size(400)
            .pool_size(8);

        let db = DBServer::new_with_params(options).unwrap();
        let mut tx = db.new_tx().unwrap();

        let mut sch = Schema::new();
        sch.add_int_field("A".to_string());
        sch.add_string_field("B".to_string(), 9);
        let layout = Layout::new(sch).expect("Unable to create layout");

        let e = [("A", 4), ("B", 8)];
        for (i, fldname) in layout.schema().fields().iter().enumerate() {
            assert_eq!(fldname, e[i].0);

            let offset = layout.offset(fldname).expect("Cannot get offset");
            assert_eq!(offset, e[i].1);
        }
        let blk = tx.append("testfile".to_string()).unwrap();

        tx.pin(&blk).unwrap();

        let mut rp = RecordPage::new(tx.clone(), blk.clone(), layout);
        rp.format().unwrap();

        let mut slot = rp.insert_after(Slot::uninit()).unwrap();
        let mut rng = rand::rng();
        let die = Uniform::new(0, 50).unwrap();
        while slot.is_init() {
            let n = die.sample(&mut rng);
            rp.set_int(slot, &String::from("A"), n).unwrap();
            rp.set_string(slot, &String::from("B"), format!("rec{n}"))
                .unwrap();
            assert!(slot <= 18.into());
            assert!((0..50).contains(&n));
            slot = rp.insert_after(slot).unwrap();
        }

        let mut count = 0;
        slot = rp.next_after(0.into()).unwrap();
        while slot.is_init() {
            let a = rp.get_int(slot, &String::from("A")).unwrap();
            let b = rp.get_string(slot, &String::from("B")).unwrap();
            if a < 25 {
                count += 1;
                assert!(slot <= 18.into());
                assert!(a < 25);
                assert_eq!(format!("rec{a}"), b);
                rp.delete(slot).unwrap();
            }
            slot = rp.next_after(slot).unwrap();
        }
        assert!((0..=18).contains(&count));

        slot = rp.next_after(0.into()).unwrap();

        while slot.is_init() {
            let a = rp.get_int(slot, &String::from("A")).unwrap();
            let b = rp.get_string(slot, &String::from("B")).unwrap();
            assert!(slot <= 18.into());
            assert!(a >= 25);
            assert_eq!(format!("rec{a}"), b);
            slot = rp.next_after(slot).unwrap();
        }
        tx.unpin(&blk).unwrap();
        tx.commit().unwrap();

        fs::remove_dir_all("recordtest").unwrap();
    }
}
