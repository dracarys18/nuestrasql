#[cfg(test)]
mod tests {
    use std::fs;

    use rand::{distr::Uniform, prelude::Distribution};

    use crate::{
        server::{DBServer, DBServerOptions},
        storage::record::{
            layout::Layout,
            scan::{table_scan::TableScan, RowImpl, Scan, UpdateScan},
            schema::Schema,
        },
    };

    #[test]
    fn tablescantest() {
        let db = DBServer::new_with_params(
            DBServerOptions::default()
                .block_size(400)
                .directory("tabletest".to_string())
                .pool_size(8),
        )
        .unwrap();
        let mut tx = db.new_tx().unwrap();

        let mut sch = Schema::new();
        sch.add_int_field("A".to_string());
        sch.add_string_field("B".to_string(), 9);
        let layout = Layout::new(sch).unwrap();

        let e = [("A", 4), ("B", 8)];
        for (i, fldname) in layout.schema().fields().iter().enumerate() {
            assert_eq!(fldname, e[i].0);

            let offset = layout.offset(fldname).unwrap();
            assert_eq!(offset, e[i].1);
        }

        let mut ts = TableScan::new(tx.clone(), "T".to_string(), layout.clone()).unwrap();
        let mut rng = rand::rng();
        let die = Uniform::new(0, 50).unwrap();
        for i in 0..50 {
            ts.insert().unwrap();
            let n = die.sample(&mut rng);
            ts.set_int("A", n).unwrap();
            ts.set_string("B", format!("rec{n}")).unwrap();

            let rid = ts.get_row_id();
            assert_eq!(rid.blk_num(), i / 19);
            assert_eq!(rid.slot().inner(), i as usize % 19);
            assert!((0..50).contains(&n));
        }

        let mut count = 0;
        ts.before_first().unwrap();
        while ts.next().unwrap() {
            let a = ts.get_int("A").unwrap();
            let b = ts.get_string("B").unwrap();
            if a < 25 {
                count += 1;

                let rid = ts.get_row_id();

                let blknum = rid.blk_num();
                assert!(blknum <= 2);

                let slot = rid.slot();
                assert!(slot.inner() < 19);

                assert!(blknum * 19 + (slot.inner() as u64) < 50);

                assert!(a < 25);
                assert_eq!(format!("rec{a}"), b);

                ts.delete().unwrap();
            }
        }
        assert!((0..=50).contains(&count));

        ts.before_first().unwrap();
        while ts.next().unwrap() {
            let a = ts.get_int("A").unwrap();
            let b = ts.get_string("B").unwrap();

            let rid = ts.get_row_id();

            let blknum = rid.blk_num();
            assert!(blknum <= 2);

            let slot = rid.slot();
            assert!(slot.inner() < 19);

            assert!(blknum * 19 + (slot.inner() as u64) < 50);

            assert!(a >= 25);
            assert_eq!(format!("rec{a}"), b);
        }
        ts.close().unwrap();
        tx.commit().unwrap();

        fs::remove_dir_all("tabletest").unwrap();
    }
}
