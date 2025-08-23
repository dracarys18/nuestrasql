mod test;

use crate::{
    common::{maybeinit::MaybeInit, slot::Slot},
    error::DbResult,
    storage::disk::block::Block,
    storage::record::{
        layout::Layout,
        record_page::RecordPage,
        rowid::RowId,
        scan::{constant::Constant, RowImpl, Scan, UpdateScan},
        schema::FieldType,
    },
    storage::tx::Transactions,
};

/// TableScan is to store and scan through the whole table
pub struct TableScan {
    tx: Transactions,
    layout: Layout,
    rp: MaybeInit<RecordPage>,
    file_name: String,
    current_slot: Slot,
}

impl TableScan {
    /// New object of tablescan takes Transactions, Table Name, Layout as parameters
    pub fn new(mut tx: Transactions, table: String, layout: Layout) -> DbResult<Self> {
        let file_name = format!("{table}.tbl");
        let mut obj = Self {
            tx: tx.clone(),
            layout,
            rp: MaybeInit::new(),
            file_name: file_name.clone(),
            current_slot: Slot::UnInit,
        };

        if tx.size(file_name)? == 0 {
            obj.move_to_new_block()?;
        } else {
            obj.move_to_block(0)?;
        };

        Ok(obj)
    }

    /// Creates a new block at the end of the file and moves the record page to that block
    fn move_to_new_block(&mut self) -> DbResult<()> {
        self.close()?;

        let blk = self.tx.append(self.file_name.clone())?;
        let mut rp = RecordPage::new(self.tx.clone(), blk, self.layout.clone());
        rp.format()?;

        self.rp.overwrite(rp);

        self.current_slot = Slot::UnInit;

        Ok(())
    }

    ///Moves the record page to a block number provided
    fn move_to_block(&mut self, block_num: u64) -> DbResult<()> {
        self.close()?;
        let blk = Block::new(self.file_name.clone(), block_num);
        let rp = RecordPage::new(self.tx.clone(), blk, self.layout.clone());
        self.rp.overwrite(rp);

        self.current_slot = Slot::UnInit;

        Ok(())
    }

    /// Checks if the current record page is at the last block of the file
    fn at_last_block(&mut self) -> DbResult<bool> {
        Ok(self.rp.block().num() == self.tx.size(self.file_name.clone())? - 1)
    }
}

impl RowImpl for TableScan {
    fn get_row_id(&self) -> RowId {
        RowId::new(self.rp.block().num(), self.current_slot)
    }

    fn move_to_row_id(&mut self, row_id: RowId) -> DbResult<()> {
        self.close()?;

        let blk = Block::new(self.file_name.clone(), row_id.blk_num());
        let rp = RecordPage::new(self.tx.clone(), blk, self.layout.clone());

        self.rp.overwrite(rp);
        self.current_slot = row_id.slot();

        Ok(())
    }
}

impl Scan for TableScan {
    fn close(&mut self) -> DbResult<()> {
        // This is the only case where we might call this function
        // without record page initialised
        if self.rp.is_init() {
            self.tx.unpin(self.rp.block())?;
        }

        Ok(())
    }

    fn next(&mut self) -> DbResult<bool> {
        self.current_slot = self.rp.next_after(self.current_slot)?;

        while self.current_slot.is_uninit() {
            if self.at_last_block()? {
                return Ok(false);
            }

            let num = self.rp.block().num();
            self.move_to_block(num + 1)?;
            self.current_slot = self.rp.next_after(self.current_slot)?;
        }

        Ok(true)
    }

    fn get_int(&mut self, field_name: &str) -> DbResult<i32> {
        self.rp.get_int(self.current_slot, field_name)
    }

    fn get_string(&mut self, field_name: &str) -> DbResult<String> {
        self.rp.get_string(self.current_slot, field_name)
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.layout.schema().has_field(field_name)
    }

    fn get_val(&mut self, field_name: &str) -> DbResult<Constant> {
        match self.layout.schema().typ(field_name)? {
            FieldType::Integer => Ok(Constant::with_int(self.get_int(field_name)?)),
            FieldType::Varchar => Ok(Constant::with_string(self.get_string(field_name)?)),
        }
    }

    fn before_first(&mut self) -> DbResult<()> {
        self.move_to_block(0)
    }
}

impl UpdateScan for TableScan {
    fn set_int(&mut self, field_name: &str, val: i32) -> DbResult<()> {
        self.rp.set_int(self.current_slot, field_name, val)
    }

    fn set_string(&mut self, field_name: &str, val: String) -> DbResult<()> {
        self.rp.set_string(self.current_slot, field_name, val)
    }

    fn set_val(&mut self, field_name: &str, val: Constant) -> DbResult<()> {
        if self.layout.schema().typ(field_name)? == FieldType::Integer {
            self.set_int(field_name, val.into_int())
        } else {
            self.set_string(field_name, val.into_string())
        }
    }

    fn insert(&mut self) -> DbResult<()> {
        self.current_slot = self.rp.insert_after(self.current_slot)?;
        while self.current_slot.is_uninit() {
            if self.at_last_block()? {
                self.move_to_new_block()?;
            } else {
                let num = self.rp.block().num();
                self.move_to_block(num + 1)?;
            }
            self.current_slot = self.rp.insert_after(self.current_slot)?;
        }

        Ok(())
    }

    fn delete(&mut self) -> DbResult<()> {
        self.rp.delete(self.current_slot)
    }
}
