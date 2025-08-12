use super::schema::FieldType;
use crate::{
    disk::block::Block,
    error::{DbError, DbResult},
    record::layout::Layout,
    tx::Transactions,
};

#[repr(i32)]
#[derive(Copy, Clone)]
pub(crate) enum RecordFlags {
    Used,
    Empty,
}

/// Stores a record in a given location in a block
///
/// Every record page maintains an array of layout which contains the information about the fields
/// in the page. This is very important for a record to be non homogeneous (variable in size). We
/// need to track what's the size of individual fields and offset
pub(crate) struct RecordPage {
    block: Block,
    tx: Transactions,
    layout: Layout,
}

impl RecordPage {
    /// Creates a new Object of RecordPage
    pub(crate) fn new(transaction: Transactions, block: Block, layout: Layout) -> Self {
        Self {
            tx: transaction,
            block,
            layout,
        }
    }

    /// Gets the postion of the field and gets the data from it
    pub(crate) fn get_int(&mut self, slot: usize, field_name: &String) -> DbResult<i32> {
        let field_pos = self.offset(slot) + self.layout.offset(field_name)?;
        self.tx.get_int(&self.block, field_pos as u32)
    }

    /// Gets the postion of the field and get the data from it
    pub(crate) fn get_string(&mut self, slot: usize, field_name: &String) -> DbResult<String> {
        let field_pos = self.offset(slot) + self.layout.offset(field_name)?;
        self.tx.get_string(&self.block, field_pos as u32)
    }

    pub(crate) fn set_int(&mut self, slot: usize, field_name: &String, val: i32) -> DbResult<()> {
        let field_pos = self.offset(slot) + self.layout.offset(field_name)?;
        self.tx.set_int(&self.block, field_pos as u32, val, true)
    }

    pub(crate) fn set_string(
        &mut self,
        slot: usize,
        field_name: &String,
        val: String,
    ) -> DbResult<()> {
        let field_pos = self.offset(slot) + self.layout.offset(field_name)?;
        self.tx.set_string(&self.block, field_pos as u32, val, true)
    }

    pub(crate) fn delete(&mut self, slot: usize) -> DbResult<()> {
        self.set_flag(slot, RecordFlags::Empty)
    }

    pub(crate) fn set_flag(&mut self, slot: usize, flag: RecordFlags) -> DbResult<()> {
        self.tx
            .set_int(&self.block, self.offset(slot) as u32, flag as i32, true)
    }

    pub(crate) fn format(&mut self) -> DbResult<()> {
        let mut slot = 0_usize;
        while self.is_valid_slot(slot) {
            self.tx.set_int(
                &self.block,
                self.offset(slot) as u32,
                RecordFlags::Empty as i32,
                false,
            )?;

            let schema = self.layout.schema();
            for field_name in schema.fields() {
                let field_pos = self.offset(slot) + self.layout.offset(field_name)?;

                if schema.typ(field_name)?.eq(&FieldType::Integer) {
                    self.tx.set_int(&self.block, field_pos as u32, 0, false)?;
                } else {
                    self.tx
                        .set_string(&self.block, field_pos as u32, String::default(), false)?;
                }
            }

            slot += 1;
        }

        Ok(())
    }

    pub fn next_after(&mut self, slot: usize) -> DbResult<usize> {
        self.search_after(slot, RecordFlags::Used)
    }

    pub fn insert_after(&mut self, slot: usize) -> DbResult<usize> {
        self.search_after(slot, RecordFlags::Empty)
            .and_then(|new_slot| {
                self.set_flag(new_slot, RecordFlags::Used)?;
                Ok(new_slot)
            })
    }

    fn search_after(&mut self, mut slot: usize, flag: RecordFlags) -> DbResult<usize> {
        slot += 1;
        while self.is_valid_slot(slot) {
            if self
                .tx
                .get_int(&self.block, self.offset(slot) as u32)?
                .eq(&(flag as i32))
            {
                return Ok(slot);
            }
            slot += 1;
        }

        Err(DbError::SlotNotFound)
    }

    /// If the slot does not exceed the current block then it's a valid slot otherwise it's full
    fn is_valid_slot(&self, slot: usize) -> bool {
        self.offset(slot + 1) as u64 <= self.tx.blocksize()
    }

    fn offset(&self, slot: usize) -> usize {
        slot * self.layout.slot_size()
    }

    pub(crate) fn block(&mut self) -> &mut Block {
        &mut self.block
    }
}
