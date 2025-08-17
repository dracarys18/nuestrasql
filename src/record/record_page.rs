use super::schema::FieldType;
use crate::{
    common::slot::Slot, disk::block::Block, error::DbResult, record::layout::Layout,
    tx::Transactions,
};
use std::mem::MaybeUninit;

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
        let mut rc = Self {
            tx: transaction,
            block,
            layout,
        };

        assert!(rc.tx.pin(&rc.block).is_ok());

        rc
    }

    /// Gets the postion of the field and gets the data from it
    pub(crate) fn get_int(&mut self, slot: Slot, field_name: &str) -> DbResult<i32> {
        let field_pos = self.offset(slot) + self.layout.offset(field_name)?;
        self.tx.get_int(&self.block, field_pos.inner() as u32)
    }

    /// Gets the postion of the field and get the data from it
    pub(crate) fn get_string(&mut self, slot: Slot, field_name: &str) -> DbResult<String> {
        let field_pos = self.offset(slot) + self.layout.offset(field_name)?;
        self.tx.get_string(&self.block, field_pos.inner() as u32)
    }

    /// Sets the int value in a layout
    pub(crate) fn set_int(&mut self, slot: Slot, field_name: &str, val: i32) -> DbResult<()> {
        let field_pos = self.offset(slot) + self.layout.offset(field_name)?;
        self.tx
            .set_int(&self.block, field_pos.inner() as u32, val, true)
    }

    /// Sets the string value in a layout
    pub(crate) fn set_string(&mut self, slot: Slot, field_name: &str, val: String) -> DbResult<()> {
        let field_pos = self.offset(slot) + self.layout.offset(field_name)?;

        self.tx
            .set_string(&self.block, field_pos.inner() as u32, val, true)
    }

    /// Sets the record to empty
    pub(crate) fn delete(&mut self, slot: Slot) -> DbResult<()> {
        self.set_flag(slot, RecordFlags::Empty)
    }

    pub(crate) fn set_flag(&mut self, slot: Slot, flag: RecordFlags) -> DbResult<()> {
        self.tx.set_int(
            &self.block,
            self.offset(slot).inner() as u32,
            flag as i32,
            true,
        )
    }

    /// Formats the record before adding a new block.
    ///
    /// Format does the garbage collection of the record pages. And aligns them properly in disk
    /// i.e if the record value was deleted before. This process will properly align all of the
    /// blocks properly in the disk
    pub(crate) fn format(&mut self) -> DbResult<()> {
        let mut slot = Slot::new(0_usize);

        while self.is_valid_slot(slot) {
            self.tx.set_int(
                &self.block,
                self.offset(slot).inner() as u32,
                RecordFlags::Empty as i32,
                false,
            )?;

            let schema = self.layout.schema();
            for field_name in schema.fields() {
                let field_pos = self.offset(slot) + self.layout.offset(field_name)?;

                if schema.typ(field_name)?.eq(&FieldType::Integer) {
                    self.tx
                        .set_int(&self.block, field_pos.inner() as u32, 0, false)?;
                } else {
                    self.tx.set_string(
                        &self.block,
                        field_pos.inner() as u32,
                        String::default(),
                        false,
                    )?;
                }
            }

            slot += 1;
        }

        Ok(())
    }

    /// Returns the next slot which is used after the given slot
    pub fn next_after(&mut self, slot: Slot) -> DbResult<Slot> {
        self.search_after(slot, RecordFlags::Used)
    }

    /// Inserts a new slot after the given slot
    pub fn insert_after(&mut self, slot: Slot) -> DbResult<Slot> {
        let new_slot = self.search_after(slot, RecordFlags::Empty)?;

        if new_slot.is_init() {
            self.set_flag(new_slot, RecordFlags::Used)?;
        }

        Ok(new_slot)
    }

    /// Searches for the next slot after the given slot which has the given flag
    fn search_after(&mut self, mut slot: Slot, flag: RecordFlags) -> DbResult<Slot> {
        slot += 1;
        while self.is_valid_slot(slot) {
            if self
                .tx
                .get_int(&self.block, self.offset(slot).inner() as u32)?
                .eq(&(flag as i32))
            {
                return Ok(slot);
            }
            slot += 1;
        }

        Ok(Slot::UnInit)
    }

    /// If the slot does not exceed the current block then it's a valid slot otherwise it's full
    fn is_valid_slot(&self, slot: Slot) -> bool {
        self.offset(slot + 1).inner() as u64 <= self.tx.blocksize()
    }

    /// Returns the offset of the slot in the block
    fn offset(&self, slot: Slot) -> Slot {
        slot * self.layout.slot_size()
    }

    pub(crate) fn block_mut(&mut self) -> &mut Block {
        &mut self.block
    }

    pub(crate) fn block(&self) -> &Block {
        &self.block
    }
}

/// Kind of sin-version of RecordPage to have an option to not have initialized RecordPage during
/// usage but still have the ability to initialize it later
pub(crate) struct MaybeInitRecordPage {
    inner: MaybeUninit<RecordPage>,
    init: bool,
}

impl MaybeInitRecordPage {
    /// Creates a new MaybeInitRecordPage
    pub(crate) fn new() -> Self {
        Self {
            inner: MaybeUninit::uninit(),
            init: false,
        }
    }
    /// Initializes the RecordPage with the given parameters
    pub(crate) fn overwrite(&mut self, record_page: RecordPage) {
        if self.init {
            unsafe {
                self.inner.as_mut_ptr().drop_in_place();
            }
        }
        self.inner.write(record_page);
        self.init = true;
    }
}

impl std::ops::Deref for MaybeInitRecordPage {
    type Target = RecordPage;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner.as_ptr() }
    }
}

impl std::ops::DerefMut for MaybeInitRecordPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.inner.as_mut_ptr() }
    }
}
