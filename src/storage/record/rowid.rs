use crate::common::slot::Slot;

pub struct RowId {
    blk_num: u64,
    slot: Slot,
}

impl RowId {
    pub fn new(blk_num: u64, slot: Slot) -> Self {
        Self { blk_num, slot }
    }

    pub fn slot(&self) -> Slot {
        self.slot
    }

    pub fn blk_num(&self) -> u64 {
        self.blk_num
    }
}
